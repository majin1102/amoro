/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.admin.controller;

import io.javalin.http.Context;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIdentifier;
import org.apache.amoro.admin.OptimizingAdmin;
import org.apache.amoro.admin.model.AMSColumnInfo;
import org.apache.amoro.admin.model.AmoroSnapshotsOfTable;
import org.apache.amoro.admin.model.DDLInfo;
import org.apache.amoro.admin.model.HiveTableInfo;
import org.apache.amoro.admin.model.OperationType;
import org.apache.amoro.admin.model.OptimizingProcessInfo;
import org.apache.amoro.admin.model.OptimizingTaskInfo;
import org.apache.amoro.admin.model.PartitionBaseInfo;
import org.apache.amoro.admin.model.PartitionFileBaseInfo;
import org.apache.amoro.admin.model.ServerTableMeta;
import org.apache.amoro.admin.model.TableMeta;
import org.apache.amoro.admin.model.TableOperation;
import org.apache.amoro.admin.model.TagOrBranchInfo;
import org.apache.amoro.admin.model.UpgradeRunningInfo;
import org.apache.amoro.admin.model.UpgradeStatus;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.properties.HiveTableProperties;
import org.apache.amoro.properties.TableProperties;
import org.apache.amoro.server.admin.ServerTableProperties;
import org.apache.amoro.server.admin.TableFormatAdmins;
import org.apache.amoro.server.admin.response.ErrorResponse;
import org.apache.amoro.server.admin.response.OkResponse;
import org.apache.amoro.server.admin.response.PageResult;
import org.apache.amoro.server.admin.utils.CommonUtil;
import org.apache.amoro.server.catalog.CatalogService;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.utils.AmsUtil;
import org.apache.amoro.shade.guava32.com.google.common.base.Function;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.amoro.shade.thrift.org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/** The controller that handles table requests. */
public class TableController {
  private static final Logger LOG = LoggerFactory.getLogger(TableController.class);
  private static final long UPGRADE_INFO_EXPIRE_INTERVAL = 60 * 60 * 1000;

  private final CatalogService catalogService;
  private final TableFormatAdmins tableFormatAdmins;
  private final OptimizingAdmin optimizingAdmin;
  private final Configurations serviceConfig;
  private final ConcurrentHashMap<TableIdentifier, UpgradeRunningInfo> upgradeRunningInfo =
      new ConcurrentHashMap<>();
  private final ScheduledExecutorService tableUpgradeExecutor;

  public TableController(
      CatalogService catalogService,
      TableFormatAdmins tableFormatAdmins,
      OptimizingAdmin optimizingAdmin,
      Configurations serviceConfig) {
    this.catalogService = catalogService;
    this.tableFormatAdmins = tableFormatAdmins;
    this.optimizingAdmin = optimizingAdmin;
    this.serviceConfig = serviceConfig;
    this.tableUpgradeExecutor =
        Executors.newScheduledThreadPool(
            0,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("async-hive-table-upgrade-%d")
                .build());
  }

  /**
   * get table detail.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTableDetail(Context ctx) {

    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(database)
            && StringUtils.isNotBlank(tableName),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkState(catalogService.catalogExist(catalog), "invalid catalog!");

    ServerTableMeta serverTableMeta =
        tableFormatAdmins.getTableDetail(TableIdentifier.of(catalog, database, tableName));
    ctx.json(OkResponse.of(serverTableMeta));
  }

  /**
   * get hive table detail.
   *
   * @param ctx - context for handling the request and response
   */
  public void getHiveTableDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    ServerCatalog serverCatalog = catalogService.getServerCatalog(catalog);
    CatalogMeta catalogMeta = serverCatalog.getMetadata();

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    HiveTableInfo hiveTableInfo =
        tableFormatAdmins.getHiveTableDetail(tableIdentifier, catalogMeta);
    ctx.json(OkResponse.of(hiveTableInfo));
  }

  /**
   * upgrade a hive table to mixed-hive table.
   *
   * @param ctx - context for handling the request and response
   */
  public void upgradeHiveTable(Context ctx) {
    throw new UnsupportedOperationException();
  }

  public void getUpgradeStatus(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    UpgradeRunningInfo info =
        upgradeRunningInfo.containsKey(TableIdentifier.of(catalog, db, table))
            ? upgradeRunningInfo.get(TableIdentifier.of(catalog, db, table))
            : new UpgradeRunningInfo(UpgradeStatus.NONE.toString());
    ctx.json(OkResponse.of(info));
  }

  /**
   * get table properties for upgrading hive table to mixed-hive table.
   *
   * @param ctx - context for handling the request and response
   */
  public void getUpgradeHiveTableProperties(Context ctx) throws IllegalAccessException {
    Map<String, String> keyValues = new TreeMap<>();
    Map<String, String> tableProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(TableProperties.class);
    tableProperties.keySet().stream()
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(tableProperties.get(key), tableProperties.get(key + "_DEFAULT")));
    ServerTableProperties.HIDDEN_EXPOSED.forEach(keyValues::remove);
    Map<String, String> hiveProperties =
        AmsUtil.getNotDeprecatedAndNotInternalStaticFields(HiveTableProperties.class);

    hiveProperties.keySet().stream()
        .filter(key -> HiveTableProperties.EXPOSED.contains(hiveProperties.get(key)))
        .filter(key -> !key.endsWith("_DEFAULT"))
        .forEach(
            key -> keyValues.put(hiveProperties.get(key), hiveProperties.get(key + "_DEFAULT")));
    ctx.json(OkResponse.of(keyValues));
  }

  /**
   * get list of optimizing processes.
   *
   * @param ctx - context for handling the request and response
   */
  public void getOptimizingProcesses(Context ctx) {

    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    int offset = (page - 1) * pageSize;
    int limit = pageSize;
    ServerCatalog serverCatalog = catalogService.getServerCatalog(catalog);
    Preconditions.checkArgument(offset >= 0, "offset[%s] must >= 0", offset);
    Preconditions.checkArgument(limit >= 0, "limit[%s] must >= 0", limit);
    Preconditions.checkState(serverCatalog.tableExists(db, table), "no such table");

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    Pair<List<OptimizingProcessInfo>, Integer> optimizingProcessesInfo =
        tableFormatAdmins.getOptimizingProcessesInfo(tableIdentifier, limit, offset);
    List<OptimizingProcessInfo> result = optimizingProcessesInfo.getLeft();
    int total = optimizingProcessesInfo.getRight();

    ctx.json(OkResponse.of(PageResult.of(result, total)));
  }

  /**
   * Get tasks of optimizing process.
   *
   * @param ctx - context for handling the request and response
   */
  public void getOptimizingProcessTasks(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String processId = ctx.pathParam("processId");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    int offset = (page - 1) * pageSize;
    int limit = pageSize;
    ServerCatalog serverCatalog = catalogService.getServerCatalog(catalog);
    Preconditions.checkArgument(offset >= 0, "offset[%s] must >= 0", offset);
    Preconditions.checkArgument(limit >= 0, "limit[%s] must >= 0", limit);
    Preconditions.checkState(serverCatalog.tableExists(db, table), "no such table");

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, db, table);
    List<OptimizingTaskInfo> optimizingTaskInfos =
        tableFormatAdmins.getOptimizingProcessTaskInfos(tableIdentifier, Long.parseLong(processId));

    PageResult<OptimizingTaskInfo> pageResult = PageResult.of(optimizingTaskInfos, offset, limit);
    ctx.json(OkResponse.of(pageResult));
  }

  /**
   * get list of snapshots.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTableSnapshots(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    // ref means tag/branch
    String ref = ctx.queryParamAsClass("ref", String.class).getOrDefault(null);
    String operation =
        ctx.queryParamAsClass("operation", String.class)
            .getOrDefault(OperationType.ALL.displayName());
    OperationType operationType = OperationType.of(operation);

    List<AmoroSnapshotsOfTable> snapshotsOfTables =
        tableFormatAdmins.getSnapshots(
            TableIdentifier.of(catalog, database, tableName), ref, operationType);
    int offset = (page - 1) * pageSize;
    PageResult<AmoroSnapshotsOfTable> pageResult =
        PageResult.of(snapshotsOfTables, offset, pageSize);
    ctx.json(OkResponse.of(pageResult));
  }

  /**
   * get detail of snapshot.
   *
   * @param ctx - context for handling the request and response
   */
  public void getSnapshotDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");
    String snapshotId = ctx.pathParam("snapshotId");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionFileBaseInfo> result =
        tableFormatAdmins.getSnapshotDetail(
            TableIdentifier.of(catalog, database, tableName), Long.parseLong(snapshotId));
    int offset = (page - 1) * pageSize;
    PageResult<PartitionFileBaseInfo> amsPageResult = PageResult.of(result, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * get partition list.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTablePartitions(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String filter = ctx.queryParamAsClass("filter", String.class).getOrDefault("");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionBaseInfo> partitionBaseInfos =
        tableFormatAdmins.getTablePartition(TableIdentifier.of(catalog, database, table));
    partitionBaseInfos =
        partitionBaseInfos.stream()
            .filter(e -> e.getPartition().contains(filter))
            .sorted(Comparator.comparing(PartitionBaseInfo::getPartition).reversed())
            .collect(Collectors.toList());
    int offset = (page - 1) * pageSize;
    PageResult<PartitionBaseInfo> amsPageResult =
        PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * get file list of some partition.
   *
   * @param ctx - context for handling the request and response
   */
  public void getPartitionFileListInfo(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String partition = ctx.pathParam("partition");

    Integer specId = ctx.queryParamAsClass("specId", Integer.class).getOrDefault(0);
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);

    List<PartitionFileBaseInfo> partitionFileBaseInfos =
        tableFormatAdmins.getTableFile(TableIdentifier.of(catalog, db, table), partition, specId);
    int offset = (page - 1) * pageSize;
    PageResult<PartitionFileBaseInfo> amsPageResult =
        PageResult.of(partitionFileBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * get table operations.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTableOperations(Context ctx) throws Exception {
    String catalogName = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String tableName = ctx.pathParam("table");

    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    int offset = (page - 1) * pageSize;

    List<DDLInfo> ddlInfoList =
        tableFormatAdmins.getTableOperations(TableIdentifier.of(catalogName, db, tableName));
    Collections.reverse(ddlInfoList);
    PageResult<TableOperation> amsPageResult =
        PageResult.of(ddlInfoList, offset, pageSize, TableOperation::buildFromDDLInfo);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * get table list of catalog.db.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTableList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String keywords = ctx.queryParam("keywords");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog) && StringUtils.isNotBlank(db),
        "catalog.database can not be empty in any element");

    ServerCatalog serverCatalog = catalogService.getServerCatalog(catalog);
    Function<TableFormat, String> formatToType =
        format -> {
          switch (format) {
            case MIXED_HIVE:
            case MIXED_ICEBERG:
              return TableMeta.TableType.ARCTIC.toString();
            case PAIMON:
              return TableMeta.TableType.PAIMON.toString();
            case ICEBERG:
              return TableMeta.TableType.ICEBERG.toString();
            default:
              throw new IllegalStateException("Unknown format");
          }
        };

    List<TableMeta> tables =
        serverCatalog.listTables(db).stream()
            .map(
                idWithFormat ->
                    new TableMeta(
                        idWithFormat.getIdentifier().getTableName(),
                        formatToType.apply(idWithFormat.getTableFormat())))
            // Sort by table format and table name
            .sorted(
                (table1, table2) -> {
                  if (Objects.equals(table1.getType(), table2.getType())) {
                    return table1.getName().compareTo(table2.getName());
                  } else {
                    return table1.getType().compareTo(table2.getType());
                  }
                })
            .collect(Collectors.toList());
    ctx.json(
        OkResponse.of(
            tables.stream()
                .filter(t -> StringUtils.isBlank(keywords) || t.getName().contains(keywords))
                .collect(Collectors.toList())));
  }

  /**
   * get databases of some catalog.
   *
   * @param ctx - context for handling the request and response
   */
  public void getDatabaseList(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String keywords = ctx.queryParam("keywords");

    List<String> dbList =
        catalogService.getServerCatalog(catalog).listDatabases().stream()
            .filter(item -> StringUtils.isBlank(keywords) || item.contains(keywords))
            .collect(Collectors.toList());
    ctx.json(OkResponse.of(dbList));
  }

  /**
   * get list of catalogs.
   *
   * @param ctx - context for handling the request and response
   */
  public void getCatalogs(Context ctx) {
    List<CatalogMeta> catalogs = catalogService.listCatalogMetas();
    ctx.json(OkResponse.of(catalogs));
  }

  /**
   * get single page query token.
   *
   * @param ctx - context for handling the request and response
   */
  public void getTableDetailTabToken(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");

    String signCal = CommonUtil.generateTablePageToken(catalog, db, table);
    ctx.json(OkResponse.of(signCal));
  }

  public void getTableTags(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    List<TagOrBranchInfo> partitionBaseInfos =
        tableFormatAdmins.getTableTags(TableIdentifier.of(catalog, database, table));
    int offset = (page - 1) * pageSize;
    PageResult<TagOrBranchInfo> amsPageResult = PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  public void getTableBranches(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    Integer page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
    Integer pageSize = ctx.queryParamAsClass("pageSize", Integer.class).getOrDefault(20);
    List<TagOrBranchInfo> partitionBaseInfos =
        tableFormatAdmins.getTableBranches(TableIdentifier.of(catalog, database, table));
    int offset = (page - 1) * pageSize;
    PageResult<TagOrBranchInfo> amsPageResult = PageResult.of(partitionBaseInfos, offset, pageSize);
    ctx.json(OkResponse.of(amsPageResult));
  }

  /**
   * cancel the running optimizing process of one certain table.
   *
   * @param ctx - context for handling the request and response
   */
  public void cancelOptimizingProcess(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String db = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String processId = ctx.pathParam("processId");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog)
            && StringUtils.isNotBlank(db)
            && StringUtils.isNotBlank(table),
        "catalog.database.tableName can not be empty in any element");
    Preconditions.checkState(catalogService.catalogExist(catalog), "invalid catalog!");

    try {
      optimizingAdmin.killOptimizingProcess(
          TableIdentifier.of(catalog, db, table), Long.parseLong(processId));
    } catch (NumberFormatException e) {
      ctx.json(ErrorResponse.of("Invalid process id: " + processId));
    }
    ctx.json(OkResponse.ok());
  }

  private List<AMSColumnInfo> transformHiveSchemaToAMSColumnInfo(List<FieldSchema> fields) {
    return fields.stream()
        .map(
            f -> {
              AMSColumnInfo columnInfo = new AMSColumnInfo();
              columnInfo.setField(f.getName());
              columnInfo.setType(f.getType());
              columnInfo.setComment(f.getComment());
              return columnInfo;
            })
        .collect(Collectors.toList());
  }
}
