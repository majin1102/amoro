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

package org.apache.amoro.server.admin;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIdentifier;
import org.apache.amoro.admin.AdminUtil;
import org.apache.amoro.admin.TableFormatAdmin;
import org.apache.amoro.admin.model.AmoroSnapshotsOfTable;
import org.apache.amoro.admin.model.DDLInfo;
import org.apache.amoro.admin.model.HiveTableInfo;
import org.apache.amoro.admin.model.OperationType;
import org.apache.amoro.admin.model.OptimizingProcessInfo;
import org.apache.amoro.admin.model.OptimizingTaskInfo;
import org.apache.amoro.admin.model.PartitionBaseInfo;
import org.apache.amoro.admin.model.PartitionFileBaseInfo;
import org.apache.amoro.admin.model.ServerTableMeta;
import org.apache.amoro.admin.model.TagOrBranchInfo;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.exception.ObjectNotExistsException;
import org.apache.amoro.server.catalog.CatalogService;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.shade.thrift.org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class TableFormatAdmins {

  private final Map<TableFormat, TableFormatAdmin> formatAdminMap = new HashMap<>();

  private final CatalogService catalogService;

  public TableFormatAdmins(CatalogService catalogService) {
    this.catalogService = catalogService;
    AdminUtil.loadTableFormatAdminMap(formatAdminMap);
  }

  public ServerTableMeta getTableDetail(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTableDetail(amoroTable));
  }

  public List<AmoroSnapshotsOfTable> getSnapshots(
      TableIdentifier tableIdentifier, String ref, OperationType operationType) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getSnapshots(amoroTable, ref, operationType));
  }

  public List<PartitionFileBaseInfo> getSnapshotDetail(
      TableIdentifier tableIdentifier, long snapshotId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getSnapshotDetail(amoroTable, snapshotId));
  }

  public List<DDLInfo> getTableOperations(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTableOperations(amoroTable));
  }

  public List<PartitionBaseInfo> getTablePartition(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTablePartitions(amoroTable));
  }

  public List<PartitionFileBaseInfo> getTableFile(
      TableIdentifier tableIdentifier, String partition, Integer specId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTableFiles(amoroTable, partition, specId));
  }

  public List<TagOrBranchInfo> getTableTags(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTableTags(amoroTable));
  }

  public List<TagOrBranchInfo> getTableBranches(TableIdentifier tableIdentifier) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getTableBranches(amoroTable));
  }

  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      TableIdentifier tableIdentifier, int limit, int offset) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(
        amoroTable, admin -> admin.getOptimizingProcessesInfo(amoroTable, limit, offset));
  }

  public List<OptimizingTaskInfo> getOptimizingProcessTaskInfos(
      TableIdentifier tableIdentifier, long processId) {
    AmoroTable<?> amoroTable = loadTable(tableIdentifier);
    return safelyCall(amoroTable, admin -> admin.getOptimizingTaskInfos(amoroTable, processId));
  }

  public HiveTableInfo getHiveTableDetail(
      TableIdentifier tableIdentifier, CatalogMeta catalogMeta) {
    HiveTableInfo hiveTableInfo = null;
    for (TableFormatAdmin tableFormatAdmin : formatAdminMap.values()) {
      hiveTableInfo = tableFormatAdmin.getHiveTableDetail(tableIdentifier, catalogMeta);
      if (hiveTableInfo != null) {
        break;
      }
    }
    return hiveTableInfo;
  }

  private AmoroTable<?> loadTable(TableIdentifier identifier) {
    ServerCatalog catalog = catalogService.getServerCatalog(identifier.getCatalog());
    return catalog.loadTable(identifier.getDatabase(), identifier.getTableName());
  }

  private <T> T safelyCall(AmoroTable<?> table, Function<TableFormatAdmin, T> function) {
    return Optional.ofNullable(formatAdminMap.get(table.format()))
        .map(function)
        .orElseThrow(() -> new ObjectNotExistsException("TableFormatAmdin: " + table.format()));
  }
}
