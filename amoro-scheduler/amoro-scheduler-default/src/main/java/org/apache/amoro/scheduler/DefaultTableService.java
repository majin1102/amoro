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

package org.apache.amoro.scheduler;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIDWithFormat;
import org.apache.amoro.api.BlockableOperation;
import org.apache.amoro.api.Blocker;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.IllegalMetadataException;
import org.apache.amoro.scheduler.catalog.InternalCatalog;
import org.apache.amoro.scheduler.optimizing.OptimizingStatus;
import org.apache.amoro.scheduler.persistence.TableRuntimeMapper;
import org.apache.amoro.scheduler.table.DefaultTableRuntime;
import org.apache.amoro.scheduler.table.RuntimeHandlerChain;
import org.apache.amoro.scheduler.table.TableBlocker;
import org.apache.amoro.scheduler.table.TableMetadata;
import org.apache.amoro.scheduler.table.TableRuntimeMeta;
import org.apache.amoro.scheduler.table.TableService;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.catalog.CatalogService;
import org.apache.amoro.server.catalog.ExternalCatalog;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.mapper.TableIdentifierMapper;
import org.apache.amoro.server.utils.Conditions;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;
import org.apache.amoro.shade.guava32.com.google.common.base.Objects;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.shade.guava32.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.amoro.utils.TablePropertyUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DefaultTableService extends CatalogService implements TableService {

  public static final Logger LOG = LoggerFactory.getLogger(DefaultTableService.class);
  private final long externalCatalogRefreshingInterval;
  private final long blockerTimeout;

  private final Map<ServerTableIdentifier, DefaultTableRuntime> tableRuntimeMap =
      new ConcurrentHashMap<>();

  private final ScheduledExecutorService tableExplorerScheduler =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("table-explorer-scheduler-%d")
              .setDaemon(true)
              .build());
  private final CompletableFuture<Boolean> initialized = new CompletableFuture<>();
  private RuntimeHandlerChain headHandler;
  private ExecutorService tableExplorerExecutors;

  public DefaultTableService(Configurations configuration) {
    super(configuration);
    this.externalCatalogRefreshingInterval =
        configuration.getLong(AmoroManagementConf.REFRESH_EXTERNAL_CATALOGS_INTERVAL);
    this.blockerTimeout = configuration.getLong(AmoroManagementConf.BLOCKER_TIMEOUT);
  }

  public InternalCatalog getInternalCatalog(String catalogName) {
    checkStarted();
    ServerCatalog serverCatalog = getServerCatalog(catalogName);
    Conditions.checkObjectNotExists(
        !(serverCatalog instanceof InternalCatalog), "Catalog " + catalogName);
    return (InternalCatalog) serverCatalog;
  }

  @Override
  public void dropTableMetadata(TableIdentifier tableIdentifier, boolean deleteData) {
    checkStarted();
    if (StringUtils.isBlank(tableIdentifier.getTableName())) {
      throw new IllegalMetadataException("table name is blank");
    }
    if (StringUtils.isBlank(tableIdentifier.getCatalog())) {
      throw new IllegalMetadataException("catalog is blank");
    }
    if (StringUtils.isBlank(tableIdentifier.getDatabase())) {
      throw new IllegalMetadataException("database is blank");
    }

    InternalCatalog internalCatalog = getInternalCatalog(tableIdentifier.getCatalog());
    String database = tableIdentifier.getDatabase();
    String table = tableIdentifier.getTableName();
    Conditions.checkObjectNotExists(!internalCatalog.tableExists(database, table), tableIdentifier);

    ServerTableIdentifier serverTableIdentifier = internalCatalog.dropTable(database, table);
    Optional.ofNullable(tableRuntimeMap.remove(serverTableIdentifier))
        .ifPresent(
            tableRuntime -> {
              if (headHandler != null) {
                headHandler.fireTableRemoved(tableRuntime);
              }
              tableRuntime.dispose();
            });
  }

  @Override
  public void createTable(String catalogName, TableMetadata tableMetadata) {
    checkStarted();
    InternalCatalog catalog = getInternalCatalog(catalogName);
    String database = tableMetadata.getTableIdentifier().getDatabase();
    String table = tableMetadata.getTableIdentifier().getTableName();
    Conditions.checkObjectAlreadyExists(
        catalog.tableExists(database, table), tableMetadata.getTableIdentifier().getIdentifier());

    TableMetadata metadata = catalog.createTable(tableMetadata);

    triggerTableAdded(catalog, metadata.getTableIdentifier());
  }

  @Override
  public List<ServerTableIdentifier> listManagedTables() {
    checkStarted();
    return getAs(TableIdentifierMapper.class, TableIdentifierMapper::selectAllTableIdentifiers);
  }

  @Override
  public AmoroTable<?> loadTable(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return getServerCatalog(tableIdentifier.getCatalog())
        .loadTable(tableIdentifier.getDatabase(), tableIdentifier.getTableName());
  }

  @Override
  public Blocker block(
      TableIdentifier tableIdentifier,
      List<BlockableOperation> operations,
      Map<String, String> properties) {
    checkStarted();
    return getAndCheckExist(getOrSyncServerTableIdentifier(tableIdentifier))
        .block(operations, properties, blockerTimeout)
        .buildBlocker();
  }

  @Override
  public void releaseBlocker(TableIdentifier tableIdentifier, String blockerId) {
    checkStarted();
    DefaultTableRuntime tableRuntime = getRuntime(getServerTableIdentifier(tableIdentifier));
    if (tableRuntime != null) {
      tableRuntime.release(blockerId);
    }
  }

  @Override
  public long renewBlocker(TableIdentifier tableIdentifier, String blockerId) {
    checkStarted();
    DefaultTableRuntime tableRuntime = getAndCheckExist(getServerTableIdentifier(tableIdentifier));
    return tableRuntime.renew(blockerId, blockerTimeout);
  }

  @Override
  public List<Blocker> getBlockers(TableIdentifier tableIdentifier) {
    checkStarted();
    return getAndCheckExist(getOrSyncServerTableIdentifier(tableIdentifier)).getBlockers().stream()
        .map(TableBlocker::buildBlocker)
        .collect(Collectors.toList());
  }

  @Override
  public void addHandlerChain(RuntimeHandlerChain handler) {
    checkNotStarted();
    if (headHandler == null) {
      headHandler = handler;
    } else {
      headHandler.appendNext(handler);
    }
  }

  @Override
  public void handleTableChanged(
      DefaultTableRuntime tableRuntime, OptimizingStatus originalStatus) {
    if (headHandler != null) {
      headHandler.fireStatusChanged(tableRuntime, originalStatus);
    }
  }

  @Override
  public void handleTableChanged(
      DefaultTableRuntime tableRuntime, TableConfiguration originalConfig) {
    if (headHandler != null) {
      headHandler.fireConfigChanged(tableRuntime, originalConfig);
    }
  }

  @Override
  public void initialize() {
    checkNotStarted();

    List<TableRuntimeMeta> tableRuntimeMetaList =
        getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectTableRuntimeMetas);
    tableRuntimeMetaList.forEach(
        tableRuntimeMeta -> {
          DefaultTableRuntime tableRuntime = tableRuntimeMeta.constructTableRuntime(this);
          tableRuntimeMap.put(tableRuntime.getTableIdentifier(), tableRuntime);
          tableRuntime.registerMetric(MetricManager.getInstance().getGlobalRegistry());
        });

    if (headHandler != null) {
      headHandler.initialize(tableRuntimeMetaList);
    }
    if (tableExplorerExecutors == null) {
      int threadCount =
          serverConfiguration.getInteger(
              AmoroManagementConf.REFRESH_EXTERNAL_CATALOGS_THREAD_COUNT);
      int queueSize =
          serverConfiguration.getInteger(AmoroManagementConf.REFRESH_EXTERNAL_CATALOGS_QUEUE_SIZE);
      tableExplorerExecutors =
          new ThreadPoolExecutor(
              threadCount,
              threadCount,
              0,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>(queueSize),
              new ThreadFactoryBuilder()
                  .setNameFormat("table-explorer-executor-%d")
                  .setDaemon(true)
                  .build());
    }
    tableExplorerScheduler.scheduleAtFixedRate(
        this::exploreExternalCatalog, 0, externalCatalogRefreshingInterval, TimeUnit.MILLISECONDS);
    initialized.complete(true);
  }

  private DefaultTableRuntime getAndCheckExist(ServerTableIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier cannot be null");
    DefaultTableRuntime tableRuntime = getRuntime(tableIdentifier);
    Conditions.checkObjectNotExists(tableRuntime == null, tableIdentifier);
    return tableRuntime;
  }

  @Override
  public ServerTableIdentifier getServerTableIdentifier(TableIdentifier id) {
    return getAs(
        TableIdentifierMapper.class,
        mapper ->
            mapper.selectTableIdentifier(id.getCatalog(), id.getDatabase(), id.getTableName()));
  }

  private ServerTableIdentifier getOrSyncServerTableIdentifier(TableIdentifier id) {
    ServerTableIdentifier serverTableIdentifier = getServerTableIdentifier(id);
    if (serverTableIdentifier != null) {
      return serverTableIdentifier;
    }
    ServerCatalog serverCatalog = getServerCatalog(id.getCatalog());
    if (serverCatalog instanceof InternalCatalog) {
      return null;
    }
    try {
      AmoroTable<?> table = serverCatalog.loadTable(id.database, id.getTableName());
      TableIdentity identity =
          new TableIdentity(id.getDatabase(), id.getTableName(), table.format());
      syncTable((ExternalCatalog) serverCatalog, identity);
      return getServerTableIdentifier(id);
    } catch (NoSuchTableException e) {
      return null;
    }
  }

  @Override
  public DefaultTableRuntime getRuntime(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return tableRuntimeMap.get(tableIdentifier);
  }

  @Override
  public boolean contains(ServerTableIdentifier tableIdentifier) {
    checkStarted();
    return tableRuntimeMap.containsKey(tableIdentifier);
  }

  public void dispose() {
    tableExplorerScheduler.shutdown();
    if (tableExplorerExecutors != null) {
      tableExplorerExecutors.shutdown();
    }
    if (headHandler != null) {
      headHandler.dispose();
    }
  }

  @VisibleForTesting
  void exploreExternalCatalog() {
    long start = System.currentTimeMillis();
    LOG.info("Loading external catalogs, timestamp: {}", start);
    Map<String, ExternalCatalog> externalCatalogMap =
        getServerCatalogMap().values().stream()
            .filter(catalog -> catalog instanceof ExternalCatalog)
            .collect(Collectors.toMap(ServerCatalog::name, catalog -> (ExternalCatalog) catalog));

    LOG.info("Syncing external catalogs: {}", String.join(",", externalCatalogMap.keySet()));
    for (ExternalCatalog externalCatalog : externalCatalogMap.values()) {
      try {
        final List<CompletableFuture<Set<TableIdentity>>> tableIdentifiersFutures =
            Lists.newArrayList();
        externalCatalog
            .listDatabases()
            .forEach(
                database -> {
                  try {
                    tableIdentifiersFutures.add(
                        CompletableFuture.supplyAsync(
                            () -> {
                              try {
                                return externalCatalog.listTables(database).stream()
                                    .map(TableIdentity::new)
                                    .collect(Collectors.toSet());
                              } catch (Exception e) {
                                LOG.error(
                                    "TableExplorer list tables in database {} error", database, e);
                                return new HashSet<>();
                              }
                            },
                            tableExplorerExecutors));
                  } catch (RejectedExecutionException e) {
                    LOG.error(
                        "The queue of table explorer is full, please increase the queue size or thread count.");
                  }
                });
        Set<TableIdentity> tableIdentifiers =
            tableIdentifiersFutures.stream()
                .map(CompletableFuture::join)
                .reduce(
                    (a, b) -> {
                      a.addAll(b);
                      return a;
                    })
                .orElse(Sets.newHashSet());
        LOG.info(
            "Loaded {} tables from external catalog {}.",
            tableIdentifiers.size(),
            externalCatalog.name());
        Map<TableIdentity, ServerTableIdentifier> serverTableIdentifiers =
            getAs(
                    TableIdentifierMapper.class,
                    mapper -> mapper.selectTableIdentifiersByCatalog(externalCatalog.name()))
                .stream()
                .collect(Collectors.toMap(TableIdentity::new, tableIdentifier -> tableIdentifier));
        LOG.info(
            "Loaded {} tables from Amoro server catalog {}.",
            serverTableIdentifiers.size(),
            externalCatalog.name());
        final List<CompletableFuture<Void>> taskFutures = Lists.newArrayList();
        Sets.difference(tableIdentifiers, serverTableIdentifiers.keySet())
            .forEach(
                tableIdentity -> {
                  try {
                    taskFutures.add(
                        CompletableFuture.runAsync(
                            () -> {
                              try {
                                syncTable(externalCatalog, tableIdentity);
                              } catch (Exception e) {
                                LOG.error(
                                    "TableExplorer sync table {} error",
                                    tableIdentity.toString(),
                                    e);
                              }
                            },
                            tableExplorerExecutors));
                  } catch (RejectedExecutionException e) {
                    LOG.error(
                        "The queue of table explorer is full, please increase the queue size or thread count.");
                  }
                });
        Sets.difference(serverTableIdentifiers.keySet(), tableIdentifiers)
            .forEach(
                tableIdentity -> {
                  try {
                    taskFutures.add(
                        CompletableFuture.runAsync(
                            () -> {
                              try {
                                disposeTable(serverTableIdentifiers.get(tableIdentity));
                              } catch (Exception e) {
                                LOG.error(
                                    "TableExplorer dispose table {} error",
                                    tableIdentity.toString(),
                                    e);
                              }
                            },
                            tableExplorerExecutors));
                  } catch (RejectedExecutionException e) {
                    LOG.error(
                        "The queue of table explorer is full, please increase the queue size or thread count.");
                  }
                });
        taskFutures.forEach(CompletableFuture::join);
      } catch (Throwable e) {
        LOG.error("TableExplorer error", e);
      }
    }

    // Clear TableRuntime objects that do not correspond to a catalog.
    // This scenario is mainly due to the fact that TableRuntime objects were not cleaned up in a
    // timely manner during the process of dropping the catalog due to concurrency considerations.
    // It is permissible to have some erroneous states in the middle, as long as the final data is
    // consistent.
    Set<String> catalogNames =
        listCatalogMetas().stream().map(CatalogMeta::getCatalogName).collect(Collectors.toSet());
    for (DefaultTableRuntime tableRuntime : tableRuntimeMap.values()) {
      if (!catalogNames.contains(tableRuntime.getTableIdentifier().getCatalog())) {
        disposeTable(tableRuntime.getTableIdentifier());
      }
    }

    long end = System.currentTimeMillis();
    LOG.info("Syncing external catalogs took {} ms.", end - start);
  }

  private void checkStarted() {
    try {
      initialized.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void checkNotStarted() {
    if (initialized.isDone()) {
      throw new IllegalStateException("Table service has started.");
    }
  }

  private void syncTable(ExternalCatalog externalCatalog, TableIdentity tableIdentity) {
    AtomicBoolean tableRuntimeAdded = new AtomicBoolean(false);
    try {
      doAsTransaction(
          () ->
              externalCatalog.syncTable(
                  tableIdentity.getDatabase(),
                  tableIdentity.getTableName(),
                  tableIdentity.getFormat()),
          () -> {
            ServerTableIdentifier tableIdentifier =
                externalCatalog.getServerTableIdentifier(
                    tableIdentity.getDatabase(), tableIdentity.getTableName());
            tableRuntimeAdded.set(triggerTableAdded(externalCatalog, tableIdentifier));
          });
    } catch (Throwable t) {
      if (tableRuntimeAdded.get()) {
        revertTableRuntimeAdded(externalCatalog, tableIdentity);
      }
      throw t;
    }
  }

  private boolean triggerTableAdded(
      ServerCatalog catalog, ServerTableIdentifier serverTableIdentifier) {
    AmoroTable<?> table =
        catalog.loadTable(
            serverTableIdentifier.getDatabase(), serverTableIdentifier.getTableName());
    if (TableFormat.ICEBERG == table.format()) {
      if (TablePropertyUtil.isMixedTableStore(table.properties())) {
        return false;
      }
    }
    DefaultTableRuntime tableRuntime =
        new DefaultTableRuntime(serverTableIdentifier, this, table.properties());
    tableRuntimeMap.put(serverTableIdentifier, tableRuntime);
    tableRuntime.registerMetric(MetricManager.getInstance().getGlobalRegistry());
    if (headHandler != null) {
      headHandler.fireTableAdded(table, tableRuntime);
    }
    return true;
  }

  private void revertTableRuntimeAdded(
      ExternalCatalog externalCatalog, TableIdentity tableIdentity) {
    ServerTableIdentifier tableIdentifier =
        externalCatalog.getServerTableIdentifier(
            tableIdentity.getDatabase(), tableIdentity.getTableName());
    if (tableIdentifier != null) {
      tableRuntimeMap.remove(tableIdentifier);
    }
  }

  private void disposeTable(ServerTableIdentifier tableIdentifier) {
    doAs(
        TableIdentifierMapper.class,
        mapper ->
            mapper.deleteTableIdByName(
                tableIdentifier.getCatalog(),
                tableIdentifier.getDatabase(),
                tableIdentifier.getTableName()));
    Optional.ofNullable(tableRuntimeMap.remove(tableIdentifier))
        .ifPresent(
            tableRuntime -> {
              if (headHandler != null) {
                headHandler.fireTableRemoved(tableRuntime);
              }
              tableRuntime.dispose();
            });
  }

  private static class TableIdentity {

    private final String database;
    private final String tableName;

    private final TableFormat format;

    protected TableIdentity(TableIDWithFormat idWithFormat) {
      this.database = idWithFormat.getIdentifier().getDatabase();
      this.tableName = idWithFormat.getIdentifier().getTableName();
      this.format = idWithFormat.getTableFormat();
    }

    protected TableIdentity(ServerTableIdentifier serverTableIdentifier) {
      this.database = serverTableIdentifier.getDatabase();
      this.tableName = serverTableIdentifier.getTableName();
      this.format = serverTableIdentifier.getFormat();
    }

    protected TableIdentity(String database, String tableName, TableFormat format) {
      this.database = database;
      this.tableName = tableName;
      this.format = format;
    }

    public String getDatabase() {
      return database;
    }

    public String getTableName() {
      return tableName;
    }

    public TableFormat getFormat() {
      return format;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableIdentity that = (TableIdentity) o;
      return Objects.equal(database, that.database) && Objects.equal(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(database, tableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("database", database)
          .add("tableName", tableName)
          .toString();
    }
  }
}
