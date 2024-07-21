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

package org.apache.amoro.admin;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableIdentifier;
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
import org.apache.amoro.shade.thrift.org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface TableFormatAdmin {

  /** Get the format supported by this descriptor. */
  List<TableFormat> getSupportedFormats();

  /** Get the table metadata information of the {@link AmoroTable}. */
  ServerTableMeta getTableDetail(AmoroTable<?> amoroTable);

  /** Get the snapshot information of the {@link AmoroTable}. */
  List<AmoroSnapshotsOfTable> getSnapshots(
      AmoroTable<?> amoroTable, String ref, OperationType operationType);

  /** Get the snapshot detail information of the {@link AmoroTable}. */
  List<PartitionFileBaseInfo> getSnapshotDetail(AmoroTable<?> amoroTable, long snapshotId);

  /** Get the DDL information of the {@link AmoroTable}. */
  List<DDLInfo> getTableOperations(AmoroTable<?> amoroTable);

  /** Get the partition information of the {@link AmoroTable}. */
  List<PartitionBaseInfo> getTablePartitions(AmoroTable<?> amoroTable);

  /** Get the file information of the {@link AmoroTable}. */
  List<PartitionFileBaseInfo> getTableFiles(
      AmoroTable<?> amoroTable, String partition, Integer specId);

  /** Get the paged optimizing process information of the {@link AmoroTable} and total size. */
  Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      AmoroTable<?> amoroTable, int limit, int offset);

  /** Get the paged optimizing process tasks information of the {@link AmoroTable}. */
  List<OptimizingTaskInfo> getOptimizingTaskInfos(AmoroTable<?> amoroTable, long processId);

  /** Get the tag information of the {@link AmoroTable}. */
  List<TagOrBranchInfo> getTableTags(AmoroTable<?> amoroTable);

  /** Get the branch information of the {@link AmoroTable}. */
  List<TagOrBranchInfo> getTableBranches(AmoroTable<?> amoroTable);

  /** Get the detail of hive table. Return null if hive table not supported */
  default HiveTableInfo getHiveTableDetail(
      TableIdentifier tableIdentifier, CatalogMeta catalogMeta) {
    return null;
  }

  /** Upgrade hive table to target table format */
  default void upgradeHiveTable(TableIdentifier tableIdentifier, CatalogMeta catalogMeta) {
    throw new UnsupportedOperationException();
  }
}
