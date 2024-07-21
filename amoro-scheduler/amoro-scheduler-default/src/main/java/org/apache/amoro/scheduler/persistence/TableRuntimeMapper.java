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

package org.apache.amoro.scheduler.persistence;

import org.apache.amoro.scheduler.table.DefaultTableRuntime;
import org.apache.amoro.scheduler.table.TableRuntimeMeta;
import org.apache.amoro.server.persistence.converter.JsonObjectConverter;
import org.apache.amoro.server.persistence.converter.Long2TsConverter;
import org.apache.amoro.server.persistence.converter.MapLong2StringConverter;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface TableRuntimeMapper {

  @Update(
      "UPDATE table_runtime SET current_snapshot_id = #{runtime.currentSnapshotId},"
          + " current_change_snapshotId =#{runtime.currentChangeSnapshotId},"
          + " last_optimized_snapshotId = #{runtime.lastOptimizedSnapshotId},"
          + " last_optimized_change_snapshotId = #{runtime.lastOptimizedChangeSnapshotId},"
          + " last_major_optimizing_time = #{runtime.lastMajorOptimizingTime, "
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " last_minor_optimizing_time = #{runtime.lastMinorOptimizingTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " last_full_optimizing_time = #{runtime.lastFullOptimizingTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " optimizing_status = #{runtime.optimizingStatus},"
          + " optimizing_status_start_time = #{runtime.currentStatusStartTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " optimizing_process_id = #{runtime.processId},"
          + " optimizer_group = #{runtime.optimizerGroup},"
          + " table_config = #{runtime.tableConfiguration,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.JsonObjectConverter},"
          + " pending_input = #{runtime.pendingInput, jdbcType=VARCHAR,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.JsonObjectConverter}"
          + " WHERE table_id = #{runtime.tableIdentifier.id}")
  void updateTableRuntime(@Param("runtime") DefaultTableRuntime runtime);

  @Delete("DELETE FROM table_runtime WHERE table_id = #{tableId}")
  void deleteOptimizingRuntime(@Param("tableId") long tableId);

  @Insert(
      "INSERT INTO table_runtime (table_id, catalog_name, db_name, table_name, current_snapshot_id,"
          + " current_change_snapshotId, last_optimized_snapshotId, last_optimized_change_snapshotId,"
          + " last_major_optimizing_time, last_minor_optimizing_time,"
          + " last_full_optimizing_time, optimizing_status, optimizing_status_start_time, optimizing_process_id,"
          + " optimizer_group, table_config, pending_input) VALUES"
          + " (#{runtime.tableIdentifier.id}, #{runtime.tableIdentifier.catalog},"
          + " #{runtime.tableIdentifier.database}, #{runtime.tableIdentifier.tableName}, #{runtime"
          + ".currentSnapshotId},"
          + " #{runtime.currentChangeSnapshotId}, #{runtime.lastOptimizedSnapshotId},"
          + " #{runtime.lastOptimizedChangeSnapshotId}, #{runtime.lastMajorOptimizingTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.lastMinorOptimizingTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.lastFullOptimizingTime,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.optimizingStatus},"
          + " #{runtime.currentStatusStartTime, "
          + " typeHandler=org.apache.amoro.server.persistence.converter.Long2TsConverter},"
          + " #{runtime.processId}, #{runtime.optimizerGroup},"
          + " #{runtime.tableConfiguration,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.JsonObjectConverter},"
          + " #{runtime.pendingInput, jdbcType=VARCHAR,"
          + " typeHandler=org.apache.amoro.server.persistence.converter.JsonObjectConverter})")
  void insertTableRuntime(@Param("runtime") DefaultTableRuntime runtime);

  @Select(
      "SELECT a.table_id, a.catalog_name, a.db_name, a.table_name, i.format, a.current_snapshot_id, a"
          + ".current_change_snapshotId, a.last_optimized_snapshotId, a.last_optimized_change_snapshotId,"
          + " a.last_major_optimizing_time, a.last_minor_optimizing_time, a.last_full_optimizing_time, a.optimizing_status,"
          + " a.optimizing_status_start_time, a.optimizing_process_id,"
          + " a.optimizer_group, a.table_config, a.pending_input, b.optimizing_type, b.target_snapshot_id,"
          + " b.target_change_snapshot_id, b.plan_time, b.from_sequence, b.to_sequence FROM table_runtime a"
          + " INNER JOIN table_identifier i ON a.table_id = i.table_id "
          + " LEFT JOIN table_optimizing_process b ON a.optimizing_process_id = b.process_id")
  @Results({
    @Result(property = "tableId", column = "table_id"),
    @Result(property = "catalogName", column = "catalog_name"),
    @Result(property = "dbName", column = "db_name"),
    @Result(property = "tableName", column = "table_name"),
    @Result(property = "format", column = "format"),
    @Result(property = "currentSnapshotId", column = "current_snapshot_id"),
    @Result(property = "currentChangeSnapshotId", column = "current_change_snapshotId"),
    @Result(property = "lastOptimizedSnapshotId", column = "last_optimized_snapshotId"),
    @Result(
        property = "lastOptimizedChangeSnapshotId",
        column = "last_optimized_change_snapshotId"),
    @Result(
        property = "lastMajorOptimizingTime",
        column = "last_major_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(
        property = "lastMinorOptimizingTime",
        column = "last_minor_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(
        property = "lastFullOptimizingTime",
        column = "last_full_optimizing_time",
        typeHandler = Long2TsConverter.class),
    @Result(property = "tableStatus", column = "optimizing_status"),
    @Result(
        property = "currentStatusStartTime",
        column = "optimizing_status_start_time",
        typeHandler = Long2TsConverter.class),
    @Result(property = "optimizingProcessId", column = "optimizing_process_id"),
    @Result(property = "optimizerGroup", column = "optimizer_group"),
    @Result(
        property = "tableConfig",
        column = "table_config",
        typeHandler = JsonObjectConverter.class),
    @Result(
        property = "pendingInput",
        column = "pending_input",
        typeHandler = JsonObjectConverter.class),
    @Result(property = "optimizingType", column = "optimizing_type"),
    @Result(property = "targetSnapshotId", column = "target_snapshot_id"),
    @Result(property = "targetChangeSnapshotId", column = "target_change_napshot_id"),
    @Result(property = "planTime", column = "plan_time", typeHandler = Long2TsConverter.class),
    @Result(
        property = "fromSequence",
        column = "from_sequence",
        typeHandler = MapLong2StringConverter.class),
    @Result(
        property = "toSequence",
        column = "to_sequence",
        typeHandler = MapLong2StringConverter.class)
  })
  List<TableRuntimeMeta> selectTableRuntimeMetas();
}
