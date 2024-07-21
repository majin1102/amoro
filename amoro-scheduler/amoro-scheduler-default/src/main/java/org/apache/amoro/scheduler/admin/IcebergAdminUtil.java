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

package org.apache.amoro.scheduler.admin;

import org.apache.amoro.admin.model.AMSColumnInfo;
import org.apache.amoro.admin.model.AMSPartitionField;
import org.apache.amoro.admin.model.OptimizingProcessInfo;
import org.apache.amoro.scheduler.optimizing.MetricsSummary;
import org.apache.amoro.scheduler.optimizing.OptimizingProcessMeta;
import org.apache.amoro.scheduler.optimizing.OptimizingTaskMeta;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;

public class IcebergAdminUtil {

  public static OptimizingProcessInfo buildProcessInfo(
      OptimizingProcessMeta meta, List<OptimizingTaskMeta> optimizingTaskStats) {
    if (meta == null) {
      return null;
    }
    OptimizingProcessInfo result = new OptimizingProcessInfo();

    if (optimizingTaskStats != null) {
      int successTasks = 0;
      int runningTasks = 0;
      for (OptimizingTaskMeta optimizingTaskStat : optimizingTaskStats) {
        switch (optimizingTaskStat.getStatus()) {
          case SUCCESS:
            successTasks++;
            break;
          case SCHEDULED:
          case ACKED:
            runningTasks++;
            break;
        }
      }
      result.setTotalTasks(optimizingTaskStats.size());
      result.setSuccessTasks(successTasks);
      result.setRunningTasks(runningTasks);
    }
    MetricsSummary summary = meta.getSummary();
    if (summary != null) {
      result.setInputFiles(summary.getInputFilesStatistics());
      result.setOutputFiles(summary.getOutputFilesStatistics());
    }

    result.setTableId(meta.getTableId());
    result.setCatalogName(meta.getCatalogName());
    result.setDbName(meta.getDbName());
    result.setTableName(meta.getTableName());

    result.setProcessId(meta.getProcessId());
    result.setStartTime(meta.getPlanTime());
    result.setOptimizingType(meta.getOptimizingType().getStatus().displayValue());
    result.setStatus(meta.getStatus().toString());
    result.setFailReason(meta.getFailReason());
    result.setDuration(
        meta.getEndTime() > 0
            ? meta.getEndTime() - meta.getPlanTime()
            : System.currentTimeMillis() - meta.getPlanTime());
    result.setFinishTime(meta.getEndTime());
    result.setSummary(meta.getSummary().summaryAsMap(true));
    return result;
  }

  public static AMSPartitionField buildPartitionFieldFrom(Schema schema, PartitionField pf) {
    return new PartitionFieldBuilder()
        .field(pf.name())
        .sourceField(schema.findColumnName(pf.sourceId()))
        .transform(pf.transform().toString())
        .fieldId(pf.fieldId())
        .sourceFieldId(pf.sourceId())
        .build();
  }

  public static class PartitionFieldBuilder {
    String field;
    String sourceField;
    String transform;
    Integer fieldId;
    Integer sourceFieldId;

    public PartitionFieldBuilder field(String field) {
      this.field = field;
      return this;
    }

    public PartitionFieldBuilder sourceField(String sourceField) {
      this.sourceField = sourceField;
      return this;
    }

    public PartitionFieldBuilder transform(String transform) {
      this.transform = transform;
      return this;
    }

    public PartitionFieldBuilder fieldId(Integer fieldId) {
      this.fieldId = fieldId;
      return this;
    }

    public PartitionFieldBuilder sourceFieldId(Integer sourceFieldId) {
      this.sourceFieldId = sourceFieldId;
      return this;
    }

    public AMSPartitionField build() {
      return new AMSPartitionField(field, sourceField, transform, fieldId, sourceFieldId);
    }
  }

  public static AMSColumnInfo buildColumnsInfoFrom(
      Schema schema, PrimaryKeySpec.PrimaryKeyField pkf) {
    return buildColumnsInfoFrom(schema.findField(pkf.fieldName()));
  }

  public static AMSColumnInfo buildColumnsInfoFrom(Types.NestedField field) {
    if (field == null) {
      return null;
    }
    return new ColumnInfoBuilder()
        .field(field.name())
        .type(field.type().toString())
        .required(field.isRequired())
        .comment(field.doc())
        .build();
  }

  private static class ColumnInfoBuilder {
    String field;
    String type;
    boolean required;
    String comment;

    public ColumnInfoBuilder field(String field) {
      this.field = field;
      return this;
    }

    public ColumnInfoBuilder type(String type) {
      this.type = type;
      return this;
    }

    public ColumnInfoBuilder required(Boolean isRequired) {
      this.required = isRequired;
      return this;
    }

    public ColumnInfoBuilder comment(String comment) {
      this.comment = comment;
      return this;
    }

    public AMSColumnInfo build() {
      return new AMSColumnInfo(field, type, required, comment);
    }
  }
}
