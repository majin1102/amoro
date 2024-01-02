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

package com.netease.arctic.optimizing;

import com.netease.arctic.ams.api.config.OptimizingConfig;
import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.expressions.Expressions;

public class TablePlanInput extends BaseOptimizingInput {
  private final ArcticTable table;
  private final OptimizingConfig optimizingConfig;
  private final long targetSnapshotId;
  private final long targetChangeSnapshotId;
  private final OptimizingType optimizingType;
  private final long lastOptimizingTime;
  private final Expressions filter;

  public TablePlanInput(
      ArcticTable table,
      OptimizingConfig optimizingConfig,
      long targetSnapshotId,
      long targetChangeSnapshotId,
      OptimizingType optimizingType,
      long lastOptimizingTime,
      Expressions filter) {
    this.table = table;
    this.optimizingConfig = optimizingConfig;
    this.targetSnapshotId = targetSnapshotId;
    this.targetChangeSnapshotId = targetChangeSnapshotId;
    this.optimizingType = optimizingType;
    this.lastOptimizingTime = lastOptimizingTime;
    this.filter = filter;
  }

  public ArcticTable getTable() {
    return table;
  }

  public OptimizingConfig getOptimizingConfig() {
    return optimizingConfig;
  }

  public long getTargetSnapshotId() {
    return targetSnapshotId;
  }

  public long getTargetChangeSnapshotId() {
    return targetChangeSnapshotId;
  }

  public OptimizingType getOptimizingType() {
    return optimizingType;
  }

  public long getLastOptimizingTime() {
    return lastOptimizingTime;
  }

  public Expressions getFilter() {
    return filter;
  }
}
