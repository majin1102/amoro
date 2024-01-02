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
import com.netease.arctic.optimizing.plan.OptimizingPlanner;
import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.expressions.Expressions;

import java.util.Map;

public class IcebergTablePlanExecutor implements OptimizingExecutor<TablePlanOutput> {
  private final ArcticTable table;
  private final OptimizingConfig optimizingConfig;
  private final String ref;
  private final long targetSnapshotId;
  private final long targetChangeSnapshotId;
  private final Expressions filter;
  private final Map<String, String> options;

  public IcebergTablePlanExecutor(
      ArcticTable table,
      OptimizingConfig optimizingConfig,
      String ref,
      long targetSnapshotId,
      long targetChangeSnapshotId,
      Expressions filter,
      Map<String, String> options) {
    this.table = table;
    this.optimizingConfig = optimizingConfig;
    this.ref = ref;
    this.targetSnapshotId = targetSnapshotId;
    this.targetChangeSnapshotId = targetChangeSnapshotId;
    this.filter = filter;
    this.options = options;
  }

  @Override
  public TablePlanOutput execute() {
    return null;
  }

  public OptimizingPlanner buildTablePlanner() {
    // TODO
    return null;
    //    return new OptimizingPlanner();
  }
}
