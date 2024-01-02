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

package com.netease.arctic.hive.optimizing.plan;

import com.netease.arctic.ams.api.config.OptimizingConfig;
import com.netease.arctic.hive.table.SupportHive;
import com.netease.arctic.optimizing.plan.OptimizingEvaluator;
import com.netease.arctic.optimizing.plan.PartitionEvaluator;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.Pair;

public class HiveOptimizingEvaluator extends OptimizingEvaluator {
  public HiveOptimizingEvaluator(
      SupportHive table, OptimizingConfig config, long snapshotId, long changeSnapshotId) {
    super(table, config, snapshotId, changeSnapshotId);
  }

  protected SupportHive hiveTable() {
    return (SupportHive) arcticTable;
  }

  @Override
  protected PartitionEvaluator buildPartitionEvaluator(Pair<Integer, StructLike> partition) {
    return new MixedHivePartitionPlan.MixedHivePartitionEvaluator(
        config,
        partition,
        partitionProperties(partition),
        hiveTable().hiveLocation(),
        System.currentTimeMillis(),
        arcticTable.isKeyedTable());
  }
}
