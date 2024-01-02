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

package com.netease.arctic.optimizing.plan;

import com.netease.arctic.ams.api.TableFormat;
import com.netease.arctic.ams.api.config.OptimizingConfig;
import com.netease.arctic.ams.api.process.PendingInput;
import com.netease.arctic.optimizing.OptimizingType;
import com.netease.arctic.optimizing.RewriteFilesInput;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.ExpressionUtil;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OptimizingPlanner extends OptimizingEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizingPlanner.class);

  private final Expression partitionFilter;

  private final double availableCore;
  protected final long planTime;
  private OptimizingType optimizingType;
  private List<RewriteFilesInput> tasks;

  private final long maxInputSizePerThread;

  public OptimizingPlanner(
      ArcticTable table,
      OptimizingConfig config,
      PendingInput pendingInput,
      double availableCore,
      long maxInputSizePerThread) {
    super(
        table,
        config,
        pendingInput.getCurrentSnapshotId(),
        pendingInput.getCurrentChangeSnapshotId());
    this.partitionFilter =
        pendingInput.getPartitions().entrySet().stream()
            .map(
                entry ->
                    ExpressionUtil.convertPartitionDataToDataFilter(
                        table, entry.getKey(), entry.getValue()))
            .reduce(Expressions::or)
            .orElse(Expressions.alwaysTrue());
    this.availableCore = availableCore;
    this.planTime = System.currentTimeMillis();
    this.maxInputSizePerThread = maxInputSizePerThread;
  }

  @Override
  protected PartitionEvaluator buildPartitionEvaluator(Pair<Integer, StructLike> partition) {
    if (TableFormat.ICEBERG == arcticTable.format()) {
      return new IcebergPartitionPlan(arcticTable, partition, config, planTime);
    } else {
      return new MixedIcebergPartitionPlan(arcticTable, partition, config, planTime);
    }
  }

  @Override
  protected Expression getPartitionFilter() {
    return partitionFilter;
  }

  @Override
  public boolean isNecessary() {
    if (!super.isNecessary()) {
      return false;
    }
    return !planTasks().isEmpty();
  }

  public List<RewriteFilesInput> planTasks() {
    if (this.tasks != null) {
      return this.tasks;
    }
    long startTime = System.nanoTime();
    if (!isInitialized) {
      initEvaluator();
    }
    if (!super.isNecessary()) {
      LOG.debug("Table {} skip planning", arcticTable.id());
      return cacheAndReturnTasks(Collections.emptyList());
    }

    List<PartitionEvaluator> evaluators = new ArrayList<>(partitionPlanMap.values());
    // prioritize partitions with high cost to avoid starvation
    evaluators.sort(Comparator.comparing(PartitionEvaluator::getWeight, Comparator.reverseOrder()));

    double maxInputSize = maxInputSizePerThread * availableCore;
    List<AbstractPartitionPlan> actualPartitionPlans = Lists.newArrayList();
    long actualInputSize = 0;
    for (PartitionEvaluator evaluator : evaluators) {
      actualPartitionPlans.add((AbstractPartitionPlan) evaluator);
      actualInputSize += evaluator.getCost();
      if (actualInputSize > maxInputSize) {
        break;
      }
    }

    double avgThreadCost = actualInputSize / availableCore;
    List<RewriteFilesInput> tasks = Lists.newArrayList();
    for (AbstractPartitionPlan partitionPlan : actualPartitionPlans) {
      tasks.addAll(partitionPlan.splitTasks((int) (actualInputSize / avgThreadCost)));
    }
    if (!tasks.isEmpty()) {
      if (evaluators.stream()
          .anyMatch(evaluator -> evaluator.getOptimizingType() == OptimizingType.FULL)) {
        optimizingType = OptimizingType.FULL;
      } else if (evaluators.stream()
          .anyMatch(evaluator -> evaluator.getOptimizingType() == OptimizingType.MAJOR)) {
        optimizingType = OptimizingType.MAJOR;
      } else {
        optimizingType = OptimizingType.MINOR;
      }
    }
    long endTime = System.nanoTime();
    LOG.info(
        "{} finish plan, type = {}, get {} tasks, cost {} ns, {} ms maxInputSize {} actualInputSize {}",
        arcticTable.id(),
        getOptimizingType(),
        tasks.size(),
        endTime - startTime,
        (endTime - startTime) / 1_000_000,
        maxInputSize,
        actualInputSize);
    return cacheAndReturnTasks(tasks);
  }

  private List<RewriteFilesInput> cacheAndReturnTasks(List<RewriteFilesInput> tasks) {
    this.tasks = tasks;
    return this.tasks;
  }

  public long getPlanTime() {
    return planTime;
  }

  public OptimizingType getOptimizingType() {
    return optimizingType;
  }
}
