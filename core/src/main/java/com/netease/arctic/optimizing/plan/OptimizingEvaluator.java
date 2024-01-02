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
import com.netease.arctic.optimizing.scan.FileScanResult;
import com.netease.arctic.optimizing.scan.IcebergTableFileScanHelper;
import com.netease.arctic.optimizing.scan.MixedTableFileScanHelper;
import com.netease.arctic.optimizing.scan.TableFileScanHelper;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.utils.ArcticTableUtil;
import com.netease.arctic.utils.TablePropertyUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class OptimizingEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(OptimizingEvaluator.class);

  protected final ArcticTable arcticTable;
  protected final OptimizingConfig config;
  protected final long snapshotId;
  protected final long changeSnapshotId;
  protected boolean isInitialized = false;

  protected Map<String, PartitionEvaluator> partitionPlanMap = Maps.newHashMap();

  public OptimizingEvaluator(
      ArcticTable table, OptimizingConfig config, long snapshotId, long changeSnapshotId) {
    this.arcticTable = table;
    this.config = config;
    this.snapshotId = snapshotId;
    this.changeSnapshotId = changeSnapshotId;
  }

  public ArcticTable getArcticTable() {
    return arcticTable;
  }

  protected void initEvaluator() {
    long startTime = System.currentTimeMillis();
    TableFileScanHelper tableFileScanHelper;
    switch (arcticTable.format()) {
      case ICEBERG:
        tableFileScanHelper =
            new IcebergTableFileScanHelper(arcticTable.asUnkeyedTable(), snapshotId);
        break;
      case MIXED_ICEBERG:
      case MIXED_HIVE:
        tableFileScanHelper =
            new MixedTableFileScanHelper(arcticTable, changeSnapshotId, snapshotId);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported table format: " + arcticTable.format());
    }
    tableFileScanHelper.withPartitionFilter(getPartitionFilter());
    initPartitionPlans(tableFileScanHelper);
    isInitialized = true;
    LOG.info(
        "{} finished evaluating, found {} partitions that need optimizing in {} ms",
        arcticTable.id(),
        partitionPlanMap.size(),
        System.currentTimeMillis() - startTime);
  }

  protected Expression getPartitionFilter() {
    return Expressions.alwaysTrue();
  }

  private void initPartitionPlans(TableFileScanHelper tableFileScanHelper) {
    long startTime = System.currentTimeMillis();
    long count = 0;
    try (CloseableIterable<FileScanResult> results = tableFileScanHelper.scan()) {
      for (FileScanResult fileScanResult : results) {
        PartitionSpec partitionSpec =
            ArcticTableUtil.getArcticTablePartitionSpecById(
                arcticTable, fileScanResult.file().specId());
        StructLike partition = fileScanResult.file().partition();
        String partitionPath = partitionSpec.partitionToPath(partition);
        PartitionEvaluator evaluator =
            partitionPlanMap.computeIfAbsent(
                partitionPath,
                ignore -> buildPartitionEvaluator(Pair.of(partitionSpec.specId(), partition)));
        evaluator.addFile(fileScanResult.file(), fileScanResult.deleteFiles());
        count++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    LOG.info(
        "{} finished file scanning, scanning {} files in {} ms",
        arcticTable.id(),
        count,
        System.currentTimeMillis() - startTime);
    partitionPlanMap.values().removeIf(plan -> !plan.isNecessary());
  }

  protected Map<String, String> partitionProperties(Pair<Integer, StructLike> partition) {
    return TablePropertyUtil.getPartitionProperties(arcticTable, partition.second());
  }

  protected PartitionEvaluator buildPartitionEvaluator(Pair<Integer, StructLike> partition) {
    if (TableFormat.ICEBERG == arcticTable.format()) {
      return new CommonPartitionEvaluator(config, partition, System.currentTimeMillis());
    } else {
      return new MixedIcebergPartitionPlan.MixedIcebergPartitionEvaluator(
          config,
          partition,
          partitionProperties(partition),
          System.currentTimeMillis(),
          arcticTable.isKeyedTable());
    }
  }

  public boolean isNecessary() {
    if (!isInitialized) {
      initEvaluator();
    }
    return !partitionPlanMap.isEmpty();
  }
}
