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

package org.apache.amoro.scheduler.optimizing;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.api.BlockableOperation;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.scheduler.table.DefaultTableRuntime;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SchedulingPolicy {

  private static final String SCHEDULING_POLICY_PROPERTY_NAME = "scheduling-policy";
  private static final String QUOTA = "quota";
  private static final String BALANCED = "balanced";

  private final Map<ServerTableIdentifier, DefaultTableRuntime> tableRuntimeMap = new HashMap<>();
  private volatile String policyName;
  private Comparator<DefaultTableRuntime> tableSorter;
  private final Lock tableLock = new ReentrantLock();

  public SchedulingPolicy(ResourceGroup group) {
    setTableSorterIfNeeded(group);
  }

  public void setTableSorterIfNeeded(ResourceGroup optimizerGroup) {
    tableLock.lock();
    try {
      policyName =
          Optional.ofNullable(optimizerGroup.getProperties())
              .orElseGet(Maps::newHashMap)
              .getOrDefault(SCHEDULING_POLICY_PROPERTY_NAME, QUOTA);
      if (policyName.equalsIgnoreCase(QUOTA)) {
        if (tableSorter == null || !(tableSorter instanceof QuotaOccupySorter)) {
          tableSorter = new QuotaOccupySorter();
        }
      } else if (policyName.equalsIgnoreCase(BALANCED)) {
        if (tableSorter == null || !(tableSorter instanceof BalancedSorter)) {
          tableSorter = new BalancedSorter();
        }
      } else {
        throw new IllegalArgumentException("Illegal scheduling policy: " + policyName);
      }
    } finally {
      tableLock.unlock();
    }
  }

  public String name() {
    return policyName;
  }

  public DefaultTableRuntime scheduleTable(Set<ServerTableIdentifier> skipSet) {
    tableLock.lock();
    try {
      fillSkipSet(skipSet);
      return tableRuntimeMap.values().stream()
          .filter(tableRuntime -> !skipSet.contains(tableRuntime.getTableIdentifier()))
          .min(tableSorter)
          .orElse(null);
    } finally {
      tableLock.unlock();
    }
  }

  public DefaultTableRuntime getTableRuntime(ServerTableIdentifier tableIdentifier) {
    tableLock.lock();
    try {
      return tableRuntimeMap.get(tableIdentifier);
    } finally {
      tableLock.unlock();
    }
  }

  private void fillSkipSet(Set<ServerTableIdentifier> originalSet) {
    long currentTime = System.currentTimeMillis();
    tableRuntimeMap.values().stream()
        .filter(
            tableRuntime ->
                !isTablePending(tableRuntime)
                    || tableRuntime.isBlocked(BlockableOperation.OPTIMIZE)
                    || currentTime - tableRuntime.getLastPlanTime()
                        < tableRuntime.getOptimizingConfig().getMinPlanInterval())
        .forEach(tableRuntime -> originalSet.add(tableRuntime.getTableIdentifier()));
  }

  private boolean isTablePending(DefaultTableRuntime tableRuntime) {
    return tableRuntime.getOptimizingStatus() == OptimizingStatus.PENDING
        && (tableRuntime.getLastOptimizedSnapshotId() != tableRuntime.getCurrentSnapshotId()
            || tableRuntime.getLastOptimizedChangeSnapshotId()
                != tableRuntime.getCurrentChangeSnapshotId());
  }

  public void addTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.put(tableRuntime.getTableIdentifier(), tableRuntime);
    } finally {
      tableLock.unlock();
    }
  }

  public void removeTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.remove(tableRuntime.getTableIdentifier());
    } finally {
      tableLock.unlock();
    }
  }

  @VisibleForTesting
  Map<ServerTableIdentifier, DefaultTableRuntime> getTableRuntimeMap() {
    return tableRuntimeMap;
  }

  private static class QuotaOccupySorter implements Comparator<DefaultTableRuntime> {

    private final Map<DefaultTableRuntime, Double> tableWeightMap = Maps.newHashMap();

    @Override
    public int compare(DefaultTableRuntime one, DefaultTableRuntime another) {
      return Double.compare(
          tableWeightMap.computeIfAbsent(one, DefaultTableRuntime::calculateQuotaOccupy),
          tableWeightMap.computeIfAbsent(another, DefaultTableRuntime::calculateQuotaOccupy));
    }
  }

  private static class BalancedSorter implements Comparator<DefaultTableRuntime> {
    @Override
    public int compare(DefaultTableRuntime one, DefaultTableRuntime another) {
      return Long.compare(
          Math.max(
              one.getLastFullOptimizingTime(),
              Math.max(one.getLastMinorOptimizingTime(), one.getLastMajorOptimizingTime())),
          Math.max(
              another.getLastFullOptimizingTime(),
              Math.max(
                  another.getLastMinorOptimizingTime(), another.getLastMajorOptimizingTime())));
    }
  }
}
