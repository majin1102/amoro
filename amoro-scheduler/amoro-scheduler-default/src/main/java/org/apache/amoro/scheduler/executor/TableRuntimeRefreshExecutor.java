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

package org.apache.amoro.scheduler.executor;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.scheduler.TableConfiguration;
import org.apache.amoro.scheduler.optimizing.OptimizingProcess;
import org.apache.amoro.scheduler.optimizing.plan.OptimizingEvaluator;
import org.apache.amoro.scheduler.table.DefaultTableRuntime;
import org.apache.amoro.scheduler.table.TableManager;
import org.apache.amoro.table.MixedTable;

/** Executor that refreshes table runtimes and evaluates optimizing status periodically. */
public class TableRuntimeRefreshExecutor extends BaseTableExecutor {

  // 1 minutes
  private final long interval;

  public TableRuntimeRefreshExecutor(TableManager tableRuntimes, int poolSize, long interval) {
    super(tableRuntimes, poolSize);
    this.interval = interval;
  }

  @Override
  protected boolean enabled(DefaultTableRuntime tableRuntime) {
    return true;
  }

  protected long getNextExecutingTime(DefaultTableRuntime tableRuntime) {
    return Math.min(tableRuntime.getOptimizingConfig().getMinorLeastInterval() * 4L / 5, interval);
  }

  private void tryEvaluatingPendingInput(DefaultTableRuntime tableRuntime, MixedTable table) {
    if (tableRuntime.isOptimizingEnabled() && !tableRuntime.getOptimizingStatus().isProcessing()) {
      OptimizingEvaluator evaluator = new OptimizingEvaluator(tableRuntime, table);
      if (evaluator.isNecessary()) {
        OptimizingEvaluator.PendingInput pendingInput = evaluator.getPendingInput();
        logger.debug(
            "{} optimizing is necessary and get pending input {}",
            tableRuntime.getTableIdentifier(),
            pendingInput);
        tableRuntime.setPendingInput(pendingInput);
      }
    }
  }

  @Override
  public void handleConfigChanged(
      DefaultTableRuntime tableRuntime, TableConfiguration originalConfig) {
    // After disabling self-optimizing, close the currently running optimizing process.
    if (originalConfig.getOptimizingConfig().isEnabled()
        && !tableRuntime.getTableConfiguration().getOptimizingConfig().isEnabled()) {
      OptimizingProcess optimizingProcess = tableRuntime.getOptimizingProcess();
      if (optimizingProcess != null
          && optimizingProcess.getStatus() == OptimizingProcess.Status.RUNNING) {
        optimizingProcess.close();
      }
    }
  }

  @Override
  public void execute(DefaultTableRuntime tableRuntime) {
    try {
      long lastOptimizedSnapshotId = tableRuntime.getLastOptimizedSnapshotId();
      long lastOptimizedChangeSnapshotId = tableRuntime.getLastOptimizedChangeSnapshotId();
      AmoroTable<?> table = loadTable(tableRuntime);
      tableRuntime.refresh(table);
      MixedTable mixedTable = (MixedTable) table.originalTable();
      if ((mixedTable.isKeyedTable()
              && (lastOptimizedSnapshotId != tableRuntime.getCurrentSnapshotId()
                  || lastOptimizedChangeSnapshotId != tableRuntime.getCurrentChangeSnapshotId()))
          || (mixedTable.isUnkeyedTable()
              && lastOptimizedSnapshotId != tableRuntime.getCurrentSnapshotId())) {
        tryEvaluatingPendingInput(tableRuntime, mixedTable);
      }
    } catch (Throwable throwable) {
      logger.error("Refreshing table {} failed.", tableRuntime.getTableIdentifier(), throwable);
    }
  }
}
