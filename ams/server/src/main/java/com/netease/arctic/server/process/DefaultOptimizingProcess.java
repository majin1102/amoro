/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netease.arctic.server.process;

import com.netease.arctic.ams.api.OptimizingTaskId;
import com.netease.arctic.ams.api.process.OptimizingStage;
import com.netease.arctic.optimizing.OptimizingType;
import com.netease.arctic.optimizing.RewriteFilesInput;
import com.netease.arctic.optimizing.RewriteFilesOutput;
import com.netease.arctic.optimizing.TableCommitInput;
import com.netease.arctic.optimizing.TableCommitOutput;
import com.netease.arctic.optimizing.TablePlanInput;
import com.netease.arctic.optimizing.TablePlanOutput;
import com.netease.arctic.server.persistence.PersistentBase;
import com.netease.arctic.server.persistence.TaskFilesPersistence;
import com.netease.arctic.server.persistence.mapper.OptimizingMapper;
import com.netease.arctic.server.process.task.TableCommitInput;
import com.netease.arctic.server.process.task.TableCommitOutput;
import com.netease.arctic.server.process.task.TablePlanInput;
import com.netease.arctic.server.process.task.TablePlanOutput;
import com.netease.arctic.server.table.DefaultTableRuntime;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultOptimizingProcess extends ManagedProcess<DefaultOptimizingState> {

  // TODO wangtaohz COMMIT_TASK_SEQUENCE
  private static final int COMMMIT_TASK_SEQUENCE = Integer.MAX_VALUE;

  private final long processId;
  // TODO wangtaohz executing tasks
  private final Map<OptimizingTaskId, TaskRuntime<?, ?>> executingMap = Maps.newHashMap();
  private final LinkedList<TaskRuntime<?, ?>> taskQueue = new LinkedList<>();
  private final QuotaProvider optimizingQuota;
  private final Lock executingLock = new ReentrantLock();
  private volatile String summary;

  public DefaultOptimizingProcess(
      DefaultOptimizingState optimizingState, DefaultTableRuntime tableRuntime, boolean recover) {
    super(optimizingState, tableRuntime);
    optimizingQuota = tableRuntime.getOptimizingQuota();
    if (recover) {
      processId = getState().getId();
      state.saveProcessRecoverd(this);
    } else {
      processId = Math.max(System.currentTimeMillis(), getState().getId() + 1);
      state.saveProcessCreated(this);
    }
  }

  @Override
  public void submit() {
    Preconditions.checkState(
        state.getStage() != OptimizingStage.IDLE && state.getStage() != OptimizingStage.PENDING);
    if (state.getStage() == OptimizingStage.PLANNING) {
      TaskRuntime<TablePlanInput, TablePlanOutput> planTask = new TablePlanTaskBuilder().build();
      submitAsyncTask(planTask, () -> handlePlanningCompleted(planTask));
    } else {
      recoverTaskRuntimes();
    }
  }

  @Override
  protected void submitTask(TaskRuntime<?, ?> taskRuntime) {
    if (taskRuntime.getStatus() == TaskRuntime.Status.PLANNED
        || taskRuntime.getStatus() == TaskRuntime.Status.FAILED) {
      taskQueue.offer(taskRuntime);
    }
  }

  @Override
  protected void handleTaskFailed(TaskRuntime<?, ?> taskRuntime) {
    optimizingQuota.removeConsumer(taskRuntime, false);
  }

  private void handlePlanningCompleted(TaskRuntime<TablePlanInput, TablePlanOutput> planTask) {
    LOG.info(
        "Completed planning table {} with {} tasks",
        tableRuntime.getTableIdentifier(),
        executingMap.size());
    // TODO
    // generate executingMap data
    List<TaskRuntime<RewriteFilesInput, RewriteFilesOutput>> optimizingTasks = null;
    new Persistence()
        .persistOptimizingTasksAndStage(planTask.getOutput().getOptimizingType(), optimizingTasks);
    optimizingTasks.forEach(this::handleSingleOptimizingTask);
    completeSubmitting();
  }

  @Override
  public TaskRuntime<?, ?> pollTask() {
    executingLock.lock();
    try {
      checkClosed();
      return taskQueue.poll();
    } finally {
      executingLock.unlock();
    }
  }

  @Override
  public void closeInternal() {
    executingLock.lock();
    try {
      if (!isClosed()) {
        executingMap.values().forEach(TaskRuntime::tryCanceling);
        taskQueue.clear();
      }
    } finally {
      executingLock.unlock();
    }
  }

  @Override
  protected void retry(TaskRuntime<?, ?> taskRuntime) {
    executingLock.lock();
    try {
      taskQueue.addFirst(taskRuntime);
    } finally {
      executingLock.unlock();
    }
  }

  private void handleSingleOptimizingTask(
      TaskRuntime<RewriteFilesInput, RewriteFilesOutput> taskRuntime) {
    submitAsyncTask(
        taskRuntime,
        () -> {
          if (taskRuntime.getStatus() == TaskRuntime.Status.SUCCESS) {
            optimizingQuota.removeConsumer(taskRuntime, false);
            if (allTasksPrepared()) {
              TaskRuntime<TableCommitInput, TableCommitOutput> committingTask =
                  new TableCommitTaskBuilder().build();
              new Persistence().persistCommittingTaskAndStage(committingTask);
              submitAsyncTask(committingTask, () -> handleCommittingCompleted(committingTask));
            }
          }
        });
  }

  private void handleCommittingCompleted(TaskRuntime<?, ?> taskRuntime) {
    LOG.info(
        "Completed committing table {} with {} tasks",
        tableRuntime.getTableIdentifier(),
        executingMap.size());
    optimizingQuota.removeConsumer(taskRuntime, state.getOptimizingType() == OptimizingType.MAJOR);
    complete();
  }

  @Override
  public TaskRuntime<?, ?> getTaskRuntime(OptimizingTaskId taskId) {
    return executingMap.get(taskId);
  }

  @Override
  public List<TaskRuntime<?, ?>> getTaskRuntimes() {
    return new ArrayList<>(executingMap.values());
  }

  /**
   * if all tasks are Prepared
   *
   * @return true if tasks is not empty and all Prepared
   */
  private boolean allTasksPrepared() {
    if (!executingMap.isEmpty()) {
      return executingMap.values().stream()
          .allMatch(t -> t.getStatus() == TaskRuntime.Status.SUCCESS);
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private void recoverTaskRuntimes() {
    List<TaskRuntime<?, ?>> taskRuntimes = new Persistence().selectTaskRuntimes();
    Map<Integer, RewriteFilesInput> inputs = TaskFilesPersistence.loadTaskInputs(processId);
    taskRuntimes.forEach(
        taskRuntime -> {
          executingMap.put(taskRuntime.getTaskId(), taskRuntime);
          if (taskRuntime.getTaskId().getTaskId() == COMMMIT_TASK_SEQUENCE) {
            TaskRuntime<TableCommitInput, TableCommitOutput> committingTask =
                (TaskRuntime<TableCommitInput, TableCommitOutput>) taskRuntime;
            // TODO wangtaohz fix loading from sysdb
            committingTask.setInput(new TableCommitInput());
            submitAsyncTask(committingTask, () -> handleCommittingCompleted(committingTask));
          } else {
            TaskRuntime<RewriteFilesInput, RewriteFilesOutput> optimizingTask =
                (TaskRuntime<RewriteFilesInput, RewriteFilesOutput>) taskRuntime;
            optimizingTask.setInput(inputs.get(taskRuntime.getTaskId().getTaskId()));
            submitAsyncTask(optimizingTask, () -> handleSingleOptimizingTask(optimizingTask));
          }
        });
    optimizingQuota.addConsumer(state, executingMap.values());
  }

  public String getSummary() {
    return summary;
  }

  @Override
  public long getId() {
    return processId;
  }

  private class Persistence extends PersistentBase {

    public List<TaskRuntime<?, ?>> selectTaskRuntimes() {
      return getAs(
          OptimizingMapper.class,
          mapper ->
              mapper.selectTaskRuntimes(tableRuntime.getTableIdentifier().getId(), processId));
    }

    public void persistCommittingTaskAndStage(
        TaskRuntime<TableCommitInput, TableCommitOutput> committingTask) {
      doAsTransaction(
          () -> {
            doAs(OptimizingMapper.class, mapper -> mapper.insertTaskRuntime(committingTask));
            state.saveCommittingStage();
            executingMap.put(committingTask.getTaskId(), committingTask);
            optimizingQuota.addConsumer(state, Lists.newArrayList(committingTask));
          });
    }

    public void persistOptimizingTasksAndStage(
        OptimizingType optimizingType,
        List<TaskRuntime<RewriteFilesInput, RewriteFilesOutput>> optimizingTasks) {
      doAsTransaction(
          () -> {
            doAs(OptimizingMapper.class, mapper -> mapper.insertOptimizingTasks(optimizingTasks));
            TaskFilesPersistence.persistOptimizingInputs(state.getId(), optimizingTasks);
            state.saveOptimizingStage(optimizingType, optimizingTasks.size(), summary);
            optimizingTasks.forEach(task -> executingMap.put(task.getTaskId(), task));
          });
    }
  }

  private class TableCommitTaskBuilder implements TaskBuilder<TableCommitInput, TableCommitOutput> {

    @Override
    public TaskRuntime<TableCommitInput, TableCommitOutput> build() {
      return null;
    }
  }

  private class TablePlanTaskBuilder implements TaskBuilder<TablePlanInput, TablePlanOutput> {

    @Override
    public TaskRuntime<TablePlanInput, TablePlanOutput> build() {
      return null;
    }
  }
}
