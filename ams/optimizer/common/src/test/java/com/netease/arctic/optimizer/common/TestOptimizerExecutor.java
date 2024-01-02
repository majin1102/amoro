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

package com.netease.arctic.optimizer.common;

import com.google.common.collect.Maps;
import com.netease.arctic.ams.api.OptimizerRegisterInfo;
import com.netease.arctic.ams.api.OptimizingTask;
import com.netease.arctic.ams.api.OptimizingTaskId;
import com.netease.arctic.ams.api.OptimizingTaskResult;
import com.netease.arctic.optimizing.OptimizingInputProperties;
import com.netease.arctic.process.TaskExecutor;
import com.netease.arctic.process.TaskExecutorFactory;
import com.netease.arctic.utils.SerializationUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestOptimizerExecutor extends OptimizerTestBase {

  private static final String FAILED_TASK_MESSAGE = "Execute Task failed";

  private OptimizerExecutor optimizerExecutor;

  @Before
  public void startOptimizer() {
    OptimizerConfig optimizerConfig =
        OptimizerTestHelpers.buildOptimizerConfig(TEST_AMS.getServerUrl());
    optimizerExecutor = new OptimizerExecutor(optimizerConfig, 0);
    new Thread(optimizerExecutor::start).start();
  }

  @After
  public void stopOptimizer() {
    optimizerExecutor.stop();
  }

  @Test
  public void testWaitForToken() throws InterruptedException {
    TEST_AMS.getOptimizerHandler().offerTask(TestTaskInput.successInput(1).toTask(0, 0));
    TimeUnit.MILLISECONDS.sleep(OptimizerTestHelpers.CALL_AMS_INTERVAL * 2);
    Assert.assertEquals(1, TEST_AMS.getOptimizerHandler().getPendingTasks().size());
  }

  @Test
  public void testExecuteTaskSuccess() throws InterruptedException, TException {
    TEST_AMS.getOptimizerHandler().authenticate(new OptimizerRegisterInfo());
    String token =
        TEST_AMS.getOptimizerHandler().getRegisteredOptimizers().keySet().iterator().next();
    TEST_AMS.getOptimizerHandler().offerTask(TestTaskInput.successInput(1).toTask(0, 0));
    Assert.assertEquals(1, TEST_AMS.getOptimizerHandler().getPendingTasks().size());
    optimizerExecutor.setToken(token);
    TimeUnit.MILLISECONDS.sleep(OptimizerTestHelpers.CALL_AMS_INTERVAL * 2);
    Assert.assertEquals(0, TEST_AMS.getOptimizerHandler().getPendingTasks().size());
    Assert.assertTrue(TEST_AMS.getOptimizerHandler().getCompletedTasks().containsKey(token));
    Assert.assertEquals(1, TEST_AMS.getOptimizerHandler().getCompletedTasks().get(token).size());
    OptimizingTaskResult taskResult =
        TEST_AMS.getOptimizerHandler().getCompletedTasks().get(token).get(0);
    Assert.assertEquals(new OptimizingTaskId(0, 0), taskResult.getTaskId());
    Assert.assertNull(taskResult.getErrorMessage());
    TestTaskOutput output = SerializationUtil.simpleDeserialize(taskResult.getTaskOutput());
    Assert.assertEquals(1, output.inputId());
  }

  @Test
  public void testExecuteTaskFailed() throws InterruptedException, TException {
    TEST_AMS.getOptimizerHandler().authenticate(new OptimizerRegisterInfo());
    String token =
        TEST_AMS.getOptimizerHandler().getRegisteredOptimizers().keySet().iterator().next();
    TEST_AMS.getOptimizerHandler().offerTask(TestTaskInput.failedInput(1).toTask(0, 0));
    Assert.assertEquals(1, TEST_AMS.getOptimizerHandler().getPendingTasks().size());
    optimizerExecutor.setToken(token);
    TimeUnit.MILLISECONDS.sleep(OptimizerTestHelpers.CALL_AMS_INTERVAL * 2);
    Assert.assertEquals(0, TEST_AMS.getOptimizerHandler().getPendingTasks().size());
    Assert.assertTrue(TEST_AMS.getOptimizerHandler().getCompletedTasks().containsKey(token));
    Assert.assertEquals(1, TEST_AMS.getOptimizerHandler().getCompletedTasks().get(token).size());
    OptimizingTaskResult taskResult =
        TEST_AMS.getOptimizerHandler().getCompletedTasks().get(token).get(0);
    Assert.assertEquals(new OptimizingTaskId(0, 0), taskResult.getTaskId());
    Assert.assertNull(taskResult.getTaskOutput());
    Assert.assertTrue(taskResult.getErrorMessage().contains(FAILED_TASK_MESSAGE));
  }

  public static class TestTaskInput implements TaskExecutor.Input {
    private final int inputId;
    private final boolean executeSuccess;

    private TestTaskInput(int inputId, boolean executeSuccess) {
      this.inputId = inputId;
      this.executeSuccess = executeSuccess;
    }

    public static TestTaskInput successInput(int inputId) {
      return new TestTaskInput(inputId, true);
    }

    public static TestTaskInput failedInput(int inputId) {
      return new TestTaskInput(inputId, false);
    }

    private int inputId() {
      return inputId;
    }

    public OptimizingTask toTask(long processId, int taskId) {
      OptimizingTask optimizingTask = new OptimizingTask(new OptimizingTaskId(processId, taskId));
      optimizingTask.setTaskInput(SerializationUtil.simpleSerialize(this));
      Map<String, String> inputProperties = Maps.newHashMap();
      inputProperties.put(
          OptimizingInputProperties.TASK_EXECUTOR_FACTORY_IMPL,
          TestTaskExecutorFactory.class.getName());
      optimizingTask.setProperties(inputProperties);
      return optimizingTask;
    }

    @Override
    public void option(String name, String value) {}

    @Override
    public void options(Map<String, String> options) {}

    @Override
    public Map<String, String> getOptions() {
      return null;
    }
  }

  public static class TestTaskExecutorFactory
      implements TaskExecutorFactory<TestTaskInput, TestTaskOutput> {

    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public TaskExecutor<TestTaskOutput> createExecutor(TestTaskInput input) {
      return new TestOptimizingExecutor(input);
    }
  }

  public static class TestOptimizingExecutor implements TaskExecutor<TestTaskOutput> {

    private final TestTaskInput input;

    private TestOptimizingExecutor(TestTaskInput input) {
      this.input = input;
    }

    @Override
    public TestTaskOutput execute() {
      if (input.executeSuccess) {
        return new TestTaskOutput(input.inputId());
      } else {
        throw new IllegalStateException(FAILED_TASK_MESSAGE);
      }
    }
  }

  public static class TestTaskOutput implements TaskExecutor.Output {

    private final int inputId;

    private TestTaskOutput(int inputId) {
      this.inputId = inputId;
    }

    private int inputId() {
      return inputId;
    }

    @Override
    public Map<String, String> summary() {
      return null;
    }
  }
}
