package org.apache.amoro.server.optimizing;

import java.util.List;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.server.resource.OptimizerThread;

public interface SchedulingQueue {

  String getContainerName();

  void refreshTable(TableRuntime tableRuntime);

  void releaseTable(TableRuntime tableRuntime);

  TaskRuntime<?> pollTask(OptimizerThread thread, long maxWaitTime, boolean breakQuotaLimit);

  TaskRuntime<?> pollTask(OptimizerThread thread, long maxWaitTime);

  void completedTask(TaskRuntime<?> taskRuntime);

  void ackTask(TaskRuntime<?> taskRuntime);

  List<TaskRuntime<?>> collectTasks();
}
