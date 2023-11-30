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

public abstract class ProcessState {

  private final long id;
  private final ProcessAction action;
  private final long startTime;
  private long endTime = -1L;

  protected ProcessState(long id, ProcessAction action, long startTime) {
    this.id = id;
    this.action = action;
    this.startTime = startTime;
  }

  protected ProcessState(long id, ProcessAction action) {
    this(id, action, System.currentTimeMillis());
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return action.getDescription();
  }

  public ProcessAction getAction() {
    return action;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getDuration() {
    return endTime > 0 ? endTime - startTime : System.currentTimeMillis() - startTime;
  }
}
