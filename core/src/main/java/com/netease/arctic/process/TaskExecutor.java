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

package com.netease.arctic.process;

import java.io.Serializable;
import java.util.Map;

public interface TaskExecutor<O extends TaskExecutor.Output>
    extends Serializable {

  /* Execute task */
  O execute();

  /* Input of TaskExecutor */
  interface Input extends Serializable {
    /** Set option for this Input. */
    void option(String name, String value);

    /** Set options for this Input. */
    void options(Map<String, String> options);

    /** Get options. */
    Map<String, String> getOptions();
  }

  /* Output of TaskExecutor */
  interface Output extends Serializable {
    /** Get summary. */
    Map<String, String> summary();
  }
}
