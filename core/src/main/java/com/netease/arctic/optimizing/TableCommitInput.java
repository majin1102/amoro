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

package com.netease.arctic.optimizing;

import com.netease.arctic.table.ArcticTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.util.Map;
import java.util.Set;

public class TableCommitInput extends BaseOptimizingInput {
  private ArcticTable table;
  private long targetSnapshotId;
  private Set<DataFile> dataFilesToReplace;
  private Set<DeleteFile> deleteFilesToReplace;
  private Set<DataFile> dataFilesToAdd;
  private Set<DeleteFile> deleteFilesToAdd;
  private Map<String, String> properties;

  public TableCommitInput() {}

  public TableCommitInput(
      ArcticTable table,
      long targetSnapshotId,
      Set<DataFile> dataFilesToReplace,
      Set<DeleteFile> deleteFilesToReplace,
      Set<DataFile> dataFilesToAdd,
      Set<DeleteFile> deleteFilesToAdd,
      Map<String, String> properties) {
    this.table = table;
    this.targetSnapshotId = targetSnapshotId;
    this.dataFilesToReplace = dataFilesToReplace;
    this.deleteFilesToReplace = deleteFilesToReplace;
    this.dataFilesToAdd = dataFilesToAdd;
    this.deleteFilesToAdd = deleteFilesToAdd;
    this.properties = properties;
  }
}
