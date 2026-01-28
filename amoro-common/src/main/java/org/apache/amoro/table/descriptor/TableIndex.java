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

package org.apache.amoro.table.descriptor;

import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;

import java.util.List;

/** Descriptor for table index information, used by server dashboard and other components. */
public class TableIndex {

  private String name;
  private List<String> fields;
  private String type;
  private String snapshot;
  private String coverage;

  public TableIndex(
      String name, List<String> fields, String type, String snapshot, String coverage) {
    this.name = name;
    this.fields = fields;
    this.type = type;
    this.snapshot = snapshot;
    this.coverage = coverage;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields(List<String> fields) {
    this.fields = fields;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(String snapshot) {
    this.snapshot = snapshot;
  }

  public String getCoverage() {
    return coverage;
  }

  public void setCoverage(String coverage) {
    this.coverage = coverage;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("fields", fields)
        .add("type", type)
        .add("snapshot", snapshot)
        .add("coverage", coverage)
        .toString();
  }
}
