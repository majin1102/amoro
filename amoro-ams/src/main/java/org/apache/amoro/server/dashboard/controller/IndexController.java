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

package org.apache.amoro.server.dashboard.controller;

import io.javalin.http.Context;
import org.apache.amoro.server.dashboard.ServerTableDescriptor;
import org.apache.amoro.server.dashboard.model.IndexInfo;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.TableIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The controller that handles table index requests. */
public class IndexController {

  private final ServerTableDescriptor tableDescriptor;

  public IndexController(ServerTableDescriptor tableDescriptor) {
    this.tableDescriptor = tableDescriptor;
  }

  public void getIndexes(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, database, table);
    List<TableIndex> tableIndexes =
        tableDescriptor.getTableIndexes(tableIdentifier.buildTableIdentifier());

    List<IndexInfo> indexInfos = new ArrayList<>();
    for (TableIndex tableIndex : tableIndexes) {
      IndexInfo info = new IndexInfo();
      info.setName(tableIndex.getName());
      info.setFields(tableIndex.getFields());
      info.setType(tableIndex.getType());
      info.setSnapshot(tableIndex.getSnapshot());
      info.setCoverage(tableIndex.getCoverage());
      indexInfos.add(info);
    }

    ctx.json(OkResponse.of(indexInfos));
  }

  public void getIndexDetail(Context ctx) {
    String catalog = ctx.pathParam("catalog");
    String database = ctx.pathParam("db");
    String table = ctx.pathParam("table");
    String indexName = ctx.pathParam("indexName");

    TableIdentifier tableIdentifier = TableIdentifier.of(catalog, database, table);
    Map<String, Object> detail =
        tableDescriptor.getTableIndexDetail(tableIdentifier.buildTableIdentifier(), indexName);

    ctx.json(OkResponse.of(detail));
  }
}
