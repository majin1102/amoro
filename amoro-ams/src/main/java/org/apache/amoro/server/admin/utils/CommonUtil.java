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

package org.apache.amoro.server.admin.utils;

import io.javalin.http.Context;
import org.apache.amoro.exception.SignatureCheckException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CommonUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);

  private static final String[] TOKEN_WHITE_LIST = {"/login/current", "/versionInfo"};

  /**
   * Check single page access token.
   *
   * @param ctx The context object containing the request information.
   * @throws SignatureCheckException If the token is invalid or missing.
   */
  public static void checkSinglePageToken(Context ctx) {
    // check if query parameters contain  token key
    String token = ctx.queryParam("token");

    if (StringUtils.isNotEmpty(token)) {
      // regex extract  catalog, db, table
      String url = ctx.req.getRequestURI();
      for (String whiteListUrl : TOKEN_WHITE_LIST) {
        if (url.contains(whiteListUrl)) {
          return;
        }
      }
      String catalog = ctx.queryParam("catalog");
      String db = ctx.queryParam("db");
      String table = ctx.queryParam("table");
      if (StringUtils.isEmpty(catalog) && StringUtils.isEmpty(db) && StringUtils.isEmpty(table)) {
        String[] splitResult = url.split("/");
        for (int i = 0; i < splitResult.length; i++) {
          switch (splitResult[i]) {
            case "catalogs":
              catalog = splitResult[i + 1];
              break;
            case "dbs":
              db = splitResult[i + 1];
              break;
            case "tables":
              table = splitResult[i + 1];
              break;
          }
        }
      }
      if (StringUtils.isEmpty(catalog)
          || StringUtils.isEmpty(db)
          || StringUtils.isEmpty(table)
          || !token.equals(generateTablePageToken(catalog, db, table))) {
        throw new SignatureCheckException();
      }
    }
  }

  /**
   * Generate the token for single table page access.
   *
   * @param catalog The catalog name.
   * @param db The database name.
   * @param table The table name.
   * @return The generated token.
   */
  public static String generateTablePageToken(String catalog, String db, String table) {
    Map<String, String> params = new HashMap<>();
    params.put("catalog", catalog);
    params.put("db", db);
    params.put("table", table);

    String paramString = ParamSignatureCalculator.generateParamStringWithValue(params);
    String plainText = String.format("%s%s%s", paramString, paramString, paramString);
    return ParamSignatureCalculator.getMD5(plainText);
  }
}
