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

package org.apache.amoro.admin;

import org.apache.amoro.TableFormat;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.Map;
import java.util.ServiceLoader;

public class AdminUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AdminUtil.class);

  /**
   * @param addresses support type 127.0.0.1:2181/ddd,host2:2181,host3:2181/service or
   *     music-hbase64.jd.163.org,music-hbase65.jd.163.org,
   *     music-hbase66.jd.163.org/hbase-music-feature-jd
   * @return true if success
   */
  public static boolean telnetOrPing(String addresses) {
    String[] split = addresses.split(",");
    for (String address : split) {
      String[] info = address.split(":");
      if (info.length < 2) {
        String[] ip = info[0].split("/");
        if (ping(ip[0])) {
          return true;
        } else {
          continue;
        }
      }
      String[] portSplit = info[1].split("/");
      int port;
      try {
        port = Integer.parseInt(portSplit[0]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(portSplit[0] + " is not port");
      }
      if (telnet(info[0], port)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param host host
   * @param port port
   * @return true if success
   */
  public static boolean telnet(String host, int port) {
    try {
      TelnetClient telnetClient = new TelnetClient("vt200");
      telnetClient.setConnectTimeout(500);
      telnetClient.connect(host, port);
      telnetClient.disconnect();
      return true;
    } catch (Exception e) {
      LOG.warn("telnet {} {} timeout! ", host, port);
      return false;
    }
  }

  public static boolean ping(String ip) {
    try {
      return InetAddress.getByName(ip).isReachable(500);
    } catch (Exception e) {
      LOG.warn("ping {} timeout! ", ip);
      return false;
    }
  }

  /** Convert size to a different unit, ensuring that the converted value is > 1 */
  public static String byteToXB(long size) {
    String[] units = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    float result = size, tmpResult = size;
    int unitIdx = 0;
    int unitCnt = units.length;
    while (true) {
      result = result / 1024;
      if (result < 1 || unitIdx >= unitCnt - 1) {
        return String.format("%2.2f%s", tmpResult, units[unitIdx]);
      }
      tmpResult = result;
      unitIdx += 1;
    }
  }

  public static String getFileName(String path) {
    return path == null ? null : new File(path).getName();
  }

  public static OptimizingAdmin loadOptimizingAdmin() {
    ServiceLoader<OptimizingAdmin> loader = ServiceLoader.load(OptimizingAdmin.class);
    OptimizingAdmin admin = null;
    for (OptimizingAdmin optimizingAdmin : loader) {
      if (admin != null) {
        throw new IllegalStateException(
            String.format(
                "Multiple OptimizingAdmin detected: %s and %s, please check manifest file",
                admin.getClass().getName(), optimizingAdmin.getClass().getName()));
      }
      admin = optimizingAdmin;
    }
    return admin;
  }

  public static void loadTableFormatAdminMap(Map<TableFormat, TableFormatAdmin> formatAdminMap) {
    ServiceLoader<TableFormatAdmin> loader = ServiceLoader.load(TableFormatAdmin.class);
    for (TableFormatAdmin tableFormatAdmin : loader) {
      tableFormatAdmin
          .getSupportedFormats()
          .forEach(
              format -> {
                if (formatAdminMap.containsKey(format)) {
                  throw new IllegalStateException(
                      String.format(
                          "Multiple format admin class found between %s and %s",
                          tableFormatAdmin.getClass().getName(),
                          formatAdminMap.get(format).getClass().getName()));
                }
                formatAdminMap.put(format, tableFormatAdmin);
              });
    }
  }
}
