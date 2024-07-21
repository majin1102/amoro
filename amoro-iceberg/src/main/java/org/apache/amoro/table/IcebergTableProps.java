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

package org.apache.amoro.table;

public class IcebergTableProps {

  public static final String WRITE_DISTRIBUTION_MODE =
      org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
  public static final String WRITE_DISTRIBUTION_MODE_NONE =
      org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
  public static final String WRITE_DISTRIBUTION_MODE_HASH =
      org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
  public static final String WRITE_DISTRIBUTION_MODE_RANGE =
      org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
  public static final String WRITE_DISTRIBUTION_MODE_DEFAULT = WRITE_DISTRIBUTION_MODE_HASH;

  public static final String SPLIT_SIZE = org.apache.iceberg.TableProperties.SPLIT_SIZE;
  public static final long SPLIT_SIZE_DEFAULT = 134217728; // 128 MB

  public static final String SPLIT_LOOKBACK = org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
  public static final int SPLIT_LOOKBACK_DEFAULT = 10;

  public static final String WRITE_TARGET_FILE_SIZE_BYTES =
      org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
  public static final long WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 134217728; // 128 MB

  public static final String DEFAULT_FILE_FORMAT =
      org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;

  public static final String DEFAULT_FILE_FORMAT_DEFAULT =
      org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
}
