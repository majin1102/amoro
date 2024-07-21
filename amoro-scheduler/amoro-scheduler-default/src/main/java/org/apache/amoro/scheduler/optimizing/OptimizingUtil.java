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

package org.apache.amoro.scheduler.optimizing;

import org.apache.iceberg.ContentFile;

public class OptimizingUtil {

  public static long getFileSize(ContentFile<?>[] contentFiles) {
    long size = 0;
    if (contentFiles != null) {
      for (ContentFile<?> contentFile : contentFiles) {
        size += contentFile.fileSizeInBytes();
      }
    }
    return size;
  }

  public static int getFileCount(ContentFile<?>[] contentFiles) {
    return contentFiles == null ? 0 : contentFiles.length;
  }

  public static long getRecordCnt(ContentFile<?>[] contentFiles) {
    int recordCnt = 0;
    if (contentFiles != null) {
      for (ContentFile<?> contentFile : contentFiles) {
        recordCnt += contentFile.recordCount();
      }
    }
    return recordCnt;
  }
}
