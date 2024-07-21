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

package org.apache.amoro.server.utils;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.exception.AlreadyExistsException;
import org.apache.amoro.exception.ObjectNotExistsException;

public class Conditions {

  public static void checkObjectNotExists(boolean condition, String object) {
    if (condition) {
      throw new ObjectNotExistsException(object);
    }
  }

  public static void checkObjectNotExists(boolean condition, TableIdentifier tableIdentifier) {
    if (condition) {
      throw new ObjectNotExistsException(tableIdentifier);
    }
  }

  public static void checkObjectNotExists(
      boolean condition, ServerTableIdentifier tableIdentifier) {
    if (condition) {
      throw new ObjectNotExistsException(tableIdentifier);
    }
  }

  public static void checkObjectAlreadyExists(boolean condition, String object) {
    if (condition) {
      throw new AlreadyExistsException(object);
    }
  }

  public static void checkObjectAlreadyExists(boolean condition, TableIdentifier tableIdentifier) {
    if (condition) {
      throw new AlreadyExistsException(tableIdentifier);
    }
  }
}
