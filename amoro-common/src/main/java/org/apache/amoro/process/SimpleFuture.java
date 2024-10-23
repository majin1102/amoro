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

package org.apache.amoro.process;

import java.util.ArrayList;
import org.apache.amoro.exception.AmoroRuntimeException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kerby.kerberos.kerb.type.ad.AndOr;

/** A simple wrapper of CompletableFuture for better code readability. */
public class SimpleFuture {

  private final CompletableFuture<?> completedFuture;
  private final List<Runnable> callbacks = new ArrayList<>();

  public SimpleFuture() {
    this(new CompletableFuture<>());
  }

  protected SimpleFuture(CompletableFuture<?> completedFuture) {
    this.completedFuture = completedFuture;
  }

  /**
   * Only proceed the callback if there is no exception. Exceptions would be thrown in the
   * complete() method and will not trigger join return.
   */
  public void whenCompleted(Runnable runnable) {
    callbacks.add(runnable);
  }

  public boolean isDone() {
    return completedFuture.isDone();
  }

  /**
   * This method will trigger all callback functions in the same thread and wait until all callback
   * functions are completed. If throws exception, completedFuture is not done.
   */
  public void complete() {
    if (completedFuture.isDone()) {
      return;
    }
    callbacks.forEach(runnable -> {
      try {
        runnable.run();
      } catch (Throwable throwable) {
        throw normalize(throwable);
      }
    });
    completedFuture.complete(null);
    try {
      completedFuture.get();
    } catch (Throwable throwable) {
      throw normalize(throwable);
    }
  }

  /** Return until completedFuture is done, which means task or process is truly finished */
  public void join() {
    try {
      completedFuture.join();
    } catch (Throwable throwable) {
      throw normalize(throwable);
    }
  }

  private static AmoroRuntimeException normalize(Throwable throwable) {
    if (throwable instanceof ExecutionException && throwable.getCause() != null) {
      return AmoroRuntimeException.wrap(throwable.getCause());
    }
    return AmoroRuntimeException.wrap(throwable);
  }

  public SimpleFuture or(SimpleFuture anotherFuture) {
    return new SimpleFuture(
        CompletableFuture.anyOf(completedFuture, anotherFuture.completedFuture));
  }

  public SimpleFuture and(SimpleFuture anotherFuture) {
    return new SimpleFuture(
        CompletableFuture.allOf(completedFuture, anotherFuture.completedFuture));
  }

  public static SimpleFuture allOf(List<SimpleFuture> futures) {
    SimpleFuture allFuture = new SimpleFuture();
    CompletableFuture<?> chainedFuture =
        CompletableFuture.allOf(
            futures.stream().map(f -> f.completedFuture).toArray(CompletableFuture[]::new));
    chainedFuture.whenComplete(
        (result, throwable) -> {
          if (throwable == null) {
            allFuture.complete();
          } else {
            throw normalize(throwable);
          }
        });
    return allFuture;
  }

  public static SimpleFuture anyOf(List<SimpleFuture> futures) {
    SimpleFuture anyFuture = new SimpleFuture();
    CompletableFuture<?> chainedFuture =
        CompletableFuture.anyOf(
            futures.stream().map(f -> f.completedFuture).toArray(CompletableFuture[]::new));
    chainedFuture.whenComplete(
        (result, throwable) -> {
          if (throwable == null) {
            anyFuture.complete();
          } else {
            throw normalize(throwable);
          }
        });
    return anyFuture;
  }
}
