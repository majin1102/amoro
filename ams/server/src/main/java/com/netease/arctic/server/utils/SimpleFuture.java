/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netease.arctic.server.utils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SimpleFuture {

  protected CompletableFuture<Void> future;

  public SimpleFuture() {
    future = new CompletableFuture<>();
  }

  private SimpleFuture(CompletableFuture<Void> future) {
    this.future = future;
  }

  public void cancel() {
    future.cancel(true);
  }

  public boolean isDone() {
    return future.isDone();
  }

  protected CompletableFuture<Void> getFuture() {
    return future;
  }

  public SimpleFuture thenCompose(Supplier<? extends SimpleFuture> action) {
    return new SimpleFuture(future.thenCompose(v -> action.get().getFuture()));
  }

  public SimpleFuture whenComplete(Consumer<? super Throwable> action) {
    return new SimpleFuture(future.whenComplete((v, e) -> action.accept(e)));
  }

  public SimpleFuture exceptionally(Function<Throwable, Void> fn) {
    return new SimpleFuture(future.exceptionally(fn));
  }

  public SavepointFuture savepoint() {
    return new SavepointFuture(this);
  }

  protected void recover() {
    future = new CompletableFuture<>();
  }

  public SimpleFuture complete() {
    future.complete(null);
    return this;
  }

  public void completeExceptionally(Throwable t) {
    future.completeExceptionally(t);
  }

  /*   public SimpleFuture exceptionally(Consumer<? super Throwable> action) {
    return new SimpleFuture(future.exceptionally(throwable -> {
      action.accept(throwable);
      return null;
    }));
  }*/
}
