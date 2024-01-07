/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.compute;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class RemoteCompletableFutureWrapper<T> extends CompletableFuture<T> {
    private CompletableFuture<T> remoteCompletableFuture;

    RemoteCompletableFutureWrapper(CompletableFuture<T> remoteCompletableFuture) {
        this.remoteCompletableFuture = remoteCompletableFuture;
    }

    public void setFuture(CompletableFuture<T> fut) {
        remoteCompletableFuture = fut;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return remoteCompletableFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return remoteCompletableFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return remoteCompletableFuture.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return remoteCompletableFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return remoteCompletableFuture.get(timeout, unit);
    }

    public void onDispose(Runnable runnable) {
        remoteCompletableFuture.whenComplete((r, e) -> runnable.run());
        // todo: dispose action
    }
}
