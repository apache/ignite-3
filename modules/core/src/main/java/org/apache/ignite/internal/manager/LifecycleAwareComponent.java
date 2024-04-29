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

package org.apache.ignite.internal.manager;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.util.ExceptionUtils;

public abstract class LifecycleAwareComponent {

    private final ComponentInflights componentLifecycle = new ComponentInflights();

    protected <T> CompletableFuture<T> startFutureAsync(Supplier<CompletableFuture<T>> function) {
        return componentLifecycle.startFutureAsync(function);
    }

    protected CompletableFuture<Void> startAsync(RunnableX function) {
        return startFutureAsync(toFuture(function));
    }

    protected CompletableFuture<Void> stopFutureAsync(Supplier<CompletableFuture<Void>> function) {
        return componentLifecycle.stopFutureAsync(function);
    }

    protected CompletableFuture<Void> stopAsync(RunnableX function) {
        return stopFutureAsync(toFuture(function));
    }

    protected <T> CompletableFuture<T> withLifecycle(Supplier<CompletableFuture<T>> function) {
        return componentLifecycle.register(function);
    }

    protected CompletableFuture<Void> withLifecycle(RunnableX function) {
        return componentLifecycle.register(toFuture(function));
    }

    public CompletableFuture<Void> startAsync() {
        return startAsync(() -> {});
    }

    public CompletableFuture<Void> stopAsync() {
        return stopAsync(() -> {});
    }

    private static Supplier<CompletableFuture<Void>> toFuture(RunnableX function) {
        return () ->
                CompletableFuture.runAsync(() -> {
                    try {
                        function.run();
                    } catch (Throwable e) {
                        ExceptionUtils.sneakyThrow(e);
                    }
                });
    }

}
