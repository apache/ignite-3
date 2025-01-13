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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.thread.PublicApiThreading;

/**
 * Wrapper around {@link JobExecution} that adds protection against thread hijacking by users.
 */
class AntiHijackBroadcastExecution<R> implements BroadcastExecution<R> {
    private final BroadcastExecution<R> execution;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    AntiHijackBroadcastExecution(BroadcastExecution<R> execution, Executor asyncContinuationExecutor) {
        this.execution = execution;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public Collection<JobExecution<R>> executions() {
        return execution.executions().stream()
                .map(execution -> new AntiHijackJobExecution<>(execution, asyncContinuationExecutor))
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<Collection<R>> resultsAsync() {
        return preventThreadHijack(execution.resultsAsync());
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }
}
