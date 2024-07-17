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

package org.apache.ignite.internal.compute.task;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link TaskExecution} that adds protection against thread hijacking by users.
 */
public class AntiHijackTaskExecution<R> implements TaskExecution<R> {
    private final TaskExecution<R> execution;

    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     *
     * @param execution Original execution.
     * @param asyncContinuationExecutor Executor to which the execution will be resubmitted.
     */
    public AntiHijackTaskExecution(TaskExecution<R> execution, Executor asyncContinuationExecutor) {
        this.execution = execution;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public CompletableFuture<List<@Nullable JobState>> statesAsync() {
        return preventThreadHijack(execution.statesAsync());
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return preventThreadHijack(execution.resultAsync());
    }

    @Override
    public CompletableFuture<@Nullable TaskState> stateAsync() {
        return preventThreadHijack(execution.stateAsync());
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return preventThreadHijack(execution.cancelAsync());
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return preventThreadHijack(execution.changePriorityAsync(newPriority));
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }
}
