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
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.jetbrains.annotations.Nullable;

/**
 * Delegates {@link TaskExecution} to the future of {@link TaskExecutionInternal}.
 *
 * @param <R> Result type.
 */
public class DelegatingTaskExecution<R> implements TaskExecution<R> {
    private final CompletableFuture<TaskExecutionInternal<R>> delegate;

    public DelegatingTaskExecution(CompletableFuture<TaskExecutionInternal<R>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return delegate.thenCompose(TaskExecutionInternal::resultAsync);
    }

    @Override
    public CompletableFuture<List<@Nullable JobStatus>> statusesAsync() {
        return delegate.thenCompose(TaskExecutionInternal::statusesAsync);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return delegate.thenCompose(TaskExecutionInternal::cancelAsync);
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        return delegate.thenCompose(TaskExecutionInternal::statusAsync);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return delegate.thenCompose(execution -> execution.changePriorityAsync(newPriority));
    }
}
