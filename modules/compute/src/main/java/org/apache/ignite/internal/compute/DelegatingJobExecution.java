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
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.executor.JobExecutionInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Delegates {@link JobExecution} to the future of {@link JobExecutionInternal}.
 *
 * @param <R> Result type.
 */
class DelegatingJobExecution<R> implements JobExecution<R> {
    private final CompletableFuture<JobExecutionInternal<R>> delegate;

    DelegatingJobExecution(CompletableFuture<JobExecutionInternal<R>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return delegate.thenCompose(JobExecutionInternal::resultAsync);
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync() {
        return delegate.thenApply(JobExecutionInternal::status);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return delegate.thenApply(JobExecutionInternal::cancel);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return delegate.thenApply(jobExecutionInternal -> jobExecutionInternal.changePriority(newPriority));
    }
}
