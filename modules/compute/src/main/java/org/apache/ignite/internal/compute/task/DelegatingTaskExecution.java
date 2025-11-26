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

import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskState;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.compute.CancellableTaskExecution;
import org.apache.ignite.internal.compute.HybridTimestampProvider;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Delegates {@link TaskExecution} to the future of {@link TaskExecutionInternal}.
 *
 * @param <R> Result type.
 */
public class DelegatingTaskExecution<I, M, T, R> implements CancellableTaskExecution<R>, MarshallerProvider<R>, HybridTimestampProvider {
    private final CompletableFuture<TaskExecutionInternal<I, M, T, R>> delegate;

    public DelegatingTaskExecution(CompletableFuture<TaskExecutionInternal<I, M, T, R>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return delegate.thenCompose(TaskExecutionInternal::resultAsync);
    }

    @Override
    public CompletableFuture<List<@Nullable JobState>> statesAsync() {
        return delegate.thenCompose(TaskExecutionInternal::statesAsync);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return delegate.thenCompose(TaskExecutionInternal::cancelAsync);
    }

    @Override
    public CompletableFuture<@Nullable TaskState> stateAsync() {
        return delegate.thenCompose(TaskExecutionInternal::stateAsync);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return delegate.thenCompose(execution -> execution.changePriorityAsync(newPriority));
    }

    @Override
    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        assert delegate.isDone() : "Task execution is supposed to be done before calling `resultMarshaller()`";

        return delegate.join().resultMarshaller();
    }

    @Override
    public boolean marshalResult() {
        assert delegate.isDone() : "Task execution is supposed to be done before calling `marshalResult()`";

        return delegate.join().marshalResult();
    }

    @Override
    public long hybridTimestamp() {
        if (delegate.isCompletedExceptionally()) {
            return NULL_HYBRID_TIMESTAMP;
        }

        TaskExecutionInternal<I, M, T, R> delegateNow = delegate.getNow(null);

        if (delegateNow == null) {
            throw new IllegalStateException("Task execution is not yet completed, cannot retrieve hybrid timestamp.");
        }

        return delegateNow.hybridTimestamp();
    }
}
