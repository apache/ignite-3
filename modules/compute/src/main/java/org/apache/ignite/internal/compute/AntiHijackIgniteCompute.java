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
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.compute.task.AntiHijackTaskExecution;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteCompute} that adds protection against thread hijacking by users.
 */
public class AntiHijackIgniteCompute implements IgniteCompute, Wrapper {
    private final IgniteCompute compute;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    public AntiHijackIgniteCompute(IgniteCompute compute, Executor asyncContinuationExecutor) {
        this.compute = compute;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return preventThreadHijack(compute.submitAsync(target, descriptor, arg, cancellationToken).thenApply(this::preventThreadHijack));
    }

    @Override
    public <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return preventThreadHijack(compute.submitAsync(target, descriptor, arg, cancellationToken).thenApply(this::preventThreadHijack));
    }

    @Override
    public <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return compute.execute(target, descriptor, arg, cancellationToken);
    }

    @Override
    public <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return compute.execute(target, descriptor, arg, cancellationToken);
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return new AntiHijackTaskExecution<>(compute.submitMapReduce(taskDescriptor, arg, cancellationToken), asyncContinuationExecutor);
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return compute.executeMapReduce(taskDescriptor, arg, cancellationToken);
    }

    private <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture) {
        return PublicApiThreading.preventThreadHijack(originalFuture, asyncContinuationExecutor);
    }

    private <T, R> JobExecution<R> preventThreadHijack(JobExecution<R> execution) {
        return new AntiHijackJobExecution<>(execution, asyncContinuationExecutor);
    }

    private <T, R> BroadcastExecution<R> preventThreadHijack(BroadcastExecution<R> execution) {
        return new AntiHijackBroadcastExecution<>(execution, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(compute);
    }
}
