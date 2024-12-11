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

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link IgniteCompute} that adds protection against thread hijacking by users.
 */
public class AntiHijackIgniteCompute implements IgniteCompute, Wrapper {
    private final IgniteComputeImpl compute;
    private final Executor asyncContinuationExecutor;

    /**
     * Constructor.
     */
    public AntiHijackIgniteCompute(IgniteCompute compute, Executor asyncContinuationExecutor) {
        this.compute = (IgniteComputeImpl) compute;
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    @Override
    public <T, R> JobExecution<R> submit(JobTarget target, JobDescriptor<T, R> descriptor,
            @Nullable T arg) {
        return preventThreadHijack(compute.submit(target, descriptor, arg));
    }

    @Override
    public <T, R> CompletableFuture<R> executeAsync(JobTarget target, JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return preventThreadHijack(compute.executeAsync(target, descriptor, cancellationToken, arg));
    }

    @Override
    public <T, R> R execute(JobTarget target, JobDescriptor<T, R> descriptor, @Nullable CancellationToken cancellationToken,
            @Nullable T arg) {
        return compute.execute(target, descriptor, cancellationToken, arg);
    }

    @Override
    public <T, R> Map<ClusterNode, JobExecution<R>> submitBroadcast(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return preventThreadHijack(compute.submitBroadcast(nodes, descriptor, arg));
    }

    @Override
    public <T, R> CompletableFuture<Map<ClusterNode, JobExecution<R>>> submitBroadcastPartitioned(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return preventThreadHijack(compute.submitBroadcastPartitioned(tableName, descriptor, arg)
                .thenApply(this::preventThreadHijack)
        );
    }

    @Override
    public <T, R> CompletableFuture<Map<ClusterNode, R>> executeBroadcastPartitionedAsync(String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        return preventThreadHijack(compute.executeBroadcastPartitionedAsync(tableName, descriptor, cancellationToken, arg));
    }

    @Override
    public <T, R> Map<ClusterNode, R> executeBroadcastPartitioned(
            String tableName,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        return compute.executeBroadcastPartitioned(tableName, descriptor, cancellationToken, arg);
    }

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return new AntiHijackTaskExecution<>(compute.submitMapReduce(taskDescriptor, arg), asyncContinuationExecutor);
    }

    @Override
    public <T, R> CompletableFuture<R> executeMapReduceAsync(TaskDescriptor<T, R> taskDescriptor,
            @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return preventThreadHijack(compute.executeMapReduceAsync(taskDescriptor, cancellationToken, arg));
    }

    @Override
    public <T, R> R executeMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable CancellationToken cancellationToken, @Nullable T arg) {
        return compute.executeMapReduce(taskDescriptor, cancellationToken, arg);
    }

    private <T, R> JobExecution<R> preventThreadHijack(JobExecution<R> execution) {
        return new AntiHijackJobExecution<>(execution, asyncContinuationExecutor);
    }

    private <R> Map<ClusterNode, JobExecution<R>> preventThreadHijack(Map<ClusterNode, JobExecution<R>> executions) {
        return executions.entrySet().stream()
                .collect(toUnmodifiableMap(Entry::getKey, entry -> preventThreadHijack(entry.getValue())));
    }

    private <R> CompletableFuture<R> preventThreadHijack(CompletableFuture<R> future) {
        return PublicApiThreading.preventThreadHijack(future, asyncContinuationExecutor);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(compute);
    }
}
