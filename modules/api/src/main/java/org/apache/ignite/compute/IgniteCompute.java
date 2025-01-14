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

package org.apache.ignite.compute;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * Provides the ability to execute Compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#executeAsync(JobExecutionContext, Object)
 */
public interface IgniteCompute {
    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Future of the job execution object which will be completed when the job is submitted.
     */
    default <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submitAsync(target, descriptor, arg, null);
    }

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Future of the job execution object which will be completed when the job is submitted.
     */
    <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Future of the broadcast job execution object which will be completed when all the jobs are submitted.
     */
    default <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submitAsync(target, descriptor, arg, null);
    }

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Future of the broadcast job execution object which will be completed when all the jobs are submitted.
     */
    <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes. A shortcut for
     * {@code submit(...).resultAsync()}.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Job result future.
     */
    default <T, R> CompletableFuture<R> executeAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return executeAsync(target, descriptor, arg, null);
    }

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes. A shortcut for
     * {@code submit(...).resultAsync()}.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Job result future.
     */
    default <T, R> CompletableFuture<R> executeAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitAsync(target, descriptor, arg, cancellationToken).thenCompose(JobExecution::resultAsync);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Job results future.
     */
    default <T, R> CompletableFuture<Collection<R>> executeAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return executeAsync(target, descriptor, arg, null);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Job results future.
     */
    default <T, R> CompletableFuture<Collection<R>> executeAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitAsync(target, descriptor, arg, cancellationToken).thenCompose(BroadcastExecution::resultsAsync);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return execute(target, descriptor, arg, null);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Collection of results.
     * @throws ComputeException If there is any problem executing the job.
     */
    default <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return execute(target, descriptor, arg, null);
    }

    /**
     * Executes a {@link ComputeJob} of the given class on all nodes in the given node set.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Broadcast execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Collection of results.
     * @throws ComputeException If there is any problem executing the job.
     */
    <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @return Task execution interface.
     */
    default <T, R> TaskExecution<R> submitMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return submitMapReduce(taskDescriptor, arg, null);
    }

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Task execution interface.
     */
    <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    );

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution. A shortcut for {@code submitMapReduce(...).resultAsync()}.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @return Task result future.
     */
    default <T, R> CompletableFuture<R> executeMapReduceAsync(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return executeMapReduceAsync(taskDescriptor, arg, null);
    }

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution. A shortcut for {@code submitMapReduce(...).resultAsync()}.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Task result future.
     */
    default <T, R> CompletableFuture<R> executeMapReduceAsync(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return submitMapReduce(taskDescriptor, arg, cancellationToken).resultAsync();
    }

    /**
     * Executes a {@link MapReduceTask} of the given class.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @return Task result.
     * @throws ComputeException If there is any problem executing the task.
     */
    default <T, R> R executeMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg) {
        return executeMapReduce(taskDescriptor, arg, null);
    }

    /**
     * Executes a {@link MapReduceTask} of the given class.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param taskDescriptor Map reduce task descriptor.
     * @param arg Task argument.
     * @param cancellationToken Cancellation token or {@code null}.
     * @return Task result.
     * @throws ComputeException If there is any problem executing the task.
     */
    <T, R> R executeMapReduce(TaskDescriptor<T, R> taskDescriptor, @Nullable T arg, @Nullable CancellationToken cancellationToken);
}
