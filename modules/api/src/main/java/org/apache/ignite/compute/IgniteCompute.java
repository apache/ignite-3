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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.task.MapReduceTask;

/**
 * Provides the ability to execute Compute jobs.
 *
 * @see ComputeJob
 * @see ComputeJob#execute(JobExecutionContext, Object...)
 */
public interface IgniteCompute {
    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type.
     * @param target Execution target.
     * @param job Job info.
     * @param args Arguments of the job.
     * @return Job execution object.
     */
    <R> JobExecution<R> submit(
            ExecutionTarget target,
            JobDescriptor job,
            Object... args
    );

    /**
     * Submits a {@link ComputeJob} of the given class for an execution on a single node from a set of candidate nodes. A shortcut for
     * {@code submit(...).resultAsync()}.
     *
     * @param <R> Job result type.
     * @param target Execution target.
     * @param job Job info.
     * @param args Arguments of the job.
     * @return Job result future.
     */
    default <R> CompletableFuture<R> executeAsync(
            ExecutionTarget target,
            JobDescriptor job,
            Object... args
    ) {
        return this.<R>submit(target, job, args).resultAsync();
    }

    /**
     * Executes a {@link ComputeJob} of the given class on a single node from a set of candidate nodes.
     *
     * @param <R> Job result type
     * @param target Execution target.
     * @param job Job info.
     * @param args Arguments of the job.
     * @return Job result.
     * @throws ComputeException If there is any problem executing the job.
     */
    <R> R execute(
            ExecutionTarget target,
            JobDescriptor job,
            Object... args
    );

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution.
     *
     * @param task Task info.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task execution interface.
     */
    <R> TaskExecution<R> submitMapReduce(TaskDescriptor task, Object... args);

    /**
     * Submits a {@link MapReduceTask} of the given class for an execution. A shortcut for {@code submitMapReduce(...).resultAsync()}.
     *
     * @param task Task info.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task result future.
     */
    default <R> CompletableFuture<R> executeMapReduceAsync(TaskDescriptor task, Object... args) {
        return this.<R>submitMapReduce(task, args).resultAsync();
    }

    /**
     * Executes a {@link MapReduceTask} of the given class.
     *
     * @param task Task info.
     * @param args Task arguments.
     * @param <R> Task result type.
     * @return Task result.
     * @throws ComputeException If there is any problem executing the task.
     */
    <R> R executeMapReduce(TaskDescriptor task, Object... args);
}
