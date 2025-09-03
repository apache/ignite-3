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
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of {@link TaskExecution} as {@link JobExecution}.
 *
 * @param <R> Job result type.
 */
class TaskToJobExecutionWrapper<R> implements CancellableJobExecution<R> {
    private final CancellableTaskExecution<R> taskExecution;
    private final ClusterNode publicLocalNode;

    TaskToJobExecutionWrapper(CancellableTaskExecution<R> taskExecution, InternalClusterNode localNode) {
        this.taskExecution = taskExecution;

        publicLocalNode = localNode.toPublicNode();
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return taskExecution.resultAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        return taskExecution.stateAsync().thenApply(JobTaskStatusMapper::toJobState);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return taskExecution.cancelAsync();
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return taskExecution.changePriorityAsync(newPriority);
    }

    @Override
    public ClusterNode node() {
        return publicLocalNode;
    }
}
