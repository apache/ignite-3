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

package org.apache.ignite.internal.compute.executor;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Internal job execution object.
 *
 * @param <R> Job result type.
 */
public class JobExecutionInternal<R> implements MarshallerProvider<R> {
    private final QueueExecution<ComputeJobDataHolder> execution;

    private final CancelHandle cancelHandle;

    private final Marshaller<R, byte[]> marshaller;

    private final boolean marshalResult;

    private final ClusterNode publicLocalNode;

    /**
     * Constructor.
     *
     * @param execution Internal execution state.
     * @param cancelHandle Cancel handle which is passed to the execution context so that the job can observe cancellation.
     * @param marshaller Result marshaller.
     * @param marshalResult Flag indicating whether the marshalling of the result will be needed.
     * @param localNode Local cluster node.
     */
    JobExecutionInternal(
            QueueExecution<ComputeJobDataHolder> execution,
            CancelHandle cancelHandle,
            @Nullable Marshaller<R, byte[]> marshaller,
            boolean marshalResult,
            InternalClusterNode localNode
    ) {
        this.execution = execution;
        this.cancelHandle = cancelHandle;
        this.marshaller = marshaller;
        this.marshalResult = marshalResult;

        this.publicLocalNode = localNode.toPublicNode();
    }

    public CompletableFuture<ComputeJobDataHolder> resultAsync() {
        return execution.resultAsync();
    }

    @Nullable
    public JobState state() {
        return execution.state();
    }

    /**
     * Cancel job execution.
     *
     * <p>Initiates cancellation of operations registered on the job's cancellation token (e.g., SQL queries)
     * and interrupts the worker thread as a fallback for jobs that don't use the token. Token cancellation
     * is asynchronous and may complete after this method returns.
     *
     * @return {@code true} if job was successfully cancelled.
     */
    public boolean cancel() {
        cancelHandle.cancelAsync();
        return execution.cancel();
    }

    /**
     * Change priority of job execution.
     *
     * @param newPriority new priority.
     * @return {@code true} if priority was successfully changed.
     */
    public boolean changePriority(int newPriority) {
        return execution.changePriority(newPriority);
    }

    @Override
    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        return marshaller;
    }

    @Override
    public boolean marshalResult() {
        return marshalResult;
    }

    public ClusterNode node() {
        return publicLocalNode;
    }
}
