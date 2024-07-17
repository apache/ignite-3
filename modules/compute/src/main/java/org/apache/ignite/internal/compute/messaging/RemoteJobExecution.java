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

package org.apache.ignite.internal.compute.messaging;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.future.InFlightFutures;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Remote job execution implementation.
 *
 * @param <R> Job result type.
 */
public class RemoteJobExecution<R> implements JobExecution<R> {
    private final ClusterNode remoteNode;

    private final CompletableFuture<UUID> jobIdFuture;

    private final CompletableFuture<R> resultFuture;

    private final InFlightFutures inFlightFutures;

    private final ComputeMessaging messaging;

    /**
     * Constructor.
     *
     * @param remoteNode Remote node.
     * @param jobIdFuture Job id future.
     * @param resultFuture Job result future.
     * @param inFlightFutures In-flight futures collection.
     * @param messaging Compute messaging service.
     */
    public RemoteJobExecution(
            ClusterNode remoteNode,
            CompletableFuture<UUID> jobIdFuture,
            CompletableFuture<R> resultFuture,
            InFlightFutures inFlightFutures,
            ComputeMessaging messaging
    ) {
        this.remoteNode = remoteNode;
        this.jobIdFuture = jobIdFuture;
        this.resultFuture = resultFuture;
        this.inFlightFutures = inFlightFutures;
        this.messaging = messaging;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return resultFuture;
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        return inFlightFutures.registerFuture(
                jobIdFuture.thenCompose(jobId -> messaging.remoteStateAsync(remoteNode, jobId))
        );
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return inFlightFutures.registerFuture(
                jobIdFuture.thenCompose(jobId -> messaging.remoteCancelAsync(remoteNode, jobId))
        );
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return inFlightFutures.registerFuture(
                jobIdFuture.thenCompose(jobId -> messaging.remoteChangePriorityAsync(remoteNode, jobId, newPriority))
        );
    }
}
