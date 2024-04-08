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

package org.apache.ignite.internal.table.distributed.wrappers;

import static java.util.function.Function.identity;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * Decorates a {@link PlacementDriver} to make sure that completion stages depending on the returned futures are always completed
 * either using the provided {@link Executor} or in the thread that executed the addition of the corresponding stage to the returned future.
 */
public class ExecutorInclinedPlacementDriver extends DelegatingPlacementDriver {
    private final Executor completionExecutor;

    /**
     * Constructor.
     */
    public ExecutorInclinedPlacementDriver(PlacementDriver placementDriver, Executor completionExecutor) {
        super(placementDriver);

        this.completionExecutor = completionExecutor;
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        return decorateFuture(super.awaitPrimaryReplica(groupId, timestamp, timeout, unit));
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForTable(
            ZonePartitionId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return decorateFuture(super.awaitPrimaryReplicaForTable(groupId, timestamp, timeout, unit));
    }

    private <T> CompletableFuture<T> decorateFuture(CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }

        return future.thenApplyAsync(identity(), completionExecutor);
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return decorateFuture(super.getPrimaryReplica(replicationGroupId, timestamp));
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return decorateFuture(super.previousPrimaryExpired(grpId));
    }

    @Override
    public CompletableFuture<Void> addSubgroups(
            ZonePartitionId zoneId,
            Long enlistmentConsistencyToken,
            Set<ReplicationGroupId> subGrps
    ) {
        return decorateFuture(super.addSubgroups(zoneId, enlistmentConsistencyToken, subGrps));
    }
}
