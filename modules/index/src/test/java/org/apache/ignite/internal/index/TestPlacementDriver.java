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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;

/** Implementation for tests. */
class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements PlacementDriver {
    private final Map<ReplicationGroupId, CompletableFuture<ReplicaMeta>> primaryReplicaMetaFutureById = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return primaryReplicaMetaFutureById.get(groupId);
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return primaryReplicaMetaFutureById.get(replicationGroupId);
    }

    @Override
    public @Nullable ReplicaMeta getCurrentPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return primaryReplicaMetaFutureById.get(replicationGroupId).join();
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TokenizedAssignments> getAssignments(
            ReplicationGroupId replicationGroupId,
            HybridTimestamp clusterTimeToAwait
    ) {
        return failedFuture(new UnsupportedOperationException("getAssignments() is not supported in FakePlacementDriver yet."));
    }

    CompletableFuture<Void> setPrimaryReplicaMeta(
            long causalityToken,
            TablePartitionId replicaId,
            CompletableFuture<ReplicaMeta> replicaMetaFuture
    ) {
        primaryReplicaMetaFutureById.put(replicaId, replicaMetaFuture);

        return replicaMetaFuture.thenCompose(replicaMeta -> fireEvent(
                PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(
                        causalityToken,
                        replicaId,
                        replicaMeta.getLeaseholderId(),
                        replicaMeta.getLeaseholder(),
                        replicaMeta.getStartTime()
                )
        ));
    }
}
