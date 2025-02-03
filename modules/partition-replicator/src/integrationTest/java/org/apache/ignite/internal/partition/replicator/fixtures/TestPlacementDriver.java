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

package org.apache.ignite.internal.partition.replicator.fixtures;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.network.ClusterNode;

/**
 * Trivial placement driver for tests.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this code
public class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>
        implements PlacementDriver {
    private static final int DEFAULT_ZONE_ID = 0;

    private volatile ClusterNode primary;

    /**
     * Set the primary replica.
     *
     * @param node Primary replica node.
     */
    public void setPrimary(ClusterNode node) {
        primary = node;
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        return getPrimaryReplicaMeta(groupId);
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getPrimaryReplicaMeta(replicationGroupId);
    }

    @Override
    public ReplicaMeta getCurrentPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getPrimaryReplicaMeta(replicationGroupId).join();
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> getAssignments(
            List<? extends ReplicationGroupId> replicationGroupIds,
            HybridTimestamp clusterTimeToAwait
    ) {
        return failedFuture(new UnsupportedOperationException("getAssignments() is not supported in FakePlacementDriver yet."));
    }

    private CompletableFuture<ReplicaMeta> getPrimaryReplicaMeta(ReplicationGroupId replicationGroupId) {
        if (replicationGroupId instanceof ZonePartitionId && ((ZonePartitionId) replicationGroupId).zoneId() == DEFAULT_ZONE_ID) {
            return nullCompletedFuture();
        }

        if (primary == null) {
            throw new IllegalStateException("Primary replica is not defined in test PlacementDriver");
        }

        return CompletableFuture.completedFuture(new ReplicaMeta() {
            @Override
            public String getLeaseholder() {
                return primary.name();
            }

            @Override
            public UUID getLeaseholderId() {
                return primary.id();
            }

            @Override
            public HybridTimestamp getStartTime() {
                return HybridTimestamp.MIN_VALUE;
            }

            @Override
            public HybridTimestamp getExpirationTime() {
                return HybridTimestamp.MAX_VALUE;
            }
        });
    }

    @Override
    public boolean isActualAt(HybridTimestamp timestamp) {
        return true;
    }
}
