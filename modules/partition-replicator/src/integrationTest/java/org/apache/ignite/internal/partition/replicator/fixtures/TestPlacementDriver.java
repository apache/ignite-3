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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.jetbrains.annotations.Nullable;

/**
 * Trivial placement driver for tests.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this code
public class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>
        implements PlacementDriver {
    private static final int DEFAULT_ZONE_ID = 0;

    private volatile ReplicaMeta primary;

    // Pre-calculated assignments for each partition.
    private volatile List<TokenizedAssignments> tokenizedAssignments;

    /**
     * Set the primary replica.
     *
     * @param node Primary replica node.
     */
    public void setPrimary(InternalClusterNode node, HybridTimestamp leaseStartTime) {
        primary = new ReplicaMeta() {
            @Override
            public @Nullable String getLeaseholder() {
                return node.name();
            }

            @Override
            public @Nullable UUID getLeaseholderId() {
                return node.id();
            }

            @Override
            public HybridTimestamp getStartTime() {
                return leaseStartTime;
            }

            @Override
            public HybridTimestamp getExpirationTime() {
                return HybridTimestamp.MAX_VALUE;
            }
        };
    }

    public void setPrimary(InternalClusterNode node) {
        setPrimary(node, HybridTimestamp.MIN_VALUE);
    }

    /**
     * Set pre-calculated assignments for each partition.
     *
     * @param tokenizedAssignments Pre-calculated assignments.
     */
    public void setAssignments(List<TokenizedAssignments> tokenizedAssignments) {
        this.tokenizedAssignments = tokenizedAssignments;
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
        List<TokenizedAssignments> assignments = tokenizedAssignments;

        if (assignments == null) {
            return failedFuture(new AssertionError("Pre-calculated assignments are not defined in test PlacementDriver yet."));
        } else {
            return completedFuture(assignments);
        }
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> awaitNonEmptyAssignments(
            List<? extends ReplicationGroupId> replicationGroupIds,
            long timeoutMillis
    ) {
        List<TokenizedAssignments> assignments = tokenizedAssignments;

        if (assignments == null) {
            return failedFuture(new AssertionError("Pre-calculated assignments are not defined in test PlacementDriver yet."));
        } else {
            return completedFuture(assignments);
        }
    }

    private CompletableFuture<ReplicaMeta> getPrimaryReplicaMeta(ReplicationGroupId replicationGroupId) {
        if (replicationGroupId instanceof ZonePartitionId && ((ZonePartitionId) replicationGroupId).zoneId() == DEFAULT_ZONE_ID) {
            return nullCompletedFuture();
        }

        if (primary == null) {
            throw new IllegalStateException("Primary replica is not defined in test PlacementDriver");
        }

        return completedFuture(primary);
    }

    @Override
    public boolean isActualAt(HybridTimestamp timestamp) {
        return true;
    }
}
