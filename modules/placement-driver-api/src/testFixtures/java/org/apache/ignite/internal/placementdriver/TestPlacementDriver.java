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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Test placement driver service that immediately returns unbounded primary replica from both await and get methods for the specified
 * leaseholder.
 */
@TestOnly
public class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>
        implements PlacementDriver {
    private volatile Supplier<? extends ReplicaMeta> primaryReplicaSupplier;

    /**
     * Auxiliary constructor that will create replica meta by {@link TestReplicaMetaImpl#TestReplicaMetaImpl(InternalClusterNode)}
     * internally.
     */
    public TestPlacementDriver(InternalClusterNode leaseholder) {
        primaryReplicaSupplier = () -> new TestReplicaMetaImpl(leaseholder);
    }

    /**
     * Auxiliary constructor that allows you to create {@link TestPlacementDriver} in cases where the node ID will be known only after the
     * start of the components/node. Will use {@link TestReplicaMetaImpl#TestReplicaMetaImpl(InternalClusterNode)}.
     */
    public TestPlacementDriver(Supplier<InternalClusterNode> leaseholderSupplier) {
        primaryReplicaSupplier = () -> {
            InternalClusterNode leaseholder = leaseholderSupplier.get();
            return leaseholder == null ? null : new TestReplicaMetaImpl(leaseholder);
        };
    }

    public TestPlacementDriver(String leaseholder, UUID leaseholderId) {
        primaryReplicaSupplier = () -> new TestReplicaMetaImpl(leaseholder, leaseholderId);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        return getReplicaMetaFuture();
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getReplicaMetaFuture();
    }

    @Override
    public @Nullable ReplicaMeta getCurrentPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getReplicaMetaFuture().join();
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> fireEvent(PrimaryReplicaEvent event, PrimaryReplicaEventParameters parameters) {
        return super.fireEvent(event, parameters);
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> getAssignments(
            List<? extends ReplicationGroupId> replicationGroupIds,
            HybridTimestamp clusterTimeToAwait
    ) {
        return failedFuture(new UnsupportedOperationException("getAssignments() is not supported in TestPlacementDriver yet."));
    }

    @Override
    public CompletableFuture<List<TokenizedAssignments>> awaitNonEmptyAssignments(
            List<? extends ReplicationGroupId> replicationGroupIds,
            long timeoutMillis
    ) {
        return failedFuture(new UnsupportedOperationException("awaitNonEmptyAssignments() is not supported in TestPlacementDriver yet."));
    }

    private CompletableFuture<ReplicaMeta> getReplicaMetaFuture() {
        try {
            return completedFuture(primaryReplicaSupplier.get());
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    public Supplier<? extends ReplicaMeta> getPrimaryReplicaSupplier() {
        return this.primaryReplicaSupplier;
    }

    /**
     * Setter for a test primary replica supplier with {@code PRIMARY_REPLICA_ELECTED} event firing that is crucial for some tests internal
     * logic that depends on the event handling.
     *
     * @param primaryReplicaSupplier The supplier that provides {@link TestReplicaMetaImpl} instance with a test primary replica meta
     *      information.
     */
    public void setPrimaryReplicaSupplier(Supplier<? extends TestReplicaMetaImpl> primaryReplicaSupplier) {
        this.primaryReplicaSupplier = primaryReplicaSupplier;

        TestReplicaMetaImpl replicaMeta = primaryReplicaSupplier.get();

        fireEvent(
                PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED,
                new PrimaryReplicaEventParameters(
                        // The only usage of causality token below is IndexBuildController that doesn't use in tests, so the actual
                        // value doesn't matter there yet.
                        0,
                        replicaMeta.getReplicationGroupId(),
                        replicaMeta.getLeaseholderId(),
                        replicaMeta.getLeaseholder(),
                        replicaMeta.getStartTime()
                )
        );
    }

    @Override
    public boolean isActualAt(HybridTimestamp timestamp) {
        return true;
    }
}
