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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Test placement driver service that immediately returns unbounded primary replica from both await and get methods for the specified
 * leaseholder.
 */
@TestOnly
public class TestPlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>
        implements PlacementDriver {
    private final Supplier<TestReplicaMetaImpl> primaryReplicaSupplier;

    @Nullable
    private BiFunction<ReplicationGroupId, HybridTimestamp, CompletableFuture<ReplicaMeta>> awaitPrimaryReplicaFunction = null;

    /** Auxiliary constructor that will create replica meta by {@link TestReplicaMetaImpl#TestReplicaMetaImpl(ClusterNode)} internally. */
    public TestPlacementDriver(ClusterNode leaseholder) {
        primaryReplicaSupplier = () -> new TestReplicaMetaImpl(leaseholder);
    }

    /**
     * Auxiliary constructor that allows you to create {@link TestPlacementDriver} in cases where the node ID will be known only after the
     * start of the components/node. Will use {@link TestReplicaMetaImpl#TestReplicaMetaImpl(ClusterNode)}.
     */
    public TestPlacementDriver(Supplier<ClusterNode> leaseholderSupplier) {
        primaryReplicaSupplier = () -> new TestReplicaMetaImpl(leaseholderSupplier.get());
    }

    public TestPlacementDriver(String leaseholder, String leaseholderId) {
        primaryReplicaSupplier = () -> new TestReplicaMetaImpl(leaseholder, leaseholderId);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        if (awaitPrimaryReplicaFunction != null) {
            return awaitPrimaryReplicaFunction.apply(groupId, timestamp);
        }

        return getReplicaMetaFuture();
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getReplicaMetaFuture();
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> fireEvent(PrimaryReplicaEvent event, PrimaryReplicaEventParameters parameters) {
        return super.fireEvent(event, parameters);
    }

    public void setAwaitPrimaryReplicaFunction(
            @Nullable BiFunction<ReplicationGroupId, HybridTimestamp, CompletableFuture<ReplicaMeta>> awaitPrimaryReplicaFunction
    ) {
        this.awaitPrimaryReplicaFunction = awaitPrimaryReplicaFunction;
    }

    private CompletableFuture<ReplicaMeta> getReplicaMetaFuture() {
        try {
            return completedFuture(primaryReplicaSupplier.get());
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }
}
