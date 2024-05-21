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

package org.apache.ignite.internal.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.WaitReplicaStateMessage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;

/**
 * Implementation of {@link PlacementDriver} that is aware if {@link ReplicaService}.
 * It delegates calls to the original {@link PlacementDriver} and after that sends {@link WaitReplicaStateMessage}
 * which calls {@link org.apache.ignite.internal.replicator.Replica#waitForActualState(long)}.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-20362
@Deprecated
public class ReplicaAwareLeaseTracker extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements
        PlacementDriver {
    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final PlacementDriver delegate;
    private final ReplicaService replicaService;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;

    /** Map for saving waiting for replica state futures for each table partition. */
    private final Map<ZonePartitionId, CompletableFuture<Void>> waitPrimaryState = new ConcurrentHashMap<>();


    /**
     * Constructor.
     *
     * @param delegate Delegate Placement Driver.
     * @param replicaService Replica Service.
     * @param clusterNodeResolver Cluster node resolver.
     */
    public ReplicaAwareLeaseTracker(PlacementDriver delegate, ReplicaService replicaService, ClusterNodeResolver clusterNodeResolver) {
        this.delegate = delegate;
        this.replicaService = replicaService;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    @Override
    public void listen(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
        delegate.listen(evt, listener);
    }

    @Override
    public void removeListener(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
        delegate.removeListener(evt, listener);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        return delegate.awaitPrimaryReplica(groupId, timestamp, timeout, unit);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForTable(
            ReplicationGroupId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        ZonePartitionId zonePartitionId = (ZonePartitionId) groupId;

        assert zonePartitionId.tableId() != 0 : "Table id should be defined.";

        ZonePartitionId pureZonePartId = ZonePartitionId.resetTableId(zonePartitionId);

        return delegate.awaitPrimaryReplicaForTable(pureZonePartId, timestamp, timeout, unit)
                .thenComposeAsync(replicaMeta -> {
                    ClusterNode leaseholderNode = clusterNodeResolver.getById(replicaMeta.getLeaseholderId());

                    if (replicaMeta.subgroups().contains(zonePartitionId)) {
                        waitPrimaryState.remove(zonePartitionId);

                        return completedFuture(replicaMeta);
                    }

                    CompletableFuture<Void> waitReplicaStateFut = waitPrimaryState.computeIfAbsent(zonePartitionId, grpId -> {
                        WaitReplicaStateMessage awaitReplicaReq = REPLICA_MESSAGES_FACTORY.waitReplicaStateMessage()
                                .groupId(zonePartitionId)
                                .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                                .timeout(unit.toMillis(timeout))
                                .build();

                        return replicaService.invoke(leaseholderNode, awaitReplicaReq);
                    });

                    return waitReplicaStateFut.thenApply((ignored) -> replicaMeta);
                }, replicaService.getPartitionOperationsExecutor());
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplicaForTable(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        ZonePartitionId zonePartitionId = (ZonePartitionId) replicationGroupId;

        assert zonePartitionId.tableId() != 0 : "Table id should be defined.";

        ZonePartitionId pureZonePartId = ZonePartitionId.resetTableId(zonePartitionId);

        return delegate.getPrimaryReplicaForTable(pureZonePartId, timestamp).thenCompose(replicaMeta -> {
            if (replicaMeta == null) {
                return completedFuture(null);
            }

            ClusterNode leaseholderNode = clusterNodeResolver.getById(replicaMeta.getLeaseholderId());

            if (replicaMeta.subgroups().contains(zonePartitionId)) {
                return completedFuture(replicaMeta);
            }

            WaitReplicaStateMessage awaitReplicaReq = REPLICA_MESSAGES_FACTORY.waitReplicaStateMessage()
                    .groupId(zonePartitionId)
                    .enlistmentConsistencyToken(replicaMeta.getStartTime().longValue())
                    .timeout(10_000)
                    .build();

            return replicaService.invoke(leaseholderNode, awaitReplicaReq).thenApply((ignored) -> replicaMeta);
        });
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return delegate.getPrimaryReplica(replicationGroupId, timestamp);
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return delegate.previousPrimaryExpired(grpId);
    }

    @Override
    public ReplicaMeta getLeaseMeta(ReplicationGroupId grpId) {
        return delegate.getLeaseMeta(grpId);
    }

    @Override
    public CompletableFuture<Void> addSubgroups(ZonePartitionId zoneId, Long enlistmentConsistencyToken, Set<ReplicationGroupId> subGrps) {
        return delegate.addSubgroups(zoneId, enlistmentConsistencyToken, subGrps);
    }
}
