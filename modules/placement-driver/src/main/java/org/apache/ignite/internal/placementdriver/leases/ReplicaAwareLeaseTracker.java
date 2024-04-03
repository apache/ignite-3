package org.apache.ignite.internal.placementdriver.leases;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.AwaitReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.WaitReplicaStateMessage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeResolver;
import org.jetbrains.annotations.Nullable;

/**
 * Tbd.
 */
public class ReplicaAwareLeaseTracker extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters> implements
        PlacementDriver {
    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final PlacementDriver delegate;
    private final ReplicaService replicaService;

    /** Resolver that resolves a node consistent ID to cluster node. */
    private final ClusterNodeResolver clusterNodeResolver;


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

    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
            ZonePartitionId groupId,
            HybridTimestamp timestamp,
            long timeout,
            TimeUnit unit
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(groupId.tableId(), groupId.partitionId());

        return delegate.awaitPrimaryReplica(tablePartitionId, timestamp, timeout, unit)
                .thenCompose(replicaMeta -> {
                    ClusterNode leaseholderNode = clusterNodeResolver.getById(replicaMeta.getLeaseholderId());

                    WaitReplicaStateMessage awaitReplicaReq = REPLICA_MESSAGES_FACTORY.waitReplicaStateMessage()
                            .groupId(tablePartitionId)
                            .syncTimeLong(replicaMeta.getExpirationTime().longValue())
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
    public @Nullable ReplicaMeta currentLease(ReplicationGroupId groupId) {
        return delegate.currentLease(groupId);
    }
}
