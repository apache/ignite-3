package org.apache.ignite.internal.placementdriver;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

public class ZoneBasedPlacementDriver  extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>  implements PlacementDriver {

    private final ClusterService topologyService;

    public ZoneBasedPlacementDriver(ClusterService topologyService) {
        this.topologyService = topologyService;
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        return getFirst();
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return getFirst();
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return nullCompletedFuture();
    }

    private CompletableFuture<ReplicaMeta> getFirst() {
        ClusterNode node = topologyService.topologyService().allMembers().stream().sorted().findFirst().get();

        if (node == null) {
            throw new IllegalStateException("No members in topology");
        }

        return CompletableFuture.completedFuture(new ReplicaMeta() {
            @Override
            public @Nullable String getLeaseholder() {
                return node.name();
            }

            @Override
            public @Nullable String getLeaseholderId() {
                return node.id();
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
}
