package org.apache.ignite.internal.placementdriver;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.TestOnly;

@TestOnly
public class TestPlacementDriver implements PlacementDriver {

    private final String leaseholder;

    // TODO: sanpwc remove.
    public TestPlacementDriver() {
        this.leaseholder = null;
    }

    public TestPlacementDriver(String leaseholder) {
        this.leaseholder = leaseholder;
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        return null;
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return null;
    }
}
