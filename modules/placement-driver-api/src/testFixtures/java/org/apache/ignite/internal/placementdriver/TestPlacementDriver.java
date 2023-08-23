package org.apache.ignite.internal.placementdriver;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.TestOnly;

@TestOnly
public class TestPlacementDriver implements PlacementDriver {

    @Override
    public CompletableFuture<LeaseMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        return null;
    }

    @Override
    public CompletableFuture<LeaseMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return null;
    }
}
