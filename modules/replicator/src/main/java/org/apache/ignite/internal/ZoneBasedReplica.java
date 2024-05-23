package org.apache.ignite.internal;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.jetbrains.annotations.TestOnly;

public class ZoneBasedReplica extends Replica {

    public ZoneBasedReplica(
            ReplicationGroupId replicaGrpId, ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient)  {
        super(replicaGrpId, listener, null, raftClient, null, null, null, null);
    }

    @TestOnly
    public ReplicaListener listener() {
        return listener;
    }

    @Override
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, String senderId) {
        return listener.invoke(request, senderId);
    }

    @Override
    public ReplicationGroupId groupId() {
        return replicaGrpId;
    }

    @Override
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        throw new UnsupportedOperationException("provessPlacementDriverMessage");
    }

    @Override
    public String proposedPrimary() {
        throw new UnsupportedOperationException("proposedPrimary");
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return super.shutdown();
    }
}
