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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;

/**
 * Replica for the zone based partitions.
 */
public class ZonePartitionReplicaImpl implements Replica {
    private final ReplicationGroupId replicaGrpId;

    private final ReplicaListener listener;

    private final TopologyAwareRaftGroupService raftClient;

    private final PlacementDriverMessageProcessor placementDriverMessageProcessor;

    /**
     * Constructor.
     *
     * @param replicaGrpId  Replication group id.
     * @param listener Listener for the replica.
     * @param raftClient Raft client.
     */
    public ZonePartitionReplicaImpl(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            TopologyAwareRaftGroupService raftClient,
            PlacementDriverMessageProcessor placementDriverMessageProcessor
    )  {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.raftClient = raftClient;
        this.placementDriverMessageProcessor = placementDriverMessageProcessor;
    }

    @Override
    public ReplicaListener listener() {
        return listener;
    }

    @Override
    public TopologyAwareRaftGroupService raftClient() {
        return raftClient;
    }

    @Override
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, UUID senderId) {
        return listener.invoke(request, senderId);
    }

    @Override
    public ReplicationGroupId groupId() {
        return replicaGrpId;
    }

    @Override
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        return placementDriverMessageProcessor.processPlacementDriverMessage(msg);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        listener.onShutdown();

        raftClient.unsubscribeLeader();
        raftClient.shutdown();

        return nullCompletedFuture();
    }

    @Override
    public void updatePeersAndLearners(PeersAndLearners peersAndLearners) {
        raftClient.updateConfiguration(peersAndLearners);
    }

    @Override
    public CompletableFuture<Void> createSnapshotOn(Member targetMember, boolean forced) {
        Peer peer = targetMember.isVotingMember()
                ? new Peer(targetMember.consistentId(), 0)
                : new Peer(targetMember.consistentId(), 1);

        return raftClient.snapshot(peer, forced);
    }

    @Override
    public CompletableFuture<Void> transferLeadershipTo(String targetConsistentId) {
        return raftClient.transferLeadership(new Peer(targetConsistentId));
    }
}
