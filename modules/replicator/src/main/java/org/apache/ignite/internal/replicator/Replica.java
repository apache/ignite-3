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
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkMessage;

/**
 * Replica server.
 */
public class Replica {
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Replica group identity, this id is the same as the considered partition's id. */
    private final ReplicationGroupId replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    /** Safe time tracker. */
    private final PendingComparableValuesTracker<HybridTimestamp> safeTime;

    /** Topology aware Raft client. */
    private final TopologyAwareRaftGroupService raftClient;

    /** Supplier that returns a {@link ClusterNode} instance of the local node. */
    private final Supplier<ClusterNode> localNodeSupplier;

    // TODO IGNITE-18960 after replica inoperability logic is introduced, this future should be replaced with something like
    //     VersionedValue (so that PlacementDriverMessages would wait for new leader election)
    private CompletableFuture<AtomicReference<ClusterNode>> leaderFuture = new CompletableFuture<>();

    private AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();

    /** Mutex to change {@link #leaseStartTime} and {@link #leaseExpirationTime} consistently. */
    private final Object leaseAcceptanceMutex = new Object();

    /** Latest lease start time. */
    private volatile HybridTimestamp leaseStartTime = null;

    /** Latest lease expiration time. */
    private volatile HybridTimestamp leaseExpirationTime = null;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param safeTime Safe time tracker.
     * @param raftClient Topology aware Raft client.
     * @param localNodeSupplier Supplier that returns a {@link ClusterNode} instance of the local node.
     */
    public Replica(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            PendingComparableValuesTracker<HybridTimestamp> safeTime,
            TopologyAwareRaftGroupService raftClient,
            Supplier<ClusterNode> localNodeSupplier
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.safeTime = safeTime;
        this.raftClient = raftClient;
        this.localNodeSupplier = localNodeSupplier;

        raftClient.subscribeLeader(this::onLeaderElected);
    }

    /**
     * Processes a replication request on the replica.
     *
     * @param request Request to replication.
     * @return Response.
     */
    public CompletableFuture<?> processRequest(ReplicaRequest request) {
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        return listener.invoke(request);
    }

    /**
     * Replica group identity, this id is the same as the considered partition's id.
     *
     * @return Group id.
     */
    public ReplicationGroupId groupId() {
        return replicaGrpId;
    }

    private void onLeaderElected(ClusterNode clusterNode, Long term) {
        leaderRef.set(clusterNode);

        if (!leaderFuture.isDone()) {
            leaderFuture.complete(leaderRef);
        }
    }

    private CompletableFuture<ClusterNode> leaderFuture() {
        return leaderFuture.thenApply(AtomicReference::get);
    }

    /**
     * Process placement driver message.
     *
     * @param msg Message to process.
     * @return Future that contains a result.
     */
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        if (msg instanceof LeaseGrantedMessage) {
            return processLeaseGrantedMessage((LeaseGrantedMessage) msg);
        }

        return failedFuture(new AssertionError("Unknown message type, msg=" + msg));
    }

    /**
     * Process lease granted message. Can either accept lease or decline with redirection proposal. In the case of lease acceptance,
     * initiates the leadership transfer, if this replica is not a group leader.
     *
     * @param msg Message to process.
     * @return Future that contains a result.
     */
    public CompletableFuture<LeaseGrantedMessageResponse> processLeaseGrantedMessage(LeaseGrantedMessage msg) {
        return leaderFuture().thenCompose(leader -> {
            if (hasAcceptedLease(msg.leaseStartTime(), msg.leaseExpirationTime())) {
                return acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime());
            } else if (msg.force()) {
                if (!leader.equals(localNodeSupplier.get())) {
                    // Replica must wait till safe time reaches the lease start timestamp to make sure that all updates made on the
                    // group leader are received.
                    return safeTime.waitFor(msg.leaseStartTime()).thenCompose(v -> {
                        CompletableFuture<LeaseGrantedMessageResponse> respFut =
                                acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime());

                        return raftClient.transferLeadership(new Peer(localNodeSupplier.get().name()))
                                .thenCompose(ignored -> respFut);
                    });
                } else {
                    return acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime());
                }
            } else {
                if (leader.equals(localNodeSupplier.get())) {
                    return acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime());
                } else {
                    return proposeLeaseRedirect(leader);
                }
            }
        });
    }

    private CompletableFuture<LeaseGrantedMessageResponse> acceptLease(HybridTimestamp leaseStartTime, HybridTimestamp leaseExpirationTime) {
        synchronized (leaseAcceptanceMutex) {
            this.leaseStartTime = leaseStartTime;
            this.leaseExpirationTime = leaseExpirationTime;
        }

        LeaseGrantedMessageResponse resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                .accepted(true)
                .build();

        return completedFuture(resp);
    }

    private CompletableFuture<LeaseGrantedMessageResponse> proposeLeaseRedirect(ClusterNode groupLeader) {
        LeaseGrantedMessageResponse resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                .accepted(false)
                .redirectProposal(groupLeader.name())
                .build();

        return completedFuture(resp);
    }

    private boolean hasAcceptedLease(HybridTimestamp leaseStartTime, HybridTimestamp leaseExpirationTime) {
        synchronized (leaseAcceptanceMutex) {
            return leaseStartTime.equals(this.leaseStartTime) && leaseExpirationTime.equals(this.leaseExpirationTime);
        }
    }
}
