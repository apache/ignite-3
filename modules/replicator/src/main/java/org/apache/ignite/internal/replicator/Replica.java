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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.retryOperationUntilSuccess;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;

/**
 * Replica server.
 */
public class Replica {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Message factory. */
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Replica group identity, this id is the same as the considered partition's id. */
    private final ReplicationGroupId replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    /** Storage index tracker. */
    private final PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    /** Topology aware Raft client. */
    private final TopologyAwareRaftGroupService raftClient;

    /** Instance of the local node. */
    private final ClusterNode localNode;

    // TODO IGNITE-19120 after replica inoperability logic is introduced, this future should be replaced with something like
    //     VersionedValue (so that PlacementDriverMessages would wait for new leader election)
    /** Completes when leader is elected. */
    private final CompletableFuture<AtomicReference<ClusterNode>> leaderFuture = new CompletableFuture<>();

    /** Container of the elected leader. */
    private final AtomicReference<ClusterNode> leaderRef = new AtomicReference<>();

    /** Latest lease expiration time. */
    private volatile HybridTimestamp leaseExpirationTime;

    /** External executor. */
    // TODO: IGNITE-20063 Maybe get rid of it
    private final ExecutorService executor;

    private final PlacementDriver placementDriver;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param storageIndexTracker Storage index tracker.
     * @param raftClient Topology aware Raft client.
     * @param localNode Instance of the local node.
     * @param executor External executor.
     * @param placementDriver Placement driver.
     */
    public Replica(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            TopologyAwareRaftGroupService raftClient,
            ClusterNode localNode,
            ExecutorService executor,
            PlacementDriver placementDriver
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.storageIndexTracker = storageIndexTracker;
        this.raftClient = raftClient;
        this.localNode = localNode;
        this.executor = executor;
        this.placementDriver = placementDriver;

        raftClient.subscribeLeader(this::onLeaderElected);
    }

    /**
     * Returns an instance of replica listener, associated with current replica.
     */
    ReplicaListener replicaListener() {
        return listener;
    }

    /**
     * Processes a replication request on the replica.
     *
     * @param request Request to replication.
     * @param senderId Sender id.
     * @return Response.
     */
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, String senderId) {
        assert replicaGrpId.equals(request.groupId()) : IgniteStringFormatter.format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        return listener.invoke(request, senderId);
    }

    /**
     * Replica group identity, this id is the same as the considered partition's id.
     *
     * @return Group id.
     */
    public ReplicationGroupId groupId() {
        return replicaGrpId;
    }

    private void onLeaderElected(ClusterNode clusterNode, long term) {
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
    private CompletableFuture<LeaseGrantedMessageResponse> processLeaseGrantedMessage(LeaseGrantedMessage msg) {
        LOG.info("Received LeaseGrantedMessage for replica belonging to group=" + groupId() + ", force=" + msg.force());

        return placementDriver.previousPrimaryExpired(groupId()).thenCompose(unused -> leaderFuture().thenCompose(leader -> {
            HybridTimestamp leaseExpirationTime = this.leaseExpirationTime;

            if (leaseExpirationTime != null) {
                assert msg.leaseExpirationTime().after(leaseExpirationTime) : "Invalid lease expiration time in message, msg=" + msg;
            }

            if (msg.force()) {
                // Replica must wait till storage index reaches the current leader's index to make sure that all updates made on the
                // group leader are received.

                return waitForActualState(msg.leaseExpirationTime().getPhysical())
                        .thenCompose(v -> {
                            CompletableFuture<LeaseGrantedMessageResponse> respFut =
                                    acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime());

                            if (leader.equals(localNode)) {
                                return respFut;
                            } else {
                                return raftClient.transferLeadership(new Peer(localNode.name()))
                                        .thenCompose(ignored -> respFut);
                            }
                        });
            } else {
                if (leader.equals(localNode)) {
                    return waitForActualState(msg.leaseExpirationTime().getPhysical())
                            .thenCompose(v -> acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime()));
                } else {
                    return proposeLeaseRedirect(leader);
                }
            }
        }));
    }

    private CompletableFuture<LeaseGrantedMessageResponse> acceptLease(
            HybridTimestamp leaseStartTime,
            HybridTimestamp leaseExpirationTime
    ) {
        LOG.info("Lease accepted [group=" + groupId() + ", leaseStartTime=" + leaseStartTime + "].");

        this.leaseExpirationTime = leaseExpirationTime;

        LeaseGrantedMessageResponse resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                .accepted(true)
                .build();

        return completedFuture(resp);
    }

    private CompletableFuture<LeaseGrantedMessageResponse> proposeLeaseRedirect(ClusterNode groupLeader) {
        LOG.info("Proposing lease redirection, proposed node=" + groupLeader);

        LeaseGrantedMessageResponse resp = PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                .accepted(false)
                .redirectProposal(groupLeader.name())
                .build();

        return completedFuture(resp);
    }

    /**
     * Tries to read index from group leader and wait for this index to appear in local storage. Can possible return failed future with
     * timeout exception, and in this case, replica would not answer to placement driver, because the response is useless. Placement driver
     * should handle this.
     *
     * @param expirationTime Lease expiration time.
     * @return Future that is completed when local storage catches up the index that is actual for leader on the moment of request.
     */
    private CompletableFuture<Void> waitForActualState(long expirationTime) {
        LOG.info("Waiting for actual storage state, group=" + groupId());

        long timeout = expirationTime - currentTimeMillis();
        if (timeout <= 0) {
            return failedFuture(new TimeoutException());
        }

        return retryOperationUntilSuccess(raftClient::readIndex, e -> currentTimeMillis() > expirationTime, executor)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .thenCompose(storageIndexTracker::waitFor);
    }

    /**
     * Returns consistent id of the most convenient primary node.
     *
     * @return Node consistent id.
     */
    public String proposedPrimary() {
        Peer leased = raftClient.leader();

        return leased != null ? leased.consistentId() : localNode.name();
    }

    /**
     * Shutdowns the replica.
     */
    public CompletableFuture<Void> shutdown() {
        listener.onShutdown();
        return raftClient.unsubscribeLeader();
    }
}
