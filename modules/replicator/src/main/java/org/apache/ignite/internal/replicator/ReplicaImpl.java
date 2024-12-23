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
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.retryOperationUntilSuccess;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.apache.ignite.network.ClusterNode;

/**
 * Replica server.
 */
public class ReplicaImpl implements Replica {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Message factory. */
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

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

    private final ClockService clockService;

    private final BiFunction<ReplicationGroupId, HybridTimestamp, Boolean> replicaReservationClosure;

    private final Function<ReplicationGroupId, CompletableFuture<byte[]>> getPendingAssignmentsSupplier;

    private LeaderElectionListener onLeaderElectedFailoverCallback;

    private final FailureManager failureManager;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param storageIndexTracker Storage index tracker.
     * @param localNode Instance of the local node.
     * @param executor External executor.
     * @param placementDriver Placement driver.
     * @param clockService Clock service.
     * @param replicaReservationClosure Closure that will be called to reserve the replica for becoming primary. It returns whether
     *     the reservation was successful.
     * @param getPendingAssignmentsSupplier The supplier of pending assignments for rebalance failover purposes.
     * @param failureManager Failure manager in case if we couldn't subscribe failover callback on raft client.
     *
     */
    public ReplicaImpl(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            PendingComparableValuesTracker<Long, Void> storageIndexTracker,
            ClusterNode localNode,
            ExecutorService executor,
            PlacementDriver placementDriver,
            ClockService clockService,
            BiFunction<ReplicationGroupId, HybridTimestamp, Boolean> replicaReservationClosure,
            Function<ReplicationGroupId, CompletableFuture<byte[]>> getPendingAssignmentsSupplier,
            FailureManager failureManager
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.storageIndexTracker = storageIndexTracker;
        this.raftClient = raftClient();
        this.localNode = localNode;
        this.executor = executor;
        this.placementDriver = placementDriver;
        this.clockService = clockService;
        this.replicaReservationClosure = replicaReservationClosure;
        this.getPendingAssignmentsSupplier = getPendingAssignmentsSupplier;
        this.failureManager = failureManager;

        raftClient.subscribeLeader(this::onLeaderElected);

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, this::registerFailoverCallback);
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::unregisterFailoverCallback);
    }

    @Override
    public ReplicaListener listener() {
        return listener;
    }

    @Override
    public final TopologyAwareRaftGroupService raftClient() {
        return (TopologyAwareRaftGroupService) listener.raftClient();
    }

    @Override
    public CompletableFuture<ReplicaResult> processRequest(ReplicaRequest request, UUID senderId) {
        assert replicaGrpId.equals(request.groupId().asReplicationGroupId()) : format(
                "Partition mismatch: request does not match the replica [reqReplicaGrpId={}, replicaGrpId={}]",
                request.groupId(),
                replicaGrpId);

        return listener.invoke(request, senderId);
    }

    @Override
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

    @Override
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        if (msg instanceof LeaseGrantedMessage) {
            return processLeaseGrantedMessage((LeaseGrantedMessage) msg)
                    .handle((v, e) -> {
                        if (e != null) {
                            Throwable ex = unwrapCause(e);

                            if (!(ex instanceof NodeStoppingException) && !(ex instanceof TrackerClosedException)) {
                                LOG.warn("Failed to process the lease granted message [msg={}].", ex, msg);
                            }

                            // Just restart the negotiation in case of exception.
                            return PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessageResponse()
                                    .accepted(false)
                                    .build();
                        } else {
                            return v;
                        }
                    });
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
        LOG.info("Received LeaseGrantedMessage for replica [groupId={}, leaseStartTime={}, force={}].", groupId(), msg.leaseStartTime(),
                msg.force());

        return placementDriver.previousPrimaryExpired(groupId()).thenCompose(unused -> leaderFuture().thenCompose(leader -> {
            HybridTimestamp leaseExpirationTime = this.leaseExpirationTime;

            if (leaseExpirationTime != null) {
                assert clockService.after(msg.leaseExpirationTime(), leaseExpirationTime)
                        : "Invalid lease expiration time in message, msg=" + msg;
            }

            if (msg.force()) {
                // Replica must wait till storage index reaches the current leader's index to make sure that all updates made on the
                // group leader are received.
                return waitForActualState(msg.leaseStartTime(), msg.leaseExpirationTime().getPhysical())
                        .thenCompose(v -> sendPrimaryReplicaChangeToReplicationGroup(
                                msg.leaseStartTime().longValue(),
                                localNode.id(),
                                localNode.name()
                        ))
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
                    return waitForActualState(msg.leaseStartTime(), msg.leaseExpirationTime().getPhysical())
                            .thenCompose(v -> sendPrimaryReplicaChangeToReplicationGroup(
                                    msg.leaseStartTime().longValue(),
                                    localNode.id(),
                                    localNode.name()
                            ))
                            .thenCompose(v -> acceptLease(msg.leaseStartTime(), msg.leaseExpirationTime()));
                } else {
                    return proposeLeaseRedirect(leader);
                }
            }
        }));
    }

    private CompletableFuture<Void> sendPrimaryReplicaChangeToReplicationGroup(
            long leaseStartTime,
            UUID primaryReplicaNodeId,
            String primaryReplicaNodeName
    ) {
        PrimaryReplicaChangeCommand cmd = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                .leaseStartTime(leaseStartTime)
                .primaryReplicaNodeId(primaryReplicaNodeId)
                .primaryReplicaNodeName(primaryReplicaNodeName)
                .build();

        return raftClient.run(cmd);
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
        LOG.info("Proposing lease redirection [groupId={}, proposed node={}].", groupId(), groupLeader);

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
     * @param startTime Lease start time.
     * @param expirationTime Lease expiration time.
     * @return Future that is completed when local storage catches up the index that is actual for leader on the moment of request.
     */
    private CompletableFuture<Void> waitForActualState(HybridTimestamp startTime, long expirationTime) {
        LOG.info("Waiting for actual storage state, group=" + groupId());

        if (!replicaReservationClosure.apply(groupId(), startTime)) {
            throw new IllegalStateException("Replica reservation failed [groupId=" + groupId() + ", leaseStartTime=" + startTime + "].");
        }

        long timeout = expirationTime - currentTimeMillis();
        if (timeout <= 0) {
            return failedFuture(new TimeoutException());
        }

        return retryOperationUntilSuccess(raftClient::readIndex, e -> currentTimeMillis() > expirationTime, executor)
                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                .thenCompose(storageIndexTracker::waitFor);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, this::registerFailoverCallback);
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, this::unregisterFailoverCallback);

        listener.onShutdown();
        return raftClient.unsubscribeLeader()
                .thenAccept(v -> raftClient.shutdown());
    }

    @Override
    public void updatePeersAndLearners(PeersAndLearners peersAndLearners) {
        raftClient.updateConfiguration(peersAndLearners);
    }

    @Override
    public CompletableFuture<Void> createSnapshotOn(Member targetMember) {
        Peer peer = targetMember.isVotingMember()
                ? new Peer(targetMember.consistentId(), 0)
                : new Peer(targetMember.consistentId(), 1);

        return raftClient.snapshot(peer);
    }

    @Override
    public CompletableFuture<Void> transferLeadershipTo(String targetConsistentId) {
        return raftClient.transferLeadership(new Peer(targetConsistentId));
    }

    private CompletableFuture<Boolean> registerFailoverCallback(PrimaryReplicaEventParameters parameters) {
        if (!parameters.leaseholderId().equals(localNode.id()) || !(replicaGrpId.equals(parameters.groupId()))) {
            return falseCompletedFuture();
        }

        assert onLeaderElectedFailoverCallback == null : format(
                "We already have failover subscription [thisGrpId={}, thisNode={}, givenExpiredPrimaryId={}, givenExpiredPrimaryNode={}",
                replicaGrpId,
                localNode.name(),
                parameters.groupId(),
                parameters.leaseholder()
        );

        onLeaderElectedFailoverCallback = (leaderNode, term) -> changePeersAndLearnersAsyncIfPendingExists(term);

        return raftClient
                .subscribeLeader(onLeaderElectedFailoverCallback)
                .exceptionally(e -> {
                    LOG.error("Rebalance failover subscription on elected primary replica failed [groupId=" + replicaGrpId + "].", e);

                    failureManager.process(new FailureContext(CRITICAL_ERROR, e));

                    return null;
                })
                .thenApply(v -> false);
    }

    private void changePeersAndLearnersAsyncIfPendingExists(long term) {
        getPendingAssignmentsSupplier.apply(replicaGrpId).exceptionally(e -> {
            LOG.error("Couldn't fetch pending assignments for rebalance failover [groupId={}, term={}].", e, replicaGrpId, term);

            return null;
        }).thenCompose(pendingsBytes -> {
            if (pendingsBytes == null) {
                return nullCompletedFuture();
            }

            PeersAndLearners newConfiguration = fromAssignments(Assignments.fromBytes(pendingsBytes).nodes());

            LOG.info(
                    "New leader elected. Going to apply new configuration [tablePartitionId={}, peers={}, learners={}]",
                    replicaGrpId,
                    newConfiguration.peers(),
                    newConfiguration.learners()
            );

            // TODO: add retries on fail https://issues.apache.org/jira/browse/IGNITE-23633
            return raftClient.changePeersAndLearnersAsync(newConfiguration, term);
        }).exceptionally(e -> {
            LOG.error("Failover ChangePeersAndLearners failed [groupId={}, term={}].", e, replicaGrpId, term);

            return null;
        });
    }

    private CompletableFuture<Boolean> unregisterFailoverCallback(PrimaryReplicaEventParameters parameters) {
        if (!parameters.leaseholderId().equals(localNode.id()) || !(replicaGrpId.equals(parameters.groupId()))) {
            return falseCompletedFuture();
        }

        /* This check defends us from the situation:
         * 1. 3 nodes {A, B, C} are started with a created table with replica factor 3
         * 2. Partition 16_part_0 starts and A.16_0 became a primary replica.
         * 3. A.16_0 partition restarts then there no more onLeaderElectedFailoverCallback neither the failover callback subscription.
         * 4. Primary replica expires and trigger the event.
         * 5. A.16_0 receives the event, found itself, but the callback is null due to the node A restart.
         *
         * In the case we should so nothing.
         */
        if (onLeaderElectedFailoverCallback == null) {
            return falseCompletedFuture();
        }

        assert onLeaderElectedFailoverCallback != null : format(
                "We have no failover subscription [thisGrpId={}, thisNode={}, givenExpiredPrimaryId={}, givenExpiredPrimaryNode={}",
                replicaGrpId,
                localNode.name(),
                parameters.groupId(),
                parameters.leaseholder()
        );

        raftClient.unsubscribeLeader(onLeaderElectedFailoverCallback);

        onLeaderElectedFailoverCallback = null;

        return falseCompletedFuture();
    }
}
