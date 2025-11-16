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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.rebalance.ChangePeersAndLearnersWithRetry;
import org.apache.ignite.internal.raft.rebalance.RaftWithTerm;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.util.IgniteBusyLock;

/**
 * Replica server.
 */
public class ReplicaImpl implements Replica {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ReplicaManager.class);

    /** Replica group identity, this id is the same as the considered partition's id. */
    private final ReplicationGroupId replicaGrpId;

    /** Replica listener. */
    private final ReplicaListener listener;

    private final IgniteBusyLock busyLock;

    private final ScheduledExecutorService rebalanceScheduler;

    /** Topology aware Raft client. */
    private final TopologyAwareRaftGroupService raftClient;

    /** Instance of the local node. */
    private final InternalClusterNode localNode;

    private final PlacementDriver placementDriver;

    private final Function<ReplicationGroupId, CompletableFuture<VersionedAssignments>> getPendingAssignmentsSupplier;

    private LeaderElectionListener onLeaderElectedFailoverCallback;

    private final FailureProcessor failureProcessor;

    private final PlacementDriverMessageProcessor placementDriverMessageProcessor;

    private final EventListener<PrimaryReplicaEventParameters> onPrimaryReplicaElected = this::registerFailoverCallback;

    private final EventListener<PrimaryReplicaEventParameters> onPrimaryReplicaExpired = this::unregisterFailoverCallback;

    private final ChangePeersAndLearnersWithRetry changePeersAndLearnersWithRetry;

    /**
     * The constructor of a replica server.
     *
     * @param replicaGrpId Replication group id.
     * @param listener Replica listener.
     * @param localNode Instance of the local node.
     * @param placementDriver Placement driver.
     * @param getPendingAssignmentsSupplier The supplier of pending assignments for rebalance failover purposes.
     * @param failureProcessor Failure processor in case if we couldn't subscribe failover callback on raft client.
     *
     */
    public ReplicaImpl(
            ReplicationGroupId replicaGrpId,
            ReplicaListener listener,
            InternalClusterNode localNode,
            PlacementDriver placementDriver,
            Function<ReplicationGroupId, CompletableFuture<VersionedAssignments>> getPendingAssignmentsSupplier,
            FailureProcessor failureProcessor,
            PlacementDriverMessageProcessor placementDriverMessageProcessor,
            IgniteBusyLock busyLock,
            ScheduledExecutorService rebalanceScheduler
    ) {
        this.replicaGrpId = replicaGrpId;
        this.listener = listener;
        this.busyLock = busyLock;
        this.rebalanceScheduler = rebalanceScheduler;
        this.raftClient = raftClient();
        this.localNode = localNode;
        this.placementDriver = placementDriver;
        this.getPendingAssignmentsSupplier = getPendingAssignmentsSupplier;
        this.failureProcessor = failureProcessor;
        this.placementDriverMessageProcessor = placementDriverMessageProcessor;
        this.changePeersAndLearnersWithRetry = new ChangePeersAndLearnersWithRetry(
                this.busyLock,
                this.rebalanceScheduler,
                () -> completedFuture(raftClient)
        );

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, onPrimaryReplicaElected);
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpired);
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

    @Override
    public CompletableFuture<? extends NetworkMessage> processPlacementDriverMessage(PlacementDriverReplicaMessage msg) {
        return placementDriverMessageProcessor.processPlacementDriverMessage(msg);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, onPrimaryReplicaElected);
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpired);

        listener.onShutdown();
        return raftClient.unsubscribeLeader()
                .thenAccept(v -> raftClient.shutdown());
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
                    if (!hasCause(e, NodeStoppingException.class)) {
                        String errorMessage = "Rebalance failover subscription on elected primary replica failed [groupId="
                                + replicaGrpId + "].";
                        failureProcessor.process(new FailureContext(e, errorMessage));
                    }

                    return null;
                })
                .thenApply(v -> false);
    }

    private void changePeersAndLearnersAsyncIfPendingExists(long term) {
        getPendingAssignmentsSupplier.apply(replicaGrpId).exceptionally(e -> {
            LOG.error("Couldn't fetch pending assignments for rebalance failover [groupId={}, term={}].", e, replicaGrpId, term);

            return null;
        }).thenCompose(versionedAssignments -> {
            if (versionedAssignments == null || versionedAssignments.assignmentsBytes() == null) {
                return nullCompletedFuture();
            }

            PeersAndLearners newConfiguration =
                    fromAssignments(AssignmentsQueue.fromBytes(versionedAssignments.assignmentsBytes()).poll().nodes());

            LOG.info(
                    "New leader elected. Going to apply new configuration [tablePartitionId={}, peers={}, learners={}]",
                    replicaGrpId,
                    newConfiguration.peers(),
                    newConfiguration.learners()
            );

            return changePeersAndLearnersWithRetry.execute(
                    newConfiguration,
                    versionedAssignments.revision(),
                    raftClient -> completedFuture(new RaftWithTerm(raftClient, term))
            );
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
