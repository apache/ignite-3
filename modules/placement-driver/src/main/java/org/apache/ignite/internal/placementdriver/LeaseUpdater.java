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

package org.apache.ignite.internal.placementdriver;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.leases.Lease.emptyLease;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteTuple3;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.leases.Leases;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverActorMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessageResponse;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseNegotiator;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * A processor to manger leases. The process is started when placement driver activates and stopped when it deactivates.
 */
public class LeaseUpdater {
    /** Message factory. */
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseUpdater.class);

    /** Update attempts interval in milliseconds. */
    private static final long UPDATE_LEASE_MS = 500L;

    /** The lock is available when the actor is changing state. */
    private final IgniteSpinBusyLock stateChangingLock = new IgniteSpinBusyLock();

    private final AtomicBoolean active = new AtomicBoolean();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    private final FailureProcessor failureProcessor;

    /** Assignments tracker. */
    private final AssignmentsTracker assignmentsTracker;

    /** Topology tracker. */
    private final TopologyTracker topologyTracker;

    private final ReplicationConfiguration replicationConfiguration;

    /** Lease tracker. */
    private final LeaseTracker leaseTracker;

    /** Cluster clock. */
    private final ClockService clockService;

    /** Closure to update leases. */
    private final Updater updater;

    /** Dedicated thread to update leases. */
    private IgniteThread updaterThread;

    /** Processor to communicate with the leaseholder to negotiate the lease. */
    private LeaseNegotiator leaseNegotiator;

    /** Node name. */
    private final String nodeName;

    private final Executor throttledLogExecutor;

    /**
     * Constructor.
     *
     * @param clusterService Cluster service.
     * @param msManager Meta storage manager.
     * @param topologyService Topology service.
     * @param leaseTracker Lease tracker.
     * @param clockService Clock service.
     * @param assignmentsTracker Assignments tracker.
     * @param replicationConfiguration Replication configuration.
     * @param throttledLogExecutor Executor to clean up the throttled logger cache.
     */
    LeaseUpdater(
            String nodeName,
            ClusterService clusterService,
            MetaStorageManager msManager,
            FailureProcessor failureProcessor,
            LogicalTopologyService topologyService,
            LeaseTracker leaseTracker,
            ClockService clockService,
            AssignmentsTracker assignmentsTracker,
            ReplicationConfiguration replicationConfiguration,
            Executor throttledLogExecutor
    ) {
        this.nodeName = nodeName;
        this.clusterService = clusterService;
        this.msManager = msManager;
        this.failureProcessor = failureProcessor;
        this.leaseTracker = leaseTracker;
        this.clockService = clockService;
        this.replicationConfiguration = replicationConfiguration;

        this.assignmentsTracker = assignmentsTracker;
        this.topologyTracker = new TopologyTracker(topologyService);
        this.updater = new Updater();
        this.throttledLogExecutor = throttledLogExecutor;

        clusterService.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, new PlacementDriverActorMessageHandler());
    }

    /** Initializes the class. */
    public void init() {
        topologyTracker.startTrack();
    }

    /** De-initializes the class. */
    void deInit() {
        topologyTracker.stopTrack();
    }

    /** Activates a lease updater to renew leases. */
    void activate() {
        if (active()) {
            return;
        }

        stateChangingLock.block();

        try {
            if (!active.compareAndSet(false, true)) {
                return;
            }

            LOG.info("Placement driver active actor is starting.");

            leaseNegotiator = new LeaseNegotiator(clusterService, throttledLogExecutor);

            updaterThread = new IgniteThread(nodeName, "lease-updater", updater);

            updaterThread.start();
        } finally {
            stateChangingLock.unblock();
        }
    }

    /** Stops a dedicated thread to renew or assign leases. */
    void deactivate() {
        if (!active()) {
            return;
        }

        stateChangingLock.block();

        try {
            if (!active.compareAndSet(true, false)) {
                return;
            }

            LOG.info("Placement driver active actor is stopping.");

            leaseNegotiator = null;

            updaterThread.interrupt();

            this.updaterThread = null;

        } finally {
            stateChangingLock.unblock();
        }
    }

    /**
     * Denies a lease and write the denied one to Meta storage.
     *
     * @param grpId Replication group id.
     * @param lease Lease to deny.
     * @param redirectProposal Consistent id of the cluster node proposed for redirection.
     * @return Future that contains the expiration time of denied lease, of {@code null} if lease denial failed.
     */
    private CompletableFuture<HybridTimestamp> denyLease(ReplicationGroupId grpId, Lease lease, @Nullable String redirectProposal) {
        Lease deniedLease = lease.denyLease(redirectProposal);

        leaseNegotiator.cancelAgreement(grpId);

        Leases leasesCurrent = leaseTracker.leasesCurrent();

        Collection<Lease> currentLeases = leasesCurrent.leaseByGroupId().values();

        ByteArray key = PLACEMENTDRIVER_LEASES_KEY;

        IgniteTuple3<List<Lease>, Boolean, Boolean> renewedLeasesTup = replaceProlongableLeaseInCollection(currentLeases, deniedLease);

        if (!renewedLeasesTup.get3()) {
            // If lease not found, return current time: the lease that don't exist can't be denied.
            return completedFuture(clockService.now());
        } else if (!renewedLeasesTup.get2()) {
            // If lease was not replaced, return null: the operation may be retried by caller.
            return nullCompletedFuture();
        } else {
            return msManager.invoke(
                    or(notExists(key), value(key).eq(leasesCurrent.leasesBytes())),
                    put(key, new LeaseBatch(renewedLeasesTup.get1()).bytes()),
                    noop()
            ).thenApply(res -> {
                if (res) {
                    return deniedLease.getExpirationTime();
                } else {
                    return null;
                }
            });
        }
    }

    private static IgniteTuple3<List<Lease>, Boolean, Boolean> replaceProlongableLeaseInCollection(
            Collection<Lease> leases,
            Lease newLease
    ) {
        List<Lease> renewedLeases = new ArrayList<>();
        boolean replaced = false;
        boolean found = false;

        for (Lease ls : leases) {
            if (ls.replicationGroupId().equals(newLease.replicationGroupId())) {
                found = true;

                if (ls.getStartTime().equals(newLease.getStartTime()) && ls.isProlongable()) {
                    renewedLeases.add(newLease);
                    replaced = true;
                }
            } else {
                renewedLeases.add(ls);
            }
        }

        return new IgniteTuple3<>(renewedLeases, replaced, found);
    }

    /**
     * Finds a node that can be the leaseholder. Stable assignments nodes set is the top priority for searching. If there no any candidate
     * among stable assignments set (e.g. all nodes from stable assignments aren't alive), then the method would search among pending
     * assignments nodes set. If even pending assignments we couldn't find a candidate, then the method returns {@code null}.
     *
     * @param stableAssignments Replication group stable assignments set that is the top priority for a candidate selection.
     * @param pendingAssignments Replication group pending assignments set that is used in case if the method didn't find any candidate
     *      among stable assignments nodes set.
     * @param grpId Replication group's identifier of a group for which one the method tries to find a lease candidate.
     * @param proposedConsistentId Proposed consistent id, found out of a lease negotiation. The parameter might be {@code null}.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    private @Nullable InternalClusterNode nextLeaseHolder(
            Set<Assignment> stableAssignments,
            Set<Assignment> pendingAssignments,
            ReplicationGroupId grpId,
            @Nullable String proposedConsistentId
    ) {
        InternalClusterNode primaryCandidate =  tryToFindCandidateAmongAssignments(stableAssignments, grpId, proposedConsistentId);

        // If there wasn't a candidate among stable assignments set then make attempt to select a candidate among pending set
        if (primaryCandidate == null) {
            primaryCandidate = tryToFindCandidateAmongAssignments(pendingAssignments, grpId, proposedConsistentId);
        }

        return primaryCandidate;
    }

    private @Nullable InternalClusterNode tryToFindCandidateAmongAssignments(
            Set<Assignment> assignments,
            ReplicationGroupId grpId,
            @Nullable String proposedConsistentId
    ) {
        // TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        InternalClusterNode primaryCandidate = null;

        for (Assignment assignment : assignments) {
            if (!assignment.isPeer()) {
                continue;
            }

            // Check whether given assignments is actually available in logical topology. It's a best effort check because it's possible
            // for proposed primary candidate to leave the topology at any time. In that case primary candidate will be recalculated.
            InternalClusterNode candidateNode = topologyTracker.nodeByConsistentId(assignment.consistentId());

            if (candidateNode == null) {
                continue;
            }

            if (assignment.consistentId().equals(proposedConsistentId)) {
                primaryCandidate = candidateNode;

                break;
            } else if (primaryCandidate == null) {
                primaryCandidate = candidateNode;
            } else {
                int candidateHash = hash(primaryCandidate.name(), grpId);
                int assignmentHash = hash(assignment.consistentId(), grpId);

                if (candidateHash > assignmentHash) {
                    primaryCandidate = candidateNode;
                }
            }
        }

        return primaryCandidate;
    }

    /** Returns {@code true} if active. */
    boolean active() {
        return active.get();
    }

    /** Runnable to update lease in Meta storage. */
    private class Updater implements Runnable {
        private int activeLeaseCount;
        private int leaseWithoutCandidateCount;

        /** Previous meta storage update future. */
        private CompletableFuture<Void> previousMsUpdate =  completedFuture(null);

        @Override
        public void run() {
            while (active() && !Thread.interrupted()) {
                if (!stateChangingLock.enterBusy()) {
                    continue;
                }

                try {
                    if (active()) {
                        updateLeaseBatchInternal();
                    }
                } catch (Throwable e) {
                    failureProcessor.process(new FailureContext(e, "Error occurred when updating the leases."));

                    if (e instanceof Error) {
                        // TODO IGNITE-20368 The node should be halted in case of an error here.
                        throw (Error) e;
                    }
                } finally {
                    stateChangingLock.leaveBusy();
                }

                try {
                    Thread.sleep(UPDATE_LEASE_MS);
                } catch (InterruptedException e) {
                    LOG.info("Lease updater is interrupted");
                }
            }
        }

        /** Updates leases in Meta storage. This method is supposed to be used in the busy lock. */
        private void updateLeaseBatchInternal() {
            HybridTimestamp currentTime = clockService.current();

            if (!previousMsUpdate.isDone()) {
                LOG.warn("Previous lease update is still in progress, skipping this round.");

                return;
            }

            long leaseExpirationInterval = replicationConfiguration.leaseExpirationIntervalMillis().value();

            long outdatedLeaseThreshold = currentTime.getPhysical() + leaseExpirationInterval / 2;

            HybridTimestamp newExpirationTimestamp = new HybridTimestamp(currentTime.getPhysical() + leaseExpirationInterval, 0);

            Leases leasesCurrent = leaseTracker.leasesCurrent();
            Map<ReplicationGroupId, LeaseAgreement> toBeNegotiated = new HashMap<>();
            Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>(leasesCurrent.leaseByGroupId().size());

            Map<ReplicationGroupId, TokenizedAssignments> tokenizedStableAssignmentsMap = assignmentsTracker.stableAssignments();
            Map<ReplicationGroupId, TokenizedAssignments> tokenizedPendingAssignmentsMap = assignmentsTracker.pendingAssignments();

            Set<ReplicationGroupId> groupsAmongCurrentStableAndPendingAssignments = union(
                    tokenizedPendingAssignmentsMap.keySet(),
                    tokenizedStableAssignmentsMap.keySet()
            );

            Map<ReplicationGroupId, Pair<Set<Assignment>, Set<Assignment>>> aggregatedStableAndPendingAssignmentsByGroups = new HashMap<>();

            for (ReplicationGroupId grpId : groupsAmongCurrentStableAndPendingAssignments) {
                Set<Assignment> stables = getAssignmentsFromTokenizedAssignmentsMap(grpId, tokenizedStableAssignmentsMap);
                Set<Assignment> pendings = getAssignmentsFromTokenizedAssignmentsMap(grpId, tokenizedPendingAssignmentsMap);

                aggregatedStableAndPendingAssignmentsByGroups.put(grpId, new Pair<>(stables, pendings));
            }

            int activeLeaseCount = 0;
            int leaseWithoutCandidateCount = 0;

            Set<ReplicationGroupId> prolongableLeaseGroupIds = new HashSet<>();

            for (Map.Entry<ReplicationGroupId, Pair<Set<Assignment>, Set<Assignment>>> entry
                    : aggregatedStableAndPendingAssignmentsByGroups.entrySet()
            ) {
                ReplicationGroupId grpId = entry.getKey();

                Set<Assignment> stableAssignments = entry.getValue().getFirst();
                Set<Assignment> pendingAssignments = entry.getValue().getSecond();

                Lease lease = requireNonNullElse(leasesCurrent.leaseByGroupId().get(grpId), emptyLease(grpId));

                if (lease.isAccepted() && !isLeaseOutdated(lease)) {
                    activeLeaseCount++;
                }

                if (!lease.isAccepted()) {
                    LeaseAgreement agreement = leaseNegotiator.getAndRemoveIfReady(grpId);

                    agreement.checkValid(grpId, topologyTracker.currentTopologySnapshot(), union(stableAssignments, pendingAssignments));

                    if (lease.isProlongable() && agreement.isAccepted()) {
                        Lease negotiatedLease = agreement.getLease();

                        publishLease(grpId, negotiatedLease, renewedLeases, leaseExpirationInterval);

                        continue;
                    } else if (!lease.isProlongable() || agreement.isDeclined()) {
                        // Here we initiate negotiations for UNDEFINED_AGREEMENT and retry them on newly started active actor as well.
                        // Also, if the lease was denied, we create the new one.
                        chooseCandidateAndCreateNewLease(
                                grpId,
                                lease,
                                agreement,
                                stableAssignments,
                                pendingAssignments,
                                renewedLeases,
                                toBeNegotiated
                        );

                        continue;
                    }
                }

                // Calculate candidate for any accepted lease to define whether it can be prolonged (prolongation is possible only
                // if the candidate is the same as the current leaseholder).
                String proposedLeaseholder = lease.isProlongable()
                        ? lease.getLeaseholder()
                        : lease.proposedCandidate();

                InternalClusterNode candidate = nextLeaseHolder(stableAssignments, pendingAssignments, grpId, proposedLeaseholder);

                boolean canBeProlonged = lease.isAccepted()
                        && lease.isProlongable()
                        && candidate != null && candidate.id().equals(lease.getLeaseholderId());

                // The lease is expired or close to this.
                if (lease.getExpirationTime().getPhysical() < outdatedLeaseThreshold) {
                    // If we couldn't find a candidate neither stable nor pending assignments set, so update stats and skip iteration
                    if (candidate == null) {
                        leaseWithoutCandidateCount++;

                        continue;
                    }

                    // We can't prolong the expired lease because we already have an interval of time when the lease was not active,
                    // so we must start a negotiation round from the beginning; the same we do for the groups that don't have
                    // leaseholders at all.
                    if (isLeaseOutdated(lease)) {
                        // New lease is granted.
                        Lease newLease = writeNewLease(grpId, candidate, renewedLeases);

                        boolean force = !lease.isProlongable() && lease.proposedCandidate() != null;

                        toBeNegotiated.put(grpId, new LeaseAgreement(newLease, force));
                    } else if (canBeProlonged) {
                        // Old lease is renewed.
                        renewedLeases.put(grpId, prolongLease(lease, newExpirationTimestamp));
                    }
                } else if (canBeProlonged) {
                    prolongableLeaseGroupIds.add(grpId);
                }
            }

            ByteArray key = PLACEMENTDRIVER_LEASES_KEY;

            this.activeLeaseCount = activeLeaseCount;
            this.leaseWithoutCandidateCount = leaseWithoutCandidateCount;

            // This condition allows to skip the meta storage invoke when there are no leases to update (renewedLeases.isEmpty()).
            // However there is the case when we need to save empty leases collection: when the assignments are empty and
            // leasesCurrent (those that reflect the meta storage state) is not empty. The negation of this condition gives us
            // the condition to skip the update and the result is:
            // !(emptyAssignments && !leasesCurrent.isEmpty()) == (!emptyAssignments || leasesCurrent.isEmpty())
            boolean emptyAssignments = aggregatedStableAndPendingAssignmentsByGroups.isEmpty();
            if (renewedLeases.isEmpty() && (!emptyAssignments || leasesCurrent.leaseByGroupId().isEmpty())) {
                LOG.debug("No leases to update found.");
                return;
            }

            leasesCurrent.leaseByGroupId().forEach(renewedLeases::putIfAbsent);

            for (Iterator<Entry<ReplicationGroupId, Lease>> iter = renewedLeases.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<ReplicationGroupId, Lease> entry = iter.next();
                ReplicationGroupId groupId = entry.getKey();
                Lease lease = entry.getValue();

                if (clockService.before(lease.getExpirationTime(), currentTime)
                        && !groupsAmongCurrentStableAndPendingAssignments.contains(groupId)) {
                    iter.remove();
                    leaseNegotiator.cancelAgreement(groupId);
                } else if (prolongableLeaseGroupIds.contains(groupId)) {
                    entry.setValue(prolongLease(lease, newExpirationTimestamp));
                }
            }

            byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

            previousMsUpdate = msManager.invoke(
                    or(notExists(key), value(key).eq(leasesCurrent.leasesBytes())),
                    put(key, renewedValue),
                    noop()
            ).whenComplete((success, e) -> {
                long duration = FastTimestamps.coarseCurrentTimeMillis() - currentTime.getPhysical();

                if (duration > leaseExpirationInterval) {
                    LOG.warn("Lease update invocation took longer than lease interval [duration={}, leaseInterval={}].",
                            duration, leaseExpirationInterval);
                }

                if (e != null) {
                    if (!hasCause(e, NodeStoppingException.class)) {
                        failureProcessor.process(new FailureContext(e, "Lease update invocation failed"));
                    }

                    return;
                }

                if (!success) {
                    LOG.warn("Lease update invocation failed because of outdated lease data on this node.");

                    return;
                }

                for (Map.Entry<ReplicationGroupId, LeaseAgreement> entry : toBeNegotiated.entrySet()) {
                    leaseNegotiator.negotiate(entry.getValue());
                }
            }).thenApply(unused -> null);
        }

        private void chooseCandidateAndCreateNewLease(
                ReplicationGroupId grpId,
                Lease existingLease,
                LeaseAgreement agreement,
                Set<Assignment> stableAssignments,
                Set<Assignment> pendingAssignments,
                Map<ReplicationGroupId, Lease> renewedLeases,
                Map<ReplicationGroupId, LeaseAgreement> toBeNegotiated
        ) {
            String proposedCandidate = null;

            if (agreement.isDeclined()) {
                proposedCandidate = agreement.getRedirectTo();
            }

            if (proposedCandidate == null) {
                proposedCandidate = existingLease.isProlongable() ? existingLease.getLeaseholder() : existingLease.proposedCandidate();
            }

            InternalClusterNode candidate = nextLeaseHolder(stableAssignments, pendingAssignments, grpId, proposedCandidate);

            if (candidate == null) {
                leaseWithoutCandidateCount++;

                return;
            }

            // New lease is granted.
            Lease newLease = writeNewLease(grpId, candidate, renewedLeases);

            boolean force = Objects.equals(existingLease.getLeaseholder(), candidate.name()) && !agreement.isCancelled();

            toBeNegotiated.put(grpId, new LeaseAgreement(newLease, force));
        }

        /**
         * Writes a new lease.
         *
         * @param grpId Replication group id.
         * @param candidate Lease candidate.
         * @param renewedLeases Leases to renew.
         * @return Created lease.
         */
        private Lease writeNewLease(
                ReplicationGroupId grpId,
                InternalClusterNode candidate,
                Map<ReplicationGroupId, Lease> renewedLeases
        ) {
            HybridTimestamp startTs = clockService.now();

            long interval = replicationConfiguration.leaseAgreementAcceptanceTimeLimitMillis().value();

            var expirationTs = new HybridTimestamp(startTs.getPhysical() + interval, 0);

            Lease renewedLease = new Lease(candidate.name(), candidate.id(), startTs, expirationTs, grpId);

            renewedLeases.put(grpId, renewedLease);

            return renewedLease;
        }

        /**
         * Prolongs the lease.
         *
         * @param lease Lease to prolong.
         * @param newExpirationTimestamp New expiration timestamp.
         * @return Prolonged lease.
         */
        private Lease prolongLease(
                Lease lease,
                HybridTimestamp newExpirationTimestamp
        ) {
            return lease.prolongLease(newExpirationTimestamp);
        }

        /**
         * Writes an accepted lease in Meta storage. After the lease will be written to Meta storage,
         * the lease becomes available to all components.
         *
         * @param grpId Replication group id.
         * @param lease Lease to accept.
         */
        private void publishLease(
                ReplicationGroupId grpId,
                Lease lease,
                Map<ReplicationGroupId, Lease> renewedLeases,
                long leaseExpirationInterval
        ) {
            var newTs = new HybridTimestamp(clockService.now().getPhysical() + leaseExpirationInterval, 0);

            Lease renewedLease = lease.acceptLease(newTs);

            renewedLeases.put(grpId, renewedLease);
        }

        /**
         * Checks that the lease is outdated.
         * {@link Lease#emptyLease} is always outdated.
         *
         * @param lease Lease.
         * @return True when the candidate can be a leaseholder, otherwise false.
         */
        private boolean isLeaseOutdated(Lease lease) {
            HybridTimestamp now = clockService.now();

            return clockService.after(now, lease.getExpirationTime());
        }

        private Set<Assignment> getAssignmentsFromTokenizedAssignmentsMap(
                ReplicationGroupId grpId,
                Map<ReplicationGroupId, TokenizedAssignments> tokenizedAssignmentsMap
        ) {
            TokenizedAssignments pendingTokenizedAssignments = tokenizedAssignmentsMap.get(grpId);

            return pendingTokenizedAssignments == null
                    ? new HashSet<>()
                    : pendingTokenizedAssignments.nodes();
        }

        int activeLeaseCount() {
            return activeLeaseCount;
        }

        int leaseWithoutCandidatesCount() {
            return leaseWithoutCandidateCount;
        }
    }

    /** Message handler to process notification from replica side. */
    private class PlacementDriverActorMessageHandler implements NetworkMessageHandler {
        @Override
        public void onReceived(NetworkMessage msg0, InternalClusterNode sender, @Nullable Long correlationId) {
            if (!(msg0 instanceof PlacementDriverActorMessage)) {
                return;
            }

            var msg = (PlacementDriverActorMessage) msg0;

            if (!active() || !stateChangingLock.enterBusy()) {
                return;
            }

            try {
                processMessageInternal(sender.name(), msg, correlationId);
            } finally {
                stateChangingLock.leaveBusy();
            }
        }

        /**
         * Processes an placement driver actor message. The method should be invoked under state lock.
         *
         * @param sender Sender node name.
         * @param msg Message.
         * @param correlationId Correlation id.
         */
        private void processMessageInternal(String sender, PlacementDriverActorMessage msg, @Nullable Long correlationId) {
            ReplicationGroupId grpId = msg.groupId();

            Lease lease = leaseTracker.getLease(grpId);

            if (msg instanceof StopLeaseProlongationMessage) {
                if (sender.equals(lease.getLeaseholder())) {
                    StopLeaseProlongationMessage stopLeaseProlongationMessage = (StopLeaseProlongationMessage) msg;

                    denyLease(grpId, lease, stopLeaseProlongationMessage.redirectProposal()).whenComplete((deniedLeaseExpTime, th) -> {
                        if (th != null) {
                            LOG.warn("Prolongation denial failed due to exception [groupId={}]", th, grpId);
                        } else {
                            LOG.info("Stop lease prolongation message was handled [groupId={}, leaseStartTime={}, leaseExpirationTime={}, "
                                            + "sender={}, denied={}]", grpId, lease.getStartTime(), deniedLeaseExpTime, sender,
                                    deniedLeaseExpTime != null);
                        }

                        if (correlationId != null) {
                            long deniedLeaseExpTimeLong = deniedLeaseExpTime == null
                                    ? NULL_HYBRID_TIMESTAMP
                                    : deniedLeaseExpTime.longValue();

                            StopLeaseProlongationMessageResponse response = PLACEMENT_DRIVER_MESSAGES_FACTORY
                                    .stopLeaseProlongationMessageResponse()
                                    .deniedLeaseExpirationTimeLong(deniedLeaseExpTimeLong)
                                    .build();

                            clusterService.messagingService().respond(sender, response, correlationId);
                        }
                    });
                }
            } else {
                LOG.warn("Unknown message type [msg={}]", msg.getClass().getSimpleName());
            }
        }
    }
}
