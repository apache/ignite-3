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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;
import static org.apache.ignite.internal.placementdriver.leases.Lease.emptyLease;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.IgniteTuple3;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.ClusterService;
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
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * A processor to manger leases. The process is started when placement driver activates and stopped when it deactivates.
 */
public class LeaseUpdater {
    /** Negative value means that printing statistics is disabled. */
    private static final int LEASE_UPDATE_STATISTICS_PRINT_ONCE_PER_ITERATIONS = IgniteSystemProperties
            .getInteger("LEASE_STATISTICS_PRINT_ONCE_PER_ITERATIONS", 10);

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
     */
    LeaseUpdater(
            String nodeName,
            ClusterService clusterService,
            MetaStorageManager msManager,
            LogicalTopologyService topologyService,
            LeaseTracker leaseTracker,
            ClockService clockService,
            AssignmentsTracker assignmentsTracker,
            ReplicationConfiguration replicationConfiguration
    ) {
        this.nodeName = nodeName;
        this.clusterService = clusterService;
        this.msManager = msManager;
        this.leaseTracker = leaseTracker;
        this.clockService = clockService;
        this.replicationConfiguration = replicationConfiguration;

        this.assignmentsTracker = assignmentsTracker;
        this.topologyTracker = new TopologyTracker(topologyService);
        this.updater = new Updater();

        clusterService.messagingService().addMessageHandler(PlacementDriverMessageGroup.class, new PlacementDriverActorMessageHandler());
    }

    /** Initializes the class. */
    public void init() {
        topologyTracker.startTrack();
        assignmentsTracker.startTrack();
    }

    /** De-initializes the class. */
    void deInit() {
        topologyTracker.stopTrack();
        assignmentsTracker.stopTrack();
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

            leaseNegotiator = new LeaseNegotiator(clusterService);

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
     * Finds a node that can be the leaseholder.
     *
     * @param assignments Replication group assignment.
     * @param grpId Group id.
     * @param proposedConsistentId Proposed consistent id, found out of a lease negotiation. The parameter might be {@code null}.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    private @Nullable ClusterNode nextLeaseHolder(
            Set<Assignment> assignments,
            ReplicationGroupId grpId,
            @Nullable String proposedConsistentId
    ) {
        // TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        ClusterNode primaryCandidate = null;

        for (Assignment assignment : assignments) {
            // Check whether given assignments is actually available in logical topology. It's a best effort check because it's possible
            // for proposed primary candidate to leave the topology at any time. In that case primary candidate will be recalculated.
            ClusterNode candidateNode = topologyTracker.nodeByConsistentId(assignment.consistentId());

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
        private LeaseStats leaseUpdateStatistics = new LeaseStats();

        /** This field should be accessed only from updater thread. */
        private int statisticsLogCounter;

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
                    LOG.error("Error occurred when updating the leases.", e);

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
                    LOG.warn("Lease updater is interrupted");
                }
            }
        }

        /** Updates leases in Meta storage. This method is supposed to be used in the busy lock. */
        private void updateLeaseBatchInternal() {
            HybridTimestamp now = clockService.now();

            leaseUpdateStatistics = new LeaseStats();

            long leaseExpirationInterval = replicationConfiguration.leaseExpirationInterval().value();

            long outdatedLeaseThreshold = now.getPhysical() + leaseExpirationInterval / 2;

            Leases leasesCurrent = leaseTracker.leasesCurrent();
            Map<ReplicationGroupId, Boolean> toBeNegotiated = new HashMap<>();
            Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>(leasesCurrent.leaseByGroupId());

            Map<ReplicationGroupId, TokenizedAssignments> currentAssignments = assignmentsTracker.assignments();
            Set<ReplicationGroupId> currentAssignmentsReplicationGroupIds = currentAssignments.keySet();

            // Remove all expired leases that are no longer present in assignments.
            renewedLeases.entrySet().removeIf(e -> clockService.before(e.getValue().getExpirationTime(), now)
                    && !currentAssignmentsReplicationGroupIds.contains(e.getKey()));

            int currentAssignmentsSize = currentAssignments.size();
            int activeLeasesCount = 0;

            for (Map.Entry<ReplicationGroupId, TokenizedAssignments> entry : currentAssignments.entrySet()) {
                ReplicationGroupId grpId = entry.getKey();
                Set<Assignment> assignments = entry.getValue().nodes();

                Lease lease = requireNonNullElse(leasesCurrent.leaseByGroupId().get(grpId), emptyLease(grpId));

                if (lease.isAccepted() && !isLeaseOutdated(lease)) {
                    activeLeasesCount++;
                }

                if (!lease.isAccepted()) {
                    LeaseAgreement agreement = leaseNegotiator.getAndRemoveIfReady(grpId);

                    agreement.checkValid(grpId, topologyTracker.currentTopologySnapshot(), assignments);

                    if (lease.isProlongable() && agreement.isAccepted()) {
                        Lease negotiatedLease = agreement.getLease();

                        // Lease information is taken from lease tracker, where it appears on meta storage watch updates, so it can
                        // contain stale leases, if watch processing was delayed for some reason. It is ok: negotiated lease is
                        // guaranteed to be already written to meta storage before negotiation begins, and in this case its start time
                        // would be greater than lease's.
                        assert negotiatedLease.getStartTime().longValue() >= lease.getStartTime().longValue()
                                : format("Can't publish the lease that was not negotiated [groupId={}, startTime={}, "
                                + "agreementLeaseStartTime={}].", grpId, lease.getStartTime(), agreement.getLease().getStartTime());

                        publishLease(grpId, negotiatedLease, renewedLeases, leaseExpirationInterval);

                        continue;
                    } else if (!lease.isProlongable() || agreement.isDeclined()) {
                        // Here we initiate negotiations for UNDEFINED_AGREEMENT and retry them on newly started active actor as well.
                        // Also, if the lease was denied, we create the new one.
                        chooseCandidateAndCreateNewLease(grpId, lease, agreement, assignments, renewedLeases, toBeNegotiated);

                        continue;
                    }
                }

                // The lease is expired or close to this.
                if (lease.getExpirationTime().getPhysical() < outdatedLeaseThreshold) {
                    String proposedLeaseholder = lease.isProlongable()
                            ? lease.getLeaseholder()
                            : lease.proposedCandidate();

                    ClusterNode candidate = nextLeaseHolder(assignments, grpId, proposedLeaseholder);

                    if (candidate == null) {
                        leaseUpdateStatistics.onLeaseWithoutCandidate();

                        continue;
                    }

                    // We can't prolong the expired lease because we already have an interval of time when the lease was not active,
                    // so we must start a negotiation round from the beginning; the same we do for the groups that don't have
                    // leaseholders at all.
                    if (isLeaseOutdated(lease)) {
                        // New lease is granted.
                        writeNewLease(grpId, candidate, renewedLeases);

                        boolean force = !lease.isProlongable() && lease.proposedCandidate() != null;

                        toBeNegotiated.put(grpId, force);
                    } else if (lease.isProlongable() && candidate.id().equals(lease.getLeaseholderId())) {
                        // Old lease is renewed.
                        prolongLease(grpId, lease, renewedLeases, leaseExpirationInterval);
                    }
                }
            }

            byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

            ByteArray key = PLACEMENTDRIVER_LEASES_KEY;

            if (shouldLogLeaseStatistics()) {
                LOG.info(
                        "Leases updated (printed once per {} iteration(s)): [inCurrentIteration={}, active={}, "
                                + "currentAssignmentsSize={}].",
                        LEASE_UPDATE_STATISTICS_PRINT_ONCE_PER_ITERATIONS,
                        leaseUpdateStatistics,
                        activeLeasesCount,
                        currentAssignmentsSize
                );
            }

            if (Arrays.equals(leasesCurrent.leasesBytes(), renewedValue)) {
                LOG.debug("No leases to update found.");
                return;
            }

            msManager.invoke(
                    or(notExists(key), value(key).eq(leasesCurrent.leasesBytes())),
                    put(key, renewedValue),
                    noop()
            ).whenComplete((success, e) -> {
                if (e != null) {
                    LOG.error("Lease update invocation failed", e);

                    cancelAgreements(toBeNegotiated.keySet());

                    return;
                }

                if (!success) {
                    LOG.warn("Lease update invocation failed because of concurrent update.");

                    cancelAgreements(toBeNegotiated.keySet());

                    return;
                }

                for (Map.Entry<ReplicationGroupId, Boolean> entry : toBeNegotiated.entrySet()) {
                    Lease lease = renewedLeases.get(entry.getKey());
                    boolean force = entry.getValue();

                    leaseNegotiator.negotiate(lease, force);
                }
            });
        }

        private void chooseCandidateAndCreateNewLease(
                ReplicationGroupId grpId,
                Lease lease,
                LeaseAgreement agreement,
                Set<Assignment> assignments,
                Map<ReplicationGroupId, Lease> renewedLeases,
                Map<ReplicationGroupId, Boolean> toBeNegotiated
        ) {
            String proposedCandidate = null;

            if (agreement.isDeclined()) {
                proposedCandidate = agreement.getRedirectTo();
            }

            if (proposedCandidate == null) {
                proposedCandidate = lease.isProlongable() ? lease.getLeaseholder() : lease.proposedCandidate();
            }

            ClusterNode candidate = nextLeaseHolder(assignments, grpId, proposedCandidate);

            if (candidate == null) {
                leaseUpdateStatistics.onLeaseWithoutCandidate();

                return;
            }

            // New lease is granted.
            writeNewLease(grpId, candidate, renewedLeases);

            // TODO https://issues.apache.org/jira/browse/IGNITE-23213 Depending on the solution we may refactor this.
            boolean force = Objects.equals(lease.getLeaseholder(), candidate.name()) && !agreement.isCancelled();
            toBeNegotiated.put(grpId, force);
        }

        /**
         * Cancel all the given agreements. This should be done if the new leases that were to be negotiated had been not written to meta
         * storage.
         *
         * @param groupIds Group ids.
         */
        private void cancelAgreements(Collection<ReplicationGroupId> groupIds) {
            for (ReplicationGroupId groupId : groupIds) {
                leaseNegotiator.cancelAgreement(groupId);
            }
        }

        /**
         * Writes a new lease.
         *
         * @param grpId Replication group id.
         * @param candidate Lease candidate.
         * @param renewedLeases Leases to renew.
         */
        private void writeNewLease(
                ReplicationGroupId grpId,
                ClusterNode candidate,
                Map<ReplicationGroupId, Lease> renewedLeases
        ) {
            HybridTimestamp startTs = clockService.now();

            long interval = replicationConfiguration.leaseAgreementAcceptanceTimeLimit().value();

            var expirationTs = new HybridTimestamp(startTs.getPhysical() + interval, 0);

            Lease renewedLease = new Lease(candidate.name(), candidate.id(), startTs, expirationTs, grpId);

            renewedLeases.put(grpId, renewedLease);

            // Lease agreement should be created synchronously before negotiation begins.
            leaseNegotiator.createAgreement(grpId, renewedLease);

            leaseUpdateStatistics.onLeaseCreate();
        }

        /**
         * Prolongs the lease.
         *
         * @param grpId Replication group id.
         * @param lease Lease to prolong.
         */
        private void prolongLease(
                ReplicationGroupId grpId,
                Lease lease,
                Map<ReplicationGroupId, Lease> renewedLeases,
                long leaseExpirationInterval
        ) {
            var newTs = new HybridTimestamp(clockService.now().getPhysical() + leaseExpirationInterval, 0);

            Lease renewedLease = lease.prolongLease(newTs);

            renewedLeases.put(grpId, renewedLease);

            leaseUpdateStatistics.onLeaseProlong();
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

            leaseUpdateStatistics.onLeasePublish();
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

        private boolean shouldLogLeaseStatistics() {
            if (LEASE_UPDATE_STATISTICS_PRINT_ONCE_PER_ITERATIONS < 0) {
                return false;
            }

            boolean result = ++statisticsLogCounter > LEASE_UPDATE_STATISTICS_PRINT_ONCE_PER_ITERATIONS;

            if (result) {
                statisticsLogCounter = 0;
            }

            return result;
        }
    }

    private static class LeaseStats {
        @IgniteToStringInclude
        int leasesCreated;

        @IgniteToStringInclude
        int leasesPublished;

        @IgniteToStringInclude
        int leasesProlonged;

        @IgniteToStringInclude
        int leasesWithoutCandidates;

        private void onLeaseCreate() {
            leasesCreated++;
        }

        private void onLeasePublish() {
            leasesPublished++;
        }

        private void onLeaseProlong() {
            leasesProlonged++;
        }

        private void onLeaseWithoutCandidate() {
            leasesWithoutCandidates++;
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    /** Message handler to process notification from replica side. */
    private class PlacementDriverActorMessageHandler implements NetworkMessageHandler {
        @Override
        public void onReceived(NetworkMessage msg0, ClusterNode sender, @Nullable Long correlationId) {
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
