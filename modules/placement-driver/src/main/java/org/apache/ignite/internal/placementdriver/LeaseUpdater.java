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

import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_LEASES_KEY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseBatch;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.leases.Leases;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverActorMessage;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessageGroup;
import org.apache.ignite.internal.placementdriver.message.StopLeaseProlongationMessage;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseNegotiator;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;

/**
 * A processor to manger leases. The process is started when placement driver activates and stopped when it deactivates.
 */
public class LeaseUpdater {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseUpdater.class);

    /** Update attempts interval in milliseconds. */
    private static final long UPDATE_LEASE_MS = 500L;

    /** Lease holding interval. */
    private static final long LEASE_INTERVAL = 10 * UPDATE_LEASE_MS;

    /** The lock is available when the actor is changing state. */
    private final IgniteSpinBusyLock stateChangingLock = new IgniteSpinBusyLock();

    private final AtomicBoolean active = new AtomicBoolean();

    /** The interval in milliseconds that is used in the beginning of lease granting process. */
    private final long longLeaseInterval;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Meta storage manager. */
    private final MetaStorageManager msManager;

    /** Assignments tracker. */
    private final AssignmentsTracker assignmentsTracker;

    /** Topology tracker. */
    private final TopologyTracker topologyTracker;

    /** Lease tracker. */
    private final LeaseTracker leaseTracker;

    /** Cluster clock. */
    private final HybridClock clock;

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
     * @param clock Cluster clock.
     */
    LeaseUpdater(
            String nodeName,
            ClusterService clusterService,
            MetaStorageManager msManager,
            LogicalTopologyService topologyService,
            LeaseTracker leaseTracker,
            HybridClock clock
    ) {
        this.nodeName = nodeName;
        this.clusterService = clusterService;
        this.msManager = msManager;
        this.leaseTracker = leaseTracker;
        this.clock = clock;

        this.longLeaseInterval = IgniteSystemProperties.getLong("IGNITE_LONG_LEASE", 120_000);
        this.assignmentsTracker = new AssignmentsTracker(msManager);
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
     * @return Future completes true when the lease will not prolong in the future, false otherwise.
     */
    private CompletableFuture<Boolean> denyLease(ReplicationGroupId grpId, Lease lease) {
        Lease deniedLease = lease.denyLease();

        leaseNegotiator.onLeaseRemoved(grpId);

        Leases leasesCurrent = leaseTracker.leasesCurrent();

        Collection<Lease> leases = leasesCurrent.leaseByGroupId().values();

        List<Lease> renewedLeases = new ArrayList<>();

        for (Lease ls : leases) {
            if (ls.replicationGroupId().equals(grpId)) {
                renewedLeases.add(deniedLease);
            } else {
                renewedLeases.add(ls);
            }
        }

        ByteArray key = PLACEMENTDRIVER_LEASES_KEY;

        return msManager.invoke(
                or(notExists(key), value(key).eq(leasesCurrent.leasesBytes())),
                put(key, new LeaseBatch(renewedLeases).bytes()),
                noop()
        );
    }

    /**
     * Finds a node that can be the leaseholder.
     *
     * @param assignments Replication group assignment.
     * @param proposedConsistentId Proposed consistent id, found out of a lease negotiation. The parameter might be {@code null}.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    private @Nullable ClusterNode nextLeaseHolder(Set<Assignment> assignments, @Nullable String proposedConsistentId) {
        //TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
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
            } else if (primaryCandidate == null || primaryCandidate.name().hashCode() > assignment.consistentId().hashCode()) {
                primaryCandidate = candidateNode;
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
        private final LeaseUpdateStatistics leaseUpdateStatistics = new LeaseUpdateStatistics();

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
            HybridTimestamp now = clock.now();

            leaseUpdateStatistics.onNewIteration();

            long outdatedLeaseThreshold = now.getPhysical() + LEASE_INTERVAL / 2;

            Leases leasesCurrent = leaseTracker.leasesCurrent();
            Map<ReplicationGroupId, Boolean> toBeNegotiated = new HashMap<>();
            Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>(leasesCurrent.leaseByGroupId());

            Map<ReplicationGroupId, Set<Assignment>> currentAssignments = assignmentsTracker.assignments();
            Set<ReplicationGroupId> currentAssignmentsReplicationGroupIds = currentAssignments.keySet();

            // Remove all expired leases that are no longer present in assignments.
            renewedLeases.entrySet().removeIf(e -> e.getValue().getExpirationTime().before(now)
                    && !currentAssignmentsReplicationGroupIds.contains(e.getKey()));

            int currentAssignmentsSize = currentAssignments.size();

            for (Map.Entry<ReplicationGroupId, Set<Assignment>> entry : currentAssignments.entrySet()) {
                ReplicationGroupId grpId = entry.getKey();

                Lease lease = leaseTracker.getLease(grpId);

                if (!lease.isAccepted()) {
                    LeaseAgreement agreement = leaseNegotiator.negotiated(grpId);

                    if (agreement.isAccepted()) {
                        publishLease(grpId, lease, renewedLeases);

                        continue;
                    } else if (agreement.ready()) {
                        // Here we initiate negotiations for UNDEFINED_AGREEMENT and retry them on newly started active actor as well.
                        ClusterNode candidate = nextLeaseHolder(entry.getValue(), agreement.getRedirectTo());

                        if (candidate == null) {
                            continue;
                        }

                        // New lease is granting.
                        writeNewLease(grpId, lease, candidate, renewedLeases, toBeNegotiated);

                        continue;
                    }
                }

                // The lease is expired or close to this.
                if (lease.getExpirationTime().getPhysical() < outdatedLeaseThreshold) {
                    ClusterNode candidate = nextLeaseHolder(
                            entry.getValue(),
                            lease.isProlongable() ? lease.getLeaseholder() : null
                    );

                    if (candidate == null) {
                        continue;
                    }

                    // We can't prolong the expired lease because we already have an interval of time when the lease was not active,
                    // so we must start a negotiation round from the beginning; the same we do for the groups that don't have
                    // leaseholders at all.
                    if (isLeaseOutdated(lease)) {
                        // New lease is granting.
                        writeNewLease(grpId, lease, candidate, renewedLeases, toBeNegotiated);
                    } else if (lease.isProlongable() && candidate.id().equals(lease.getLeaseholderId())) {
                        // Old lease is renewing.
                        prolongLease(grpId, lease, renewedLeases);
                    }
                }
            }

            byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

            ByteArray key = PLACEMENTDRIVER_LEASES_KEY;

            boolean shouldLogLeaseStatistics = leaseUpdateStatistics.shouldLogLeaseStatistics();
            LeaseUpdateStatisticsParameters leasesUpdatedInCurrentIteration = leaseUpdateStatistics.leasesUpdatedInCurrentIteration();

            msManager.invoke(
                    or(notExists(key), value(key).eq(leasesCurrent.leasesBytes())),
                    put(key, renewedValue),
                    noop()
            ).whenComplete((success, e) -> {
                if (e != null) {
                    LOG.error("Lease update invocation failed", e);

                    return;
                }

                if (!success) {
                    LOG.debug("Lease update invocation failed");

                    return;
                }

                LeaseUpdateStatisticsParameters totalLeases = leaseUpdateStatistics.onSuccessfulIteration(leasesUpdatedInCurrentIteration);

                if (shouldLogLeaseStatistics) {
                    LOG.info("Leases updated (printed once per " + LeaseUpdateStatistics.PRINT_ONCE_PER_ITERATIONS + " iterations): ["
                            + "currentIteration=" + leasesUpdatedInCurrentIteration
                            + ", total=" + totalLeases
                            + ", currentAssignmentsSize=" + currentAssignmentsSize + "]");
                }

                for (Map.Entry<ReplicationGroupId, Boolean> entry : toBeNegotiated.entrySet()) {
                    Lease lease = renewedLeases.get(entry.getKey());
                    boolean force = entry.getValue();

                    leaseNegotiator.negotiate(lease, force);
                }
            });
        }

        /**
         * Writes a new lease.
         *
         * @param grpId Replication group id.
         * @param lease Old lease to apply CAS in Meta storage.
         * @param candidate Lease candidate.
         * @param renewedLeases Leases to renew.
         * @param toBeNegotiated Leases that are required to be negotiated.
         */
        private void writeNewLease(ReplicationGroupId grpId, Lease lease, ClusterNode candidate,
                Map<ReplicationGroupId, Lease> renewedLeases, Map<ReplicationGroupId, Boolean> toBeNegotiated) {
            HybridTimestamp startTs = clock.now();

            var expirationTs = new HybridTimestamp(startTs.getPhysical() + longLeaseInterval, 0);

            Lease renewedLease = new Lease(candidate.name(), candidate.id(), startTs, expirationTs, grpId);

            renewedLeases.put(grpId, renewedLease);

            toBeNegotiated.put(grpId, !lease.isAccepted() && Objects.equals(lease.getLeaseholder(), candidate.name()));

            leaseUpdateStatistics.onLeaseCreate();
        }

        /**
         * Prolongs the lease.
         *
         * @param grpId Replication group id.
         * @param lease Lease to prolong.
         */
        private void prolongLease(ReplicationGroupId grpId, Lease lease, Map<ReplicationGroupId, Lease> renewedLeases) {
            var newTs = new HybridTimestamp(clock.now().getPhysical() + LEASE_INTERVAL, 0);

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
        private void publishLease(ReplicationGroupId grpId, Lease lease, Map<ReplicationGroupId, Lease> renewedLeases) {
            var newTs = new HybridTimestamp(clock.now().getPhysical() + LEASE_INTERVAL, 0);

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
            HybridTimestamp now = clock.now();

            return now.after(lease.getExpirationTime());
        }
    }

    private static class LeaseUpdateStatistics {
        static final int PRINT_ONCE_PER_ITERATIONS = 10;

        /** This field should be accessed only from updater thread. */
        private int statisticsLogCounter;

        /** This field is iteration-local and should be accessed only from updater thread. */
        private LeaseUpdateStatisticsParameters iteration = new LeaseUpdateStatisticsParameters(0, 0, 0);

        private final LeaseUpdateStatisticsParameters total = new LeaseUpdateStatisticsParameters(0, 0, 0);

        /**
         * Should be called only from the updater thread.
         */
        private void onLeaseCreate() {
            iteration.leasesCreated++;
        }

        /**
         * Should be called only from the updater thread.
         */
        private void onLeasePublish() {
            iteration.leasesPublished++;
        }

        /**
         * Should be called only from the updater thread.
         */
        private void onLeaseProlong() {
            iteration.leasesProlonged++;
        }

        /**
         * Should be called only from the updater thread.
         */
        private LeaseUpdateStatisticsParameters leasesUpdatedInCurrentIteration() {
            return new LeaseUpdateStatisticsParameters(iteration.leasesCreated, iteration.leasesPublished, iteration.leasesProlonged);
        }

        /**
         * Should be called only from the updater thread.
         */
        private boolean shouldLogLeaseStatistics() {
            boolean result = ++statisticsLogCounter > PRINT_ONCE_PER_ITERATIONS;

            if (result) {
                statisticsLogCounter = 0;
            }

            return result;
        }

        /**
         * Should be called only from the updater thread.
         */
        private void onNewIteration() {
            iteration = new LeaseUpdateStatisticsParameters(0, 0, 0);
        }

        /**
         * Updates the total count of updated leases after the successful update.
         *
         * @param iterationParameters Leases updated in current iteration.
         * @return Updated value of total updated leases.
         */
        private synchronized LeaseUpdateStatisticsParameters onSuccessfulIteration(LeaseUpdateStatisticsParameters iterationParameters) {
            total.leasesCreated += iterationParameters.leasesCreated;
            total.leasesPublished += iterationParameters.leasesPublished;
            total.leasesProlonged += iterationParameters.leasesProlonged;

            return total;
        }
    }

    private static class LeaseUpdateStatisticsParameters {
        long leasesCreated;
        long leasesPublished;
        long leasesProlonged;

        public LeaseUpdateStatisticsParameters(long leasesCreated, long leasesPublished, long leasesProlonged) {
            this.leasesCreated = leasesCreated;
            this.leasesPublished = leasesPublished;
            this.leasesProlonged = leasesProlonged;
        }

        @Override
        public String toString() {
            return "[leasesCreated=" + leasesCreated
                    + ", leasesPublished=" + leasesPublished
                    + ", leasesProlonged=" + leasesProlonged + ']';
        }
    }

    /** Message handler to process notification from replica side. */
    private class PlacementDriverActorMessageHandler implements NetworkMessageHandler {
        @Override
        public void onReceived(NetworkMessage msg0, String sender, @Nullable Long correlationId) {
            if (!(msg0 instanceof PlacementDriverActorMessage)) {
                return;
            }

            var msg = (PlacementDriverActorMessage) msg0;

            if (!active() || !stateChangingLock.enterBusy()) {
                return;
            }

            try {
                processMessageInternal(sender, msg);
            } finally {
                stateChangingLock.leaveBusy();
            }
        }

        /**
         * Processes an placement driver actor message. The method should be invoked under state lock.
         *
         * @param sender Sender node name.
         * @param msg Message.
         */
        private void processMessageInternal(String sender, PlacementDriverActorMessage msg) {
            ReplicationGroupId grpId = msg.groupId();

            Lease lease = leaseTracker.getLease(grpId);

            if (msg instanceof StopLeaseProlongationMessage) {
                if (lease.isProlongable() && sender.equals(lease.getLeaseholder())) {
                    denyLease(grpId, lease).whenComplete((res, th) -> {
                        if (th != null) {
                            LOG.warn("Prolongation denial failed due to exception [groupId={}]", th, grpId);
                        } else {
                            LOG.info("Stop lease prolongation message was handled [groupId={}, sender={}, deny={}]", grpId, sender, res);
                        }
                    });
                }
            } else {
                LOG.warn("Unknown message type [msg={}]", msg.getClass().getSimpleName());
            }
        }
    }
}
