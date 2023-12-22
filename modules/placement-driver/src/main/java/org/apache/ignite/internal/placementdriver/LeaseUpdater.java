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
import static org.apache.ignite.internal.placementdriver.org.apache.ignite.internal.placementdriver.event.AssignmentsTrackerEvent.ASSIGNMENTS_CHANGED;
import static org.apache.ignite.internal.placementdriver.org.apache.ignite.internal.placementdriver.event.LeaseNegotiatorEvent.LEASE_ACCEPTED;
import static org.apache.ignite.internal.placementdriver.org.apache.ignite.internal.placementdriver.event.TopologyTrackerEvent.TOPOLOGY_CHANGED;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.thread.NamedThreadFactory;
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
    private static final long UPDATE_LEASE_MS = 2500L;

    /** Lease holding interval. */
    private static final long LEASE_INTERVAL = 2 * UPDATE_LEASE_MS;

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

    /** Dedicated single-thread executor to update leases. */
    private volatile ScheduledExecutorService updaterExecutor;

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

            updaterExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(nodeName + "-lease-updater", LOG));

            updaterExecutor.scheduleAtFixedRate(updater, 0, UPDATE_LEASE_MS, TimeUnit.MILLISECONDS);

            // Listeners are just an optimisation in order to detect lease renewal triggers faster. If it'll be missed for some reason,
            // nothing will break, because common scheduler will catch up given triggers. This is also the reason
            // why instead of adding/removing listener on activation/deactivation every node ones activated will get the notification
            // and check whether it's active or not. Given approach is simpler and thus less error prone.
            assignmentsTracker.listen(ASSIGNMENTS_CHANGED, ((parameters, exception) -> onRenewLeasesTrigger()));
            topologyTracker.listen(TOPOLOGY_CHANGED, ((parameters, exception) -> onRenewLeasesTrigger()));
            leaseNegotiator.listen(LEASE_ACCEPTED, ((parameters, exception) -> onRenewLeasesTrigger()));

        } finally {
            stateChangingLock.unblock();
        }
    }

    private CompletableFuture<Boolean> onRenewLeasesTrigger() {
        stateChangingLock.enterBusy();

        try {
            if (active()) {
                updaterExecutor.submit(updater);
            }
        } finally {
            stateChangingLock.leaveBusy();
        }

        return falseCompletedFuture();
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

            // We do shutdownNow() right away without doing usual shutdown()+awaitTermination() because
            // this would make us wait till all the scheduled tasks get executed (which might take a lot
            // of time).
            updaterExecutor.shutdownNow();

            this.updaterExecutor = null;

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
        @Override
        public void run() {
            stateChangingLock.enterBusy();

            try {
                if (active()) {
                    updateLeaseBatchInternal();
                }
            } finally {
                stateChangingLock.leaveBusy();
            }
        }

        /** Updates leases in Meta storage. This method is supposed to be used in the busy lock. */
        private void updateLeaseBatchInternal() {
            HybridTimestamp now = clock.now();

            Leases leasesCurrent = leaseTracker.leasesCurrent();
            Map<ReplicationGroupId, Boolean> toBeNegotiated = new HashMap<>();
            Map<ReplicationGroupId, Lease> renewedLeases = new HashMap<>(leasesCurrent.leaseByGroupId());

            Map<ReplicationGroupId, Set<Assignment>> currentAssignments = assignmentsTracker.assignments();
            Set<ReplicationGroupId> currentAssignmentsReplicationGroupIds = currentAssignments.keySet();

            // Remove all expired leases that are no longer present in assignments.
            renewedLeases.entrySet().removeIf(e -> e.getValue().getExpirationTime().before(now)
                    && !currentAssignmentsReplicationGroupIds.contains(e.getKey()));

            for (Map.Entry<ReplicationGroupId, Set<Assignment>> entry : currentAssignments.entrySet()) {
                ReplicationGroupId grpId = entry.getKey();

                Lease lease = leaseTracker.getLease(grpId);

                if (!lease.isAccepted()) {
                    LeaseAgreement agreement = leaseNegotiator.negotiated(grpId);

                    if (agreement.isAccepted()) {
                        publishLease(grpId, lease, renewedLeases);

                        continue;
                    } else if (agreement.ready()) {
                        ClusterNode candidate = nextLeaseHolder(entry.getValue(), agreement.getRedirectTo());

                        if (candidate == null) {
                            continue;
                        }

                        // New lease is granting.
                        writeNewLease(grpId, lease, candidate, renewedLeases, toBeNegotiated);

                        continue;
                    }
                }

                // Prolong existing leases.
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

            byte[] renewedValue = new LeaseBatch(renewedLeases.values()).bytes();

            var key = PLACEMENTDRIVER_LEASES_KEY;

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
