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
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement;
import org.apache.ignite.internal.placementdriver.negotiation.LeaseNegotiator;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.Nullable;

/**
 * A processor to manger leases. The process is started when placement driver activates and stopped when it deactivates.
 */
public class LeaseUpdater {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseUpdater.class);

    /** Update attempts interval in milliseconds. */
    private static final long UPDATE_LEASE_MS = 200L;

    /** Lease holding interval. */
    public static final long LEASE_INTERVAL = 10 * UPDATE_LEASE_MS;

    /** Long lease interval. The interval is used between lease granting attempts. */
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
    private volatile Thread updaterTread;

    /** Processor to communicate with the leaseholder to negotiate the lease. */
    private LeaseNegotiator leaseNegotiator;

    /** Node name. */
    private String nodeName;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param vaultManager Vault manager.
     * @param msManager Meta storage manager.
     * @param topologyService Topology service.
     * @param tablesConfiguration Tables configuration.
     * @param leaseTracker Lease tracker.
     * @param clock Cluster clock.
     */
    public LeaseUpdater(
            ClusterService clusterService,
            VaultManager vaultManager,
            MetaStorageManager msManager,
            LogicalTopologyService topologyService,
            TablesConfiguration tablesConfiguration,
            DistributionZonesConfiguration distributionZonesConfiguration,
            LeaseTracker leaseTracker,
            HybridClock clock
    ) {
        this.clusterService = clusterService;
        this.msManager = msManager;
        this.leaseTracker = leaseTracker;
        this.clock = clock;

        this.longLeaseInterval = IgniteSystemProperties.getLong("IGNITE_LONG_LEASE", 120_000L);
        this.assignmentsTracker = new AssignmentsTracker(vaultManager, msManager, tablesConfiguration, distributionZonesConfiguration);
        this.topologyTracker = new TopologyTracker(topologyService);
        this.updater = new Updater();
    }

    /**
     * Initializes the class.
     */
    public void init(String nodeName) {
        this.nodeName = nodeName;

        topologyTracker.startTrack();
        assignmentsTracker.startTrack();
    }

    /**
     * De-initializes the class.
     */
    public void deInit() {
        topologyTracker.stopTrack();
        assignmentsTracker.stopTrack();
    }

    /**
     * Activates a lease updater to renew leases.
     */
    public void activate() {
        leaseNegotiator = new LeaseNegotiator(clusterService);

        //TODO: IGNITE-18879 Implement lease maintenance.
        updaterTread = new Thread(updater, NamedThreadFactory.threadPrefix(nodeName, "lease-updater"));

        updaterTread.start();
    }

    /**
     * Stops a dedicated thread to renew or assign leases.
     */
    public void deactivate() {
        //TODO: IGNITE-18879 Implement lease maintenance.
        if (updaterTread != null) {
            updaterTread.interrupt();

            updaterTread = null;
        }

        leaseNegotiator = null;
    }

    /**
     * Finds a node that can be the leaseholder.
     *
     * @param assignments Replication group assignment.
     * @param proposedConsistentId Proposed consistent id, found out of a lease negotiation. The parameter might be {@code null}.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    private ClusterNode nextLeaseHolder(Set<Assignment> assignments, @Nullable String proposedConsistentId) {
        //TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        String consistentId = null;

        for (Assignment assignment : assignments) {
            if (assignment.consistentId().equals(proposedConsistentId)) {
                consistentId = proposedConsistentId;

                break;
            } else if (consistentId == null || consistentId.hashCode() > assignment.consistentId().hashCode()) {
                consistentId = assignment.consistentId();
            }
        }

        if (consistentId != null) {
            ClusterNode candidate = topologyTracker.nodeByConsistentId(consistentId);

            if (candidate != null) {
                return candidate;
            }
        }

        return null;
    }

    /**
     * Runnable to update lease in Meta storage.
     */
    private class Updater implements Runnable {
        @Override
        public void run() {
            while (updaterTread != null && !updaterTread.isInterrupted()) {
                long outdatedLeaseThreshold = clock.now().getPhysical() + LEASE_INTERVAL / 2;

                for (Map.Entry<ReplicationGroupId, Set<Assignment>> entry : assignmentsTracker.assignments().entrySet()) {
                    ReplicationGroupId grpId = entry.getKey();

                    Lease lease = leaseTracker.getLease(grpId);

                    if (!lease.isAccepted()) {
                        LeaseAgreement agreement = leaseNegotiator.negotiated(grpId);

                        if (agreement.isAccepted()) {
                            publishLease(grpId, lease);

                            continue;
                        } else if (agreement.ready()) {
                            ClusterNode candidate = nextLeaseHolder(entry.getValue(), agreement.getRedirectTo());

                            if (candidate == null) {
                                continue;
                            }

                            // New lease is granting.
                            writeNewLeaseInMetaStorage(grpId, lease, candidate);
                            continue;
                        }
                    }

                    // The lease is expired or close to this.
                    if (lease.getExpirationTime().getPhysical() < outdatedLeaseThreshold) {
                        ClusterNode candidate = nextLeaseHolder(
                                entry.getValue(),
                                lease.isAccepted() ? lease.getLeaseholder().name() : null
                        );

                        if (candidate == null) {
                            continue;
                        }

                        // We can't prolong the expired lease because we already have an interval of time when the lease was not active,
                        // so we must start a negotiation round from the beginning; the same we do for the groups that don't have
                        // leaseholders at all.
                        if (isLeaseOutdated(lease)) {
                            // New lease is granting.
                            writeNewLeaseInMetaStorage(grpId, lease, candidate);
                        } else if (lease.isAccepted() && candidate.equals(lease.getLeaseholder())) {
                            // Old lease is renewing.
                            prolongLeaseInMetaStorage(grpId, lease);
                        }
                    }
                }

                try {
                    Thread.sleep(UPDATE_LEASE_MS);
                } catch (InterruptedException e) {
                    LOG.warn("Lease updater is interrupted");
                }
            }
        }

        /**
         * Writes a new lease in Meta storage.
         *
         * @param grpId Replication group id.
         * @param lease Old lease to apply CAS in Meta storage.
         * @param candidate Lease candidate.
         */
        private void writeNewLeaseInMetaStorage(ReplicationGroupId grpId, Lease lease, ClusterNode candidate) {
            var leaseKey = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + grpId);

            HybridTimestamp startTs = clock.now();

            var expirationTs = new HybridTimestamp(startTs.getPhysical() + longLeaseInterval, 0);

            byte[] leaseRaw = ByteUtils.toBytes(lease);

            Lease renewedLease = new Lease(candidate, startTs, expirationTs);

            msManager.invoke(
                    or(notExists(leaseKey), value(leaseKey).eq(leaseRaw)),
                    put(leaseKey, ByteUtils.toBytes(renewedLease)),
                    noop()
            ).thenAccept(isCreated -> {
                if (isCreated) {
                    boolean force = candidate.equals(lease.getLeaseholder());

                    leaseNegotiator.negotiate(grpId, renewedLease, force);
                }
            });
        }

        /**
         * Writes a prolong lease in Meta storage.
         *
         * @param grpId Replication group id.
         * @param lease Lease to prolong.
         */
        private void prolongLeaseInMetaStorage(ReplicationGroupId grpId, Lease lease) {
            var leaseKey = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + grpId);
            var newTs = new HybridTimestamp(clock.now().getPhysical() + LEASE_INTERVAL, 0);

            byte[] leaseRaw = ByteUtils.toBytes(lease);

            Lease renewedLease = lease.prolongLease(newTs);

            msManager.invoke(
                    value(leaseKey).eq(leaseRaw),
                    put(leaseKey, ByteUtils.toBytes(renewedLease)),
                    noop()
            );
        }

        /**
         * Writes an accepted lease in Meta storage. After the lease will be written to Meta storage,
         * the lease becomes available to all components.
         *
         * @param grpId Replication group id.
         * @param lease Lease to accept.
         */
        private void publishLease(ReplicationGroupId grpId, Lease lease) {
            var leaseKey = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + grpId);
            var newTs = new HybridTimestamp(clock.now().getPhysical() + LEASE_INTERVAL, 0);

            byte[] leaseRaw = ByteUtils.toBytes(lease);

            Lease renewedLease = lease.acceptLease(newTs);

            msManager.invoke(
                    value(leaseKey).eq(leaseRaw),
                    put(leaseKey, ByteUtils.toBytes(renewedLease)),
                    noop()
            );
        }

        /**
         * Checks that the lease is outdated.
         * {@link Lease#EMPTY_LEASE} is always outdated.
         *
         * @param lease Lease.
         * @return True when the candidate can be a leaseholder, otherwise false.
         */
        private boolean isLeaseOutdated(Lease lease) {
            HybridTimestamp now = clock.now();

            return now.after(lease.getExpirationTime());
        }
    }
}
