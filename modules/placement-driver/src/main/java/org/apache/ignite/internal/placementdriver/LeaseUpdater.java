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
import static org.apache.ignite.internal.placementdriver.Lease.EMPTY_LEASE;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.PLACEMENTDRIVER_PREFIX;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;

/**
 * A processor to manger leases. The process is started when placement driver activates and stopped when it deactivates.
 */
public class LeaseUpdater {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseUpdater.class);

    /**
     * Cluster cLock skew. The constant determines the undefined inclusive interval to compares timestamp from various nodes.
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     */
    private static final long CLOCK_SKEW = 7L;

    /** Update attempts interval in milliseconds. */
    private static final long UPDATE_LEASE_MS = 200L;

    /** Lease holding interval. */
    private static final long LEASE_PERIOD = 10 * UPDATE_LEASE_MS;

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

    /** Node name. */
    private String nodeName;

    /**
     * The constructor.
     *
     * @param vaultManager Vault manager.
     * @param msManager Metastorage manager.
     * @param topologyService Topology service.
     * @param tablesConfiguration Tables configuration.
     * @param leaseTracker Lease tracker.
     * @param clock Cluster clock.
     */
    public LeaseUpdater(
            VaultManager vaultManager,
            MetaStorageManager msManager,
            LogicalTopologyService topologyService,
            TablesConfiguration tablesConfiguration,
            LeaseTracker leaseTracker,
            HybridClock clock
    ) {
        this.msManager = msManager;
        this.leaseTracker = leaseTracker;
        this.clock = clock;

        this.assignmentsTracker = new AssignmentsTracker(vaultManager, msManager, tablesConfiguration);
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
    }

    /**
     * Compares two timestamps with the clock skew.
     * t1, t2 comparable if t1 is not contained on [t2 - CLOCK_SKEW; t2 + CLOCK_SKEW].
     * TODO: IGNITE-18978 Method to comparison timestamps with clock skew.
     *
     * @param ts1 First timestamp.
     * @param ts2 Second timestamp.
     * @return Result of comparison can be positive or negative, or {@code 0} if timestamps are not comparable.
     */
    private static int compareWithClockSkew(HybridTimestamp ts1, HybridTimestamp ts2) {
        if (ts1.getPhysical() - CLOCK_SKEW <= ts2.getPhysical() && ts1.getPhysical() + CLOCK_SKEW >= ts2.getPhysical()) {
            return 0;
        }

        return ts1.compareTo(ts2);
    }

    /**
     * Finds a node that can be the leaseholder.
     *
     * @param assignments Replication group assignment.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    private ClusterNode nextLeaseHolder(Set<Assignment> assignments) {
        //TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        for (Assignment assignment : assignments) {
            ClusterNode candidate = topologyTracker.nodeByConsistentId(assignment.consistentId());

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
                for (Map.Entry<ReplicationGroupId, Set<Assignment>> entry : assignmentsTracker.assignments().entrySet()) {
                    ReplicationGroupId grpId = entry.getKey();

                    Lease lease = leaseTracker.getLease(grpId);

                    HybridTimestamp now = clock.now();

                    // Nothing holds the lease.
                    if (lease == EMPTY_LEASE
                            // The lease is near to expiration.
                            || now.getPhysical() > (lease.getLeaseExpirationTime().getPhysical() - LEASE_PERIOD / 2)) {
                        ClusterNode candidate = nextLeaseHolder(entry.getValue());

                        if (candidate == null) {
                            continue;
                        }

                        if (isReplicationGroupUpdateLeaseholder(lease, candidate)) {
                            updateLeaseInMetaStorage(grpId, lease, candidate);
                            // New lease is granting.
                        } else if (candidate.equals(lease.getLeaseholder())) {
                            updateLeaseInMetaStorage(grpId, lease, candidate);
                            // Old lease is renewing.
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
         * Writes a lease in Meta storage.
         *
         * @param grpId Replication group id.
         * @param lease Old lease to apply CAS in Meta storage.
         * @param candidate Lease candidate.
         */
        private void updateLeaseInMetaStorage(ReplicationGroupId grpId, Lease lease, ClusterNode candidate) {
            var leaseKey = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + grpId);
            var newTs = new HybridTimestamp(clock.now().getPhysical() + LEASE_PERIOD, 0);

            byte[] leaseRaw = ByteUtils.toBytes(lease);

            Lease renewedLease = new Lease(candidate, newTs);

            msManager.invoke(
                    or(notExists(leaseKey), value(leaseKey).eq(leaseRaw)),
                    put(leaseKey, ByteUtils.toBytes(renewedLease)),
                    noop()
            );
        }

        /**
         * Checks that a leaseholder candidate can take a lease on the replication group.
         *
         * @param lease Lease.
         * @param candidate The node is a leaseholder candidate.
         * @return True when the candidate can be a leaseholder, otherwise false.
         */
        private boolean isReplicationGroupUpdateLeaseholder(Lease lease, ClusterNode candidate) {
            HybridTimestamp now = clock.now();

            return lease == EMPTY_LEASE
                    || (!candidate.equals(lease.getLeaseholder()) && compareWithClockSkew(now, lease.getLeaseExpirationTime()) > 0);
        }
    }
}
