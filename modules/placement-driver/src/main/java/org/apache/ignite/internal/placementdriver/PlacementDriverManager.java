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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.jetbrains.annotations.TestOnly;

/**
 * Placement driver manager.
 * The manager is a leaseholder tracker: response of renewal of the lease and discover of appear/disappear of a replication group member.
 * The another role of the manager is providing a node, which is leaseholder at the moment, for a particular replication group.
 */
public class PlacementDriverManager implements IgniteComponent {
    public static final String PLACEMENTDRIVER_PREFIX = "placementdriver.lease.";
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PlacementDriverManager.class);
    private static final long UPDATE_LEASE_MS = 200L;
    public static final long LEASE_PERIOD = 10 * UPDATE_LEASE_MS;
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final RaftMessagesFactory raftMessagesFactory = new RaftMessagesFactory();
    /** Prevents double stopping of the component. */
    private final AtomicBoolean isStopped = new AtomicBoolean();
    /** Metastorage manager. */
    private final MetaStorageManager metaStorageManager;
    /**
     * Vault manager.
     */
    private final VaultManager vaultManager;
    /**
     * Configuration.
     * TODO: The property is removed after assignments will moved to metastorage.
     */
    private final TablesConfiguration tablesCfg;
    private final HybridClock clock;
    private final ClusterService clusterService;
    private final ReplicationGroupId replicationGroupId;
    private final Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider;
    /** Raft client future. Can contain null, if this node is not in placement driver group. */
    private final CompletableFuture<TopologyAwareRaftGroupService> raftClientFuture;
    private final ScheduledExecutorService raftClientExecutor;
    private final LogicalTopologyService logicalTopologyService;
    private final RaftConfiguration raftConfiguration;
    private volatile boolean isActiveActor;
    /** Assignment configuration listener. */
    private final ConfigurationListener<byte[]> assignmentCfgListener = new AssignmentCfgListener();
    /** Assignment metastorage watch listener. */
    private final AssignmentWatchListener assignmentWatchListener = new AssignmentWatchListener();
    /** Topology metastorage watch listener. */
    private final LogicalTopologyEventListener topologyEventListener = new PlacementDriverLogicalTopologyEventListener();

    /** Listener to update a leaseholder map. */
    private final UpdateLeaseHolderWatchListener updateLeaseWatchListener = new UpdateLeaseHolderWatchListener();
    /**
     * Lease group cache.
     */
    private final Map<ReplicationGroupId, ReplicationGroupLease> replicationGroups;
    /** Logical topology snapshot. The property is updated when the snapshot saves in metastorage. */
    private AtomicReference<LogicalTopologySnapshot> logicalTopologySnap;

    /** Map replication group id to assignment nodes. */
    private Map<ReplicationGroupId, Set<Assignment>> groupAssignments;
    /** Tread to update leases. */
    private Thread updater;

    /**
     * The constructor.
     *
     * @param metaStorageMgr Meta Storage manager.
     * @param replicationGroupId Id of placement driver group.
     * @param clusterService Cluster service.
     * @param raftConfiguration Raft configuration.
     * @param placementDriverNodesNamesProvider Provider of the set of placement driver nodes' names.
     * @param logicalTopologyService Logical topology service.
     * @param raftClientExecutor Raft client executor.
     * @param tablesCfg Table configuration.
     * @param clock Hybrid clock.
     */
    public PlacementDriverManager(
            MetaStorageManager metaStorageMgr,
            VaultManager vaultManager,
            ReplicationGroupId replicationGroupId,
            ClusterService clusterService,
            RaftConfiguration raftConfiguration,
            Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider,
            LogicalTopologyService logicalTopologyService,
            ScheduledExecutorService raftClientExecutor,
            TablesConfiguration tablesCfg,
            HybridClock clock
    ) {
        this.metaStorageManager = metaStorageMgr;
        this.vaultManager = vaultManager;
        this.replicationGroupId = replicationGroupId;
        this.clusterService = clusterService;
        this.raftConfiguration = raftConfiguration;
        this.placementDriverNodesNamesProvider = placementDriverNodesNamesProvider;
        this.logicalTopologyService = logicalTopologyService;
        this.raftClientExecutor = raftClientExecutor;
        this.tablesCfg = tablesCfg;
        this.clock = clock;

        this.replicationGroups = new ConcurrentHashMap<>();
        this.groupAssignments = new ConcurrentHashMap<>();
        this.logicalTopologySnap = new AtomicReference<>();
        this.raftClientFuture = new CompletableFuture<>();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        metaStorageManager.registerPrefixWatch(ByteArray.fromString(PLACEMENTDRIVER_PREFIX), updateLeaseWatchListener);

        placementDriverNodesNamesProvider.get()
                .thenCompose(placementDriverNodes -> {
                    String thisNodeName = clusterService.topologyService().localMember().name();

                    if (placementDriverNodes.contains(thisNodeName)) {
                        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().listen(assignmentCfgListener);
                        metaStorageManager.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), assignmentWatchListener);
                        logicalTopologyService.addEventListener(topologyEventListener);

                        return TopologyAwareRaftGroupService.start(
                                replicationGroupId,
                                clusterService,
                                raftMessagesFactory,
                                raftConfiguration,
                                PeersAndLearners.fromConsistentIds(placementDriverNodes),
                                true,
                                raftClientExecutor,
                                logicalTopologyService,
                                true
                            ).thenCompose(client -> {
                                TopologyAwareRaftGroupService topologyAwareClient = (TopologyAwareRaftGroupService) client;

                                return topologyAwareClient.subscribeLeader(this::onLeaderChange).thenApply(v -> topologyAwareClient);
                            });
                    } else {
                        return completedFuture(null);
                    }
                })
                .whenComplete((client, ex) -> {
                    if (ex == null) {
                        raftClientFuture.complete(client);
                    } else {
                        raftClientFuture.completeExceptionally(ex);
                    }
                });

        restoreReplicationGroupMap();
    }

    /** {@inheritDoc} */
    @Override
    public void beforeNodeStop() {
        withRaftClientIfPresent(c -> {
            c.unsubscribeLeader().join();

            logicalTopologyService.removeEventListener(topologyEventListener);
            metaStorageManager.unregisterWatch(assignmentWatchListener);
            ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().stopListen(assignmentCfgListener);
        });
        metaStorageManager.registerPrefixWatch(ByteArray.fromString(PLACEMENTDRIVER_PREFIX), updateLeaseWatchListener);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        withRaftClientIfPresent(TopologyAwareRaftGroupService::shutdown);
    }

    private void withRaftClientIfPresent(Consumer<TopologyAwareRaftGroupService> closure) {
        raftClientFuture.thenAccept(client -> {
            if (client != null) {
                closure.accept(client);
            }
        });
    }

    private void onLeaderChange(ClusterNode leader, Long term) {
        if (leader.equals(clusterService.topologyService().localMember())) {
            takeOverActiveActor();
        } else {
            stepDownActiveActor();
        }
    }

    /**
     * Takes over active actor of placement driver group.
     */
    private void takeOverActiveActor() {
        isActiveActor = true;

        startUpdater();
    }

    /**
     * Restores a map that contains context of replication group lease.
     */
    private void restoreReplicationGroupMap() {
        logicalTopologyService.logicalTopologyOnLeader().thenAccept(topologySnap -> {
            LogicalTopologySnapshot logicalTopologySnap0;

            do {
                logicalTopologySnap0 = logicalTopologySnap.get();

                if (logicalTopologySnap0 != null && logicalTopologySnap0.version() >= topologySnap.version()) {
                    break;
                }
            } while (!logicalTopologySnap.compareAndSet(logicalTopologySnap0, topologySnap));

            LOG.info("Logical topology initialized in placement driver [topologySnap={}]", topologySnap);
        });

        try (Cursor<VaultEntry> cursor = vaultManager.range(
                ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX),
                ByteArray.fromString(incrementLastChar(STABLE_ASSIGNMENTS_PREFIX))
        )) {
            for (VaultEntry entry : cursor) {
                String key = entry.key().toString();

                key = key.replace(STABLE_ASSIGNMENTS_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);

                Set<Assignment> assignments = ByteUtils.fromBytes(entry.value());

                groupAssignments.put(grpId, assignments);
            }
        }

        LOG.info("Assignment cache initialized in placement driver [groupAssignments={}]", groupAssignments);

        try (Cursor<VaultEntry> cursor = vaultManager.range(
                ByteArray.fromString(PLACEMENTDRIVER_PREFIX),
                ByteArray.fromString(incrementLastChar(PLACEMENTDRIVER_PREFIX))
        )) {
            for (VaultEntry entry : cursor) {
                String key = entry.key().toString();

                key = key.replace(PLACEMENTDRIVER_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);
                ReplicationGroupLease lease = ByteUtils.fromBytes(entry.value());

                replicationGroups.put(grpId, lease);
            }
        }

        LOG.info("Leases map recovered [replicationGroups={}]", replicationGroups);
    }

    private static String incrementLastChar(String str) {
        char lastChar = str.charAt(str.length() - 1);

        return str.substring(0, str.length() - 1) + (char) (lastChar + 1);
    }


    /**
     * Steps down as active actor.
     */
    private void stepDownActiveActor() {
        isActiveActor = false;

        stopUpdater();
    }

    @TestOnly
    boolean isActiveActor() {
        return isActiveActor;
    }

    /**
     * Starts a dedicated thread to renew or assign leases.
     */
    private void startUpdater() {
        //TODO: IGNITE-18879 Implement lease maintenance.
        updater = new Thread(NamedThreadFactory.threadPrefix(clusterService.topologyService().localMember().name(), "lease-updater")) {
            @Override
            public void run() {
                while (isActiveActor) {
                    for (Map.Entry<ReplicationGroupId, Set<Assignment>> entry : groupAssignments.entrySet()) {
                        ReplicationGroupId grpId = entry.getKey();

                        ReplicationGroupLease lease = replicationGroups.getOrDefault(grpId, new ReplicationGroupLease());

                        HybridTimestamp now = clock.now();

                        if (lease.stopLeas == null || now.getPhysical() > (lease.stopLeas.getPhysical() - LEASE_PERIOD / 2)) {
                            var leaseKey = ByteArray.fromString(PLACEMENTDRIVER_PREFIX + grpId);

                            var newTs = new HybridTimestamp(now.getPhysical() + LEASE_PERIOD, 0);

                            ClusterNode holder = nextLeaseHolder(entry.getValue());

                            if (lease.leaseholder == null && holder != null
                                    || lease.leaseholder != null && lease.leaseholder.equals(holder)
                                    || holder != null && (lease.stopLeas == null || now.getPhysical() > lease.stopLeas.getPhysical())) {
                                byte[] leaseRaw = ByteUtils.toBytes(lease);

                                ReplicationGroupLease renewedLease = new ReplicationGroupLease();
                                renewedLease.stopLeas = newTs;
                                renewedLease.leaseholder = holder;

                                metaStorageManager.invoke(
                                        or(notExists(leaseKey), value(leaseKey).eq(leaseRaw)),
                                        put(leaseKey, ByteUtils.toBytes(renewedLease)),
                                        noop()
                                );
                            }
                        }
                    }

                    try {
                        Thread.sleep(UPDATE_LEASE_MS);
                    } catch (InterruptedException e) {
                        LOG.error("Lease updater is interrupted");
                    }
                }
            }
        };

        updater.start();
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
            ClusterNode candidate = clusterService.topologyService().getByConsistentId(assignment.consistentId());

            if (candidate != null && hasNodeInLogicalTopology(candidate)) {
                return candidate;
            }
        }

        return null;
    }

    /**
     * Checks a node to contains in the logical topology.
     *
     * @param node Node to check.
     * @return True if node is in the logical topology.
     */
    private boolean hasNodeInLogicalTopology(ClusterNode node) {
        LogicalTopologySnapshot logicalTopologySnap0 = logicalTopologySnap.get();

        return logicalTopologySnap0 != null && logicalTopologySnap0.nodes().stream().anyMatch(n -> n.name().equals(node.name()));
    }

    /**
     * Stops a dedicated thread to renew or assign leases.
     */
    private void stopUpdater() {
        //TODO: IGNITE-18879 Implement lease maintenance.
        if (updater != null) {
            updater.interrupt();

            updater = null;
        }
    }

    /**
     * Triggers to renew leases forcibly.
     */
    private void triggerToRenewLeases() {
        if (!isActiveActor) {
            return;
        }

        //TODO: IGNITE-18879 Implement lease maintenance.
    }

    /**
     * Listen lease holder updates.
     */
    private class UpdateLeaseHolderWatchListener implements WatchListener {
        @Override
        public void onUpdate(WatchEvent event) {
            for (EntryEvent entry : event.entryEvents()) {
                Entry msEntry = entry.newEntry();

                String key = new ByteArray(msEntry.key()).toString();

                key = key.replace(PLACEMENTDRIVER_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);

                if (msEntry.empty()) {
                    replicationGroups.remove(grpId);
                } else {
                    ReplicationGroupLease lease = ByteUtils.fromBytes(msEntry.value());

                    replicationGroups.put(grpId, lease);
                }

                if (msEntry.empty() || entry.oldEntry().empty()) {
                    triggerToRenewLeases();
                }
            }
        }

        @Override
        public void onError(Throwable e) {
        }
    }


    /**
     * Configuration assignments listener.
     */
    private class AssignmentCfgListener implements ConfigurationListener<byte[]> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<byte[]> assignmentsCtx) {
            ExtendedTableConfiguration tblCfg = assignmentsCtx.config(ExtendedTableConfiguration.class);

            UUID tblId = tblCfg.id().value();

            LOG.debug("Table assignments configuration update for placement driver [revision={}, tblId={}]",
                    assignmentsCtx.storageRevision(), tblId);

            List<Set<Assignment>> tableAssignments =
                    assignmentsCtx.newValue() == null ? null : ByteUtils.fromBytes(assignmentsCtx.newValue());

            for (int part = 0; part < tblCfg.partitions().value(); part++) {
                var replicationGrpId = new TablePartitionId(tblId, part);

                if (tableAssignments == null) {
                    groupAssignments.remove(replicationGrpId);
                } else {
                    groupAssignments.put(replicationGrpId, tableAssignments.get(part));
                }
            }

            return completedFuture(null);
        }
    }

    /**
     * Metastorage topology watch.
     */
    private class PlacementDriverLogicalTopologyEventListener implements LogicalTopologyEventListener {
        @Override
        public void onNodeJoined(ClusterNode joinedNode, LogicalTopologySnapshot newTopology) {
            onUpdate(newTopology);
        }

        @Override
        public void onNodeLeft(ClusterNode leftNode, LogicalTopologySnapshot newTopology) {
            onUpdate(newTopology);
        }

        /**
         * Updates local topology cache.
         *
         * @param topologySnap Topology snasphot.
         */
        public void onUpdate(LogicalTopologySnapshot topologySnap) {
            LogicalTopologySnapshot logicalTopologySnap0;

            do {
                logicalTopologySnap0 = logicalTopologySnap.get();

                if (logicalTopologySnap0 != null && logicalTopologySnap0.version() >= topologySnap.version()) {
                    break;
                }
            } while (!logicalTopologySnap.compareAndSet(logicalTopologySnap0, topologySnap));

            LOG.debug("Logical topology updated in placement driver [topologySnap={}]", topologySnap);

            triggerToRenewLeases();
        }
    }

    /**
     * Metastorage assignments watch.
     */
    private class AssignmentWatchListener implements WatchListener {
        @Override
        public void onUpdate(WatchEvent event) {
            assert !event.entryEvent().newEntry().empty() : "New assignments are empty";

            LOG.debug("Assignment update [revision={}, key={}]", event.revision(),
                    new ByteArray(event.entryEvent().newEntry().key()));

            for (EntryEvent evt : event.entryEvents()) {
                var replicationGrpId = TablePartitionId.fromString(
                        new String(evt.newEntry().key(), StandardCharsets.UTF_8).replace(STABLE_ASSIGNMENTS_PREFIX, ""));

                if (evt.newEntry().empty()) {
                    groupAssignments.remove(replicationGrpId);
                } else {
                    groupAssignments.put(replicationGrpId, ByteUtils.fromBytes(evt.newEntry().value()));
                }
            }
        }

        @Override
        public void onError(Throwable e) {
        }
    }

    /**
     * Replica group holder.
     */
    public static class ReplicationGroupLease implements Serializable {
        ClusterNode leaseholder;
        HybridTimestamp stopLeas;

        @Override
        public String toString() {
            return "ReplicationGroup{"
                    + "leaseHolder=" + leaseholder
                    + ", stopLeas=" + stopLeas
                    + '}';
        }
    }
}
