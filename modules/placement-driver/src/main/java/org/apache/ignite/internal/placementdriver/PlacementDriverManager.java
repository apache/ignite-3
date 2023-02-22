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
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.or;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.stablePartAssignmentsKey;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Placement driver manager.
 * The manager is a leaseholder tracker: response of renewal of the lease and discover of appear/disappear of a replication group member.
 * The another role of the manager is providing a node, which is leaseholder at the moment, for a particular replication group.
 */
public class PlacementDriverManager implements IgniteComponent {
    public static final String PLACEMENTDRIVER_PREFIX = "placementdriver.lease";
    public static final String LEASE_STOP_KEY_PREFIX = PLACEMENTDRIVER_PREFIX + ".stop.";
    public static final String LEASE_HOLDER_KEY_PREFIX = PLACEMENTDRIVER_PREFIX + ".holder.";
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
    private final Object leaseUpdaterLocker;
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
    /**
     * Lease group cache.
     */
    private final Map<ReplicationGroupId, ReplicationGroup> replicationGroups;
    /** Logical topology snapshot. The property is updated when the snapshot saves in metastorage. */
    private LogicalTopologySnapshot logicalTopologySnap;
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
        this.leaseUpdaterLocker = new Object();

        raftClientFuture = new CompletableFuture<>();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        metaStorageManager.registerPrefixWatch(ByteArray.fromString(PLACEMENTDRIVER_PREFIX), new UpdateLeaseHolderWatchListener());

        placementDriverNodesNamesProvider.get()
                .thenCompose(placementDriverNodes -> {
                    String thisNodeName = clusterService.topologyService().localMember().name();

                    if (placementDriverNodes.contains(thisNodeName)) {
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
    }

    /** {@inheritDoc} */
    @Override
    public void beforeNodeStop() {
        withRaftClientIfPresent(c -> c.unsubscribeLeader().join());
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

        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().listen(assignmentCfgListener);
        metaStorageManager.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), assignmentWatchListener);
        logicalTopologyService.addEventListener(topologyEventListener);

        restoreReplicationGroupMap();

        startUpdater();
    }

    /**
     * Restores a map that contains context of replication group lease.
     */
    private void restoreReplicationGroupMap() {
        logicalTopologyService.logicalTopologyOnLeader().thenAccept(topologySnap -> {
            if (logicalTopologySnap == null || logicalTopologySnap.version() < topologySnap.version()) {
                logicalTopologySnap = topologySnap;

                LOG.debug("Logical topology initialized in placement driver [topologySnap={}]", topologySnap);
            }
        });

        for (int i = 0; i < tablesCfg.tables().value().size(); i++) {
            var tblView = (ExtendedTableView) tablesCfg.tables().value().get(i);

            List<Set<Assignment>> tblAssignment = ByteUtils.fromBytes(tblView.assignments());

            int part = 0;

            for (Set<Assignment> assignments : tblAssignment) {
                TablePartitionId grpId = new TablePartitionId(tblView.id(), part);

                replicationGroups.compute(grpId, (groupId, replicationGroup) -> {
                    if (replicationGroup == null) {
                        replicationGroup = new ReplicationGroup();
                    }

                    replicationGroup.assignments = assignments;

                    return replicationGroup;
                });

                part++;
            }
        }

        try (Cursor<VaultEntry> cursor = vaultManager.range(
                ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX),
                ByteArray.fromString(incrementLastChar(STABLE_ASSIGNMENTS_PREFIX))
        )) {
            for (VaultEntry entry : cursor) {
                String key = entry.key().toString();

                key = key.replace(STABLE_ASSIGNMENTS_PREFIX, "");

                TablePartitionId grpId = TablePartitionId.fromString(key);

                Set<Assignment> assignments = ByteUtils.fromBytes(entry.value());

                replicationGroups.compute(grpId, (groupId, replicationGroup) -> {
                    if (replicationGroup == null) {
                        replicationGroup = new ReplicationGroup();
                    }

                    replicationGroup.assignments = assignments;

                    return replicationGroup;
                });
            }
        }

        try (Cursor<VaultEntry> cursor = vaultManager.range(
                ByteArray.fromString(PLACEMENTDRIVER_PREFIX),
                ByteArray.fromString(incrementLastChar(PLACEMENTDRIVER_PREFIX))
        )) {
            for (VaultEntry entry : cursor) {
                String key = entry.key().toString();

                HybridTimestamp ts = null;
                ClusterNode leaseHolder = null;

                if (key.startsWith(LEASE_STOP_KEY_PREFIX)) {
                    key = key.replace(LEASE_STOP_KEY_PREFIX, "");
                    ts = ByteUtils.fromBytes(entry.value());
                } else if (key.startsWith(LEASE_HOLDER_KEY_PREFIX)) {
                    key = key.replace(LEASE_HOLDER_KEY_PREFIX, "");
                    leaseHolder = ByteUtils.fromBytes(entry.value());
                }

                TablePartitionId grpId = TablePartitionId.fromString(key);

                var tsFinal = ts;
                var leaseHolderFinal = leaseHolder;

                replicationGroups.compute(grpId, (groupId, replicationGroup) -> {
                    if (replicationGroup == null) {
                        replicationGroup = new ReplicationGroup();
                    }

                    if (replicationGroup.stopLeas != null) {
                        return replicationGroup;
                    }

                    replicationGroup.stopLeas = tsFinal;
                    replicationGroup.leaseHolder = leaseHolderFinal;

                    if (replicationGroup.assignments == null) {
                        VaultEntry vaultEntry = vaultManager.get(stablePartAssignmentsKey(grpId)).join();

                        if (vaultEntry == null || vaultEntry.value() == null) {
                            restoreAssignmentsFromCfg(grpId, replicationGroup);
                        } else {
                            replicationGroup.assignments = ByteUtils.fromBytes(vaultEntry.value());
                        }
                    }

                    return replicationGroup;
                });
            }
        }
    }

    /**
     * Restores assignments from a table configuration.
     *
     * @param grpId Table replication group id.;
     * @param replicationGroup Lease group context.
     */
    private void restoreAssignmentsFromCfg(TablePartitionId grpId, ReplicationGroup replicationGroup) {
        var tables = tablesCfg.tables().value();

        for (int i = 0; i < tables.size(); i++) {
            var tblCfg = ((ExtendedTableConfiguration) tables.get(i));

            if (grpId.tableId().equals(tblCfg.id().value())) {
                replicationGroup.assignments = ByteUtils.fromBytes(tblCfg.assignments().value());
            }
        }
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

        ((ExtendedTableConfiguration) tablesCfg.tables().any()).assignments().stopListen(assignmentCfgListener);
        metaStorageManager.unregisterWatch(assignmentWatchListener);
        logicalTopologyService.removeEventListener(topologyEventListener);

        if (updater != null) {
            updater.interrupt();
            updater = null;
        }

        replicationGroups.clear();
    }

    @TestOnly
    boolean isActiveActor() {
        return isActiveActor;
    }

    /**
     * Starts a dedicated thread to renew or assign leases.
     */
    private void startUpdater() {
        updater = new Thread("lease-updater") {
            @Override
            public void run() {
                while (isActiveActor) {
                    for (ReplicationGroupId grpId : replicationGroups.keySet()) {
                        ReplicationGroup grp = replicationGroups.get(grpId);

                        HybridTimestamp now = clock.now();

                        if (grp.stopLeas == null || now.getPhysical() > (grp.stopLeas.getPhysical() - LEASE_PERIOD / 2)) {
                            var leaseStopKey = ByteArray.fromString(LEASE_STOP_KEY_PREFIX + grpId);
                            var leaseHolderKey = ByteArray.fromString(LEASE_HOLDER_KEY_PREFIX + grpId);

                            var newTs = new HybridTimestamp(now.getPhysical() + LEASE_PERIOD, 0);

                            ClusterNode holder = nextLeaseHolder(grp);

                            metaStorageManager.invoke(
                                    or(notExists(leaseStopKey), value(leaseStopKey).eq(ByteUtils.toBytes(grp.stopLeas))),
                                    List.of(put(leaseStopKey, ByteUtils.toBytes(newTs)), put(leaseHolderKey, ByteUtils.toBytes(holder))),
                                    List.of()
                            ).thenAccept(applied -> {
                                if (applied) {
                                    updateHolderInternal(grp, newTs, holder);
                                }
                            });
                        }
                    }

                    synchronized (leaseUpdaterLocker) {
                        try {
                            leaseUpdaterLocker.wait(UPDATE_LEASE_MS);
                        } catch (InterruptedException e) {
                            LOG.error("Lease updater is interrupted");
                        }
                    }
                }
            }
        };

        updater.start();
    }

    /**
     * Finds a node that can be the leaseholder.
     *
     * @param grp Replication group lease context.
     * @return Cluster node, or {@code null} if no node in assignments can be the leaseholder.
     */
    @Nullable
    private ClusterNode nextLeaseHolder(ReplicationGroup grp) {
        ClusterNode curHolder = grp.leaseHolder;

        if (curHolder != null
                && hasNodeIsAssignment(grp.assignments, curHolder)
                && hasNodeInCluster(curHolder)
                && hasNodeInLogicalTopology(curHolder)) {
            return curHolder;
        }

        //TODO: IGNITE-18879 Implement more intellectual algorithm to choose a node.
        for (Assignment assignment : grp.assignments) {
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
        return logicalTopologySnap != null && logicalTopologySnap.nodes().stream().anyMatch(n -> n.name().equals(node.name()));
    }

    /**
     * Checks a node to contains in the cluster topology.
     *
     * @param node Node to check.
     * @return True if node is in the cluster topology.
     */
    private boolean hasNodeInCluster(ClusterNode node) {
        return clusterService.topologyService().getByConsistentId(node.name()) != null;
    }

    /**
     * Checks a node to contains in the replication group assignment.
     *
     * @param assignments Replication group assignment.
     * @param node Node to check.
     * @return True if node is in the replication group assignment.
     */
    private static boolean hasNodeIsAssignment(Set<Assignment> assignments, ClusterNode node) {
        return assignments.stream().anyMatch(assignment -> assignment.consistentId().equals(node.name()));
    }

    /**
     * Updates lease holder.
     *
     * @param grp Replication group state.
     */
    private void updateHolderInternal(ReplicationGroup grp, HybridTimestamp ts, ClusterNode leaseHolder) {
        grp.stopLeas = ts;
        grp.leaseHolder = leaseHolder;

        //TODO: IGNITE-18742 Implement logic for a maintenance phase of group lease management.
    }

    /**
     * Listen lease holder updates.
     */
    private class UpdateLeaseHolderWatchListener implements WatchListener {
        @Override
        public void onUpdate(WatchEvent event) {
            //TODO: IGNITE-18859 Update list of primary on each cluster node.
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

            if (assignmentsCtx.newValue() == null) {
                assert assignmentsCtx.oldValue() != null : "Old assignments are empty";

                List<Set<Assignment>> oldAssignments = ByteUtils.fromBytes(assignmentsCtx.oldValue());

                for (int part = 0; part < oldAssignments.size(); part++) {
                    var replicationGrpId = new TablePartitionId(tblId, part);

                    replicationGroups.remove(replicationGrpId);

                    removeLeaseFormStorage(replicationGrpId);
                }
            } else {
                List<Set<Assignment>> assignments = ByteUtils.fromBytes(assignmentsCtx.newValue());

                for (int part = 0; part < assignments.size(); part++) {
                    Set<Assignment> partAssignments = assignments.get(part);

                    var replicationGrpId = new TablePartitionId(tblId, part);

                    replicationGroups.compute(replicationGrpId, (id, grp) -> {
                        if (!isActiveActor) {
                            return null;
                        }

                        if (grp == null) {
                            grp = new ReplicationGroup();
                        }

                        grp.assignments = partAssignments;

                        return grp;
                    });
                }
            }

            synchronized (leaseUpdaterLocker) {
                leaseUpdaterLocker.notifyAll();
            }

            return completedFuture(null);
        }
    }

    /**
     * Removes lease group info from storage.
     *
     * @param replicationGrpId Replication group id.
     */
    private void removeLeaseFormStorage(TablePartitionId replicationGrpId) {
        var leaseStopKey = ByteArray.fromString(LEASE_STOP_KEY_PREFIX + replicationGrpId);
        var leaseHolderKey = ByteArray.fromString(LEASE_HOLDER_KEY_PREFIX + replicationGrpId);

        metaStorageManager.invoke(or(exists(leaseStopKey), exists(leaseHolderKey)),
                List.of(remove(leaseStopKey), remove(leaseHolderKey)), List.of());
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
            if (logicalTopologySnap == null || logicalTopologySnap.version() < topologySnap.version()) {
                logicalTopologySnap = topologySnap;

                LOG.debug("Logical topology updated in placement driver [topologySnap={}]", topologySnap);
            }

            synchronized (leaseUpdaterLocker) {
                leaseUpdaterLocker.notifyAll();
            }
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
                    replicationGroups.remove(replicationGrpId);

                    removeLeaseFormStorage(replicationGrpId);
                } else {
                    Set<Assignment> assignments = ByteUtils.fromBytes(evt.newEntry().value());

                    replicationGroups.compute(replicationGrpId, (id, grp) -> {
                        if (!isActiveActor) {
                            return null;
                        }

                        if (grp == null) {
                            grp = new ReplicationGroup();
                        }

                        grp.assignments = assignments;

                        return grp;
                    });
                }
            }

            synchronized (leaseUpdaterLocker) {
                leaseUpdaterLocker.notifyAll();
            }
        }

        @Override
        public void onError(Throwable e) {
        }
    }

    /**
     * Replica group holder.
     */
    private static class ReplicationGroup {
        ClusterNode leaseHolder;
        HybridTimestamp stopLeas;
        Set<Assignment> assignments;

        @Override
        public String toString() {
            return "ReplicationGroup{"
                    + "leaseHolder=" + leaseHolder
                    + ", stopLeas=" + stopLeas
                    + ", assignments=" + assignments
                    + '}';
        }
    }
}
