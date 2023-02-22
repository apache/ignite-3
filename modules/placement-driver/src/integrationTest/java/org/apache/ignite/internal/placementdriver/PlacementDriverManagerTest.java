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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.affinity.AffinityUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl.LOGICAL_TOPOLOGY_KEY;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.LEASE_HOLDER_KEY_PREFIX;
import static org.apache.ignite.internal.placementdriver.PlacementDriverManager.LEASE_STOP_KEY_PREFIX;
import static org.apache.ignite.internal.raft.Loza.CLIENT_POOL_NAME;
import static org.apache.ignite.internal.raft.Loza.CLIENT_POOL_SIZE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.lang.ByteArray.fromString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There are tests for Placement driver manager.
 */
@ExtendWith(ConfigurationExtension.class)
public class PlacementDriverManagerTest extends IgniteAbstractTest {
    public static final int PORT = 1234;
    private String nodeName;

    HybridClock clock = new HybridClockImpl();
    private VaultManager vaultManager;

    private ClusterService clusterService;

    private Loza raftManager;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private TablesConfiguration tblsCfg;

    private MetaStorageManagerImpl metaStorageManager;

    private PlacementDriverManager placementDriverManager;

    private ScheduledExecutorService raftExecutorService;

    private TestInfo testInfo;

    @BeforeEach
    private void beforeTest(TestInfo testInfo) throws NodeStoppingException {
        this.nodeName = testNodeName(testInfo, PORT);
        this.testInfo = testInfo;

        startPlacementDriverManager();
    }

    private void startPlacementDriverManager() throws NodeStoppingException {
        vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault")));

        var nodeFinder = new StaticNodeFinder(Collections.singletonList(new NetworkAddress("localhost", PORT)));

        clusterService = ClusterServiceTestUtils.clusterService(testInfo, PORT, nodeFinder);

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageNodes())
                .thenReturn(completedFuture(Set.of(clusterService.localConfiguration().getName())));

        raftManager = new Loza(clusterService, raftConfiguration, workDir.resolve("loza"), new HybridClockImpl());

        var storage = new SimpleInMemoryKeyValueStorage(nodeName);

        metaStorageManager = new MetaStorageManagerImpl(
                vaultManager,
                clusterService,
                cmgManager,
                mock(LogicalTopologyService.class),
                raftManager,
                storage
        );

        raftExecutorService = new ScheduledThreadPoolExecutor(CLIENT_POOL_SIZE,
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(clusterService.localConfiguration().getName(),
                        CLIENT_POOL_NAME), log
                ));

        placementDriverManager = new PlacementDriverManager(
                metaStorageManager,
                vaultManager,
                MetastorageGroupId.INSTANCE,
                clusterService,
                raftConfiguration,
                () -> completedFuture(peersAndLearners(
                        new HashMap<>(Map.of(new NetworkAddress("localhost", PORT), clusterService)),
                        addr -> true,
                        1)
                        .peers().stream().map(Peer::consistentId).collect(toSet())),
                new LogicalTopologyServiceTestImpl(clusterService),
                raftExecutorService,
                tblsCfg,
                clock);

        vaultManager.start();
        clusterService.start();
        raftManager.start();
        metaStorageManager.start();
        placementDriverManager.start();

        metaStorageManager.deployWatches();
    }

    @AfterEach
    private void afterEach() throws Exception {
        stopPlacementDriverManager();
    }

    private void stopPlacementDriverManager() throws Exception {
        placementDriverManager.beforeNodeStop();
        metaStorageManager.beforeNodeStop();
        raftManager.beforeNodeStop();
        clusterService.beforeNodeStop();
        vaultManager.beforeNodeStop();

        placementDriverManager.stop();
        metaStorageManager.stop();
        raftManager.stop();
        clusterService.stop();
        vaultManager.stop();
        raftExecutorService.shutdown();
    }

    private static PeersAndLearners peersAndLearners(
            HashMap<NetworkAddress, ClusterService> clusterServices,
            Predicate<NetworkAddress> isServerAddress,
            int nodes
    ) {
        return PeersAndLearners.fromConsistentIds(
                getNetworkAddresses(nodes).stream().filter(isServerAddress)
                        .map(netAddr -> clusterServices.get(netAddr).topologyService().localMember().name()).collect(
                                toSet()));
    }

    /**
     * Generates a node address for each node.
     *
     * @param nodes Node count.
     * @return List on network addresses.
     */
    private static List<NetworkAddress> getNetworkAddresses(int nodes) {
        List<NetworkAddress> addresses = IntStream.range(PORT, PORT + nodes)
                .mapToObj(port -> new NetworkAddress("localhost", port))
                .collect(Collectors.toList());
        return addresses;
    }

    @Test
    public void testLeaseRenew() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        Set<Assignment> assignments = calculateAssignmentForPartition(Collections.singleton(nodeName), 1, 1);

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), ByteUtils.toBytes(assignments));

        var snapshot = new LogicalTopologySnapshot(1,
                Collections.singleton(new ClusterNode(nodeName, nodeName, new NetworkAddress("localhost", PORT))));

        metaStorageManager.put(new ByteArray(LOGICAL_TOPOLOGY_KEY), ByteUtils.toBytes(snapshot));

        checkLeaseCreated(grpPart0);

        var leaseStopFut = metaStorageManager.get(fromString(LEASE_STOP_KEY_PREFIX + grpPart0));

        HybridTimestamp ts = ByteUtils.fromBytes(leaseStopFut.join().value());

        assertNotNull(ts);

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(fromString(LEASE_STOP_KEY_PREFIX + grpPart0));

            HybridTimestamp tsRenew = ByteUtils.fromBytes(fut.join().value());

            return ts.compareTo(tsRenew) < 0;

        }, 10_000));
    }

    @Test
    public void testLeaseholderUpdate() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0);

        Set<Assignment> assignments = Set.of();

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), ByteUtils.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(fromString(LEASE_HOLDER_KEY_PREFIX + grpPart0));

            ClusterNode leaseholder = ByteUtils.fromBytes(fut.join().value());

            return leaseholder == null;

        }, 10_000));

        assignments = calculateAssignmentForPartition(Collections.singleton(nodeName), 1, 1);

        metaStorageManager.put(fromString(STABLE_ASSIGNMENTS_PREFIX + grpPart0), ByteUtils.toBytes(assignments));

        assertTrue(waitForCondition(() -> {
            var fut = metaStorageManager.get(fromString(LEASE_HOLDER_KEY_PREFIX + grpPart0));

            ClusterNode leaseholder = ByteUtils.fromBytes(fut.join().value());

            return leaseholder != null && leaseholder.name().equals(nodeName);

        }, 10_000));
    }

    @Test
    public void testLeaseRestore() throws Exception {
        TablePartitionId grpPart0 = createTableAssignment();

        checkLeaseCreated(grpPart0);

        stopPlacementDriverManager();
        startPlacementDriverManager();

        checkLeaseCreated(grpPart0);
    }

    /**
     * Checks if a group lease is created during the timeout.
     *
     * @param grpPartId Replication group id.
     * @throws InterruptedException If the waiting is interrupted.
     */
    private void checkLeaseCreated(TablePartitionId grpPartId) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            var leaseFut = metaStorageManager.getAll(Set.of(
                    fromString(LEASE_STOP_KEY_PREFIX + grpPartId),
                    fromString(LEASE_HOLDER_KEY_PREFIX + grpPartId)
            ));

            var entries = leaseFut.join();

            Entry leaseStopMsEntry = entries.get(fromString(LEASE_STOP_KEY_PREFIX + grpPartId));
            Entry leaseHolderMsEntry = entries.get(fromString(LEASE_HOLDER_KEY_PREFIX + grpPartId));

            return leaseStopMsEntry != null && leaseHolderMsEntry != null && !leaseStopMsEntry.empty() && !leaseHolderMsEntry.empty();
        }, 10_000));
    }

    /**
     * Creates an assignment for the fake table.
     *
     * @return Replication group id.
     * @throws Exception If failed.
     */
    private TablePartitionId createTableAssignment() throws Exception {
        AtomicReference<UUID> tblIdRef = new AtomicReference<>();

        List<Set<Assignment>> assignments = AffinityUtils.calculateAssignments(Collections.singleton(nodeName), 1, 1);

        tblsCfg.tables().change(tableViewTableChangeNamedListChange -> {
            tableViewTableChangeNamedListChange.create("test-table", tableChange -> {
                var extConfCh = ((ExtendedTableChange) tableChange);
                extConfCh.changePartitions(1);
                extConfCh.changeReplicas(1);

                tblIdRef.set(extConfCh.id());

                extConfCh.changeAssignments(ByteUtils.toBytes(assignments));
            });
        }).get();

        var grpPart0 = new TablePartitionId(tblIdRef.get(), 0);

        log.info("Fake table created [id={}, repGrp={}]", tblIdRef.get(), grpPart0);
        return grpPart0;
    }

    /**
     * Test implementation of {@link LogicalTopologyService}.
     */
    protected static class LogicalTopologyServiceTestImpl implements LogicalTopologyService {
        private final ClusterService clusterService;

        public LogicalTopologyServiceTestImpl(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public void addEventListener(LogicalTopologyEventListener listener) {

        }

        @Override
        public void removeEventListener(LogicalTopologyEventListener listener) {

        }

        @Override
        public CompletableFuture<LogicalTopologySnapshot> logicalTopologyOnLeader() {
            return completedFuture(new LogicalTopologySnapshot(1, clusterService.topologyService().allMembers()));
        }

        @Override
        public CompletableFuture<Set<ClusterNode>> validatedNodesOnLeader() {
            return completedFuture(Set.copyOf(clusterService.topologyService().allMembers()));
        }
    }
}
