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

package org.apache.ignite.internal.configuration.storage;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZoneReplicas;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.utils.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.utils.RebalanceUtil.extractTableId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.HashIndexConfigurationSchema;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.impl.TestDataStorageModule;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageChange;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.table.Table;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Test suite for rebalance process, when replicas' number changed.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
@Timeout(120)
public class ItRebalanceDistributedTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceDistributedTest.class);

    private static final String TABLE_NAME = "TBL1";

    private static final String ZONE_NAME = "zone1";

    private static final int BASE_PORT = 20_000;

    private static final String HOST = "localhost";

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    private static final int NODE_COUNT = 3;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface UseTestTxStateStorage {
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface UseRocksMetaStorage {
    }

    @WorkDirectory
    private Path workDir;

    private StaticNodeFinder finder;

    private List<Node> nodes;

    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        nodes = new ArrayList<>();

        List<NetworkAddress> nodeAddresses = new ArrayList<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);

        for (NetworkAddress addr : nodeAddresses) {
            var node = new Node(testInfo, addr);

            nodes.add(node);

            node.start();
        }

        Node node0 = getNode(0);
        Node node2 = getNode(2);

        node0.cmgManager.initCluster(List.of(node2.name), List.of(node2.name), "cluster");

        nodes.forEach(Node::waitWatches);

        assertThat(
                allOf(nodes.stream().map(n -> n.cmgManager.onJoinReady()).toArray(CompletableFuture[]::new)),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(
                () -> {
                    CompletableFuture<LogicalTopologySnapshot> logicalTopologyFuture = node0.cmgManager.logicalTopology();

                    assertThat(logicalTopologyFuture, willCompleteSuccessfully());

                    return logicalTopologyFuture.join().nodes().size() == NODE_COUNT;
                },
                10_000
        ));
    }

    @AfterEach
    void after() {
        nodes.forEach(Node::stop);
    }

    @Test
    void testOneRebalance() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionClusterNodes(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        checkPartitionNodes(0, 2);
    }

    @Test
    void testTwoQueuedRebalances() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionClusterNodes(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 2);
        alterZone(node, ZONE_NAME, 3);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        checkPartitionNodes(0, 3);
    }

    @Test
    void testThreeQueuedRebalances() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionClusterNodes(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 2);
        alterZone(node, ZONE_NAME, 3);
        alterZone(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        checkPartitionNodes(0, 2);
    }

    @Test
    void testOnLeaderElectedRebalanceRestart() throws Exception {
        Node node0 = getNode(0);
        Node node1 = getNode(1);

        String zoneName = "zone2";

        createZone(node0, zoneName, 1, 2);

        // Tests that the distribution zone created on node0 is available on node1.
        createTable(node1, zoneName, TABLE_NAME);

        InternalTable table = getInternalTable(node1, TABLE_NAME);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        Set<String> partitionNodesConsistentIds = getPartitionClusterNodes(node0, 0).stream()
                .map(Assignment::consistentId)
                .collect(toSet());

        Node newNode = nodes.stream().filter(n -> !partitionNodesConsistentIds.contains(n.name)).findFirst().orElseThrow();

        Node leaderNode = findNodeByConsistentId(table.leaderAssignment(0).name());

        String nonLeaderNodeConsistentId = partitionNodesConsistentIds.stream()
                .filter(n -> !n.equals(leaderNode.name))
                .findFirst()
                .orElseThrow();

        TableImpl nonLeaderTable = (TableImpl) findNodeByConsistentId(nonLeaderNodeConsistentId).tableManager.table(TABLE_NAME);

        var countDownLatch = new CountDownLatch(1);

        RaftNodeId partitionNodeId = leaderNode.raftManager.server()
                .localNodes()
                .stream()
                .filter(nodeId -> nodeId.groupId().toString().contains("part"))
                .findFirst()
                .orElseThrow();

        ((JraftServerImpl) leaderNode.raftManager.server()).blockMessages(
                partitionNodeId, (msg, peerId) -> {
                    if (peerId.equals(newNode.name) && msg instanceof RpcRequests.PingRequest) {
                        countDownLatch.countDown();

                        return true;
                    }
                    return false;
                });

        alterZone(node0, zoneName, 3);

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));

        assertThat(
                nonLeaderTable.internalTable().partitionRaftGroupService(0).transferLeadership(new Peer(nonLeaderNodeConsistentId)),
                willCompleteSuccessfully()
        );

        ((JraftServerImpl) leaderNode.raftManager.server()).stopBlockMessages(partitionNodeId);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        checkPartitionNodes(0, 3);
    }

    @Test
    void testRebalanceRetryWhenCatchupFailed() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionClusterNodes(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 1);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        JraftServerImpl raftServer = (JraftServerImpl) nodes.stream()
                .filter(n -> n.raftManager.localNodes().stream().anyMatch(grp -> grp.toString().contains("_part_")))
                .findFirst()
                .get().raftManager.server();

        AtomicInteger counter = new AtomicInteger(0);

        RaftNodeId partitionNodeId = raftServer.localNodes().stream()
                .filter(grp -> grp.toString().contains("_part_"))
                .findFirst()
                .orElseThrow();

        raftServer.blockMessages(partitionNodeId, (msg, peerId) -> {
            if (msg instanceof RpcRequests.PingRequest) {
                // We block ping request to prevent starting replicator, hence we fail catch up and fail rebalance.
                checkPartitionNodes(0, 1);

                return counter.incrementAndGet() <= 5;
            }
            return false;
        });

        alterZone(node, ZONE_NAME, 3);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        checkPartitionNodes(0, 3);
    }

    @Test
    @UseTestTxStateStorage
    void testDestroyPartitionStoragesOnEvictNode() throws Exception {
        Node node = getNode(0);

        createTableWithOnePartition(node, TABLE_NAME, ZONE_NAME, 3, true);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionClusterNodes(node, 0);

        changeTableReplicasForSinglePartition(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        Set<Assignment> assignmentsAfterChangeReplicas = getPartitionClusterNodes(node, 0);

        Set<Assignment> evictedAssignments = getEvictedAssignments(assignmentsBeforeChangeReplicas, assignmentsAfterChangeReplicas);

        assertThat(
                String.format("before=%s, after=%s", assignmentsBeforeChangeReplicas, assignmentsAfterChangeReplicas),
                evictedAssignments,
                hasSize(1)
        );

        Node evictedNode = findNodeByConsistentId(first(evictedAssignments).consistentId());

        assertNotNull(evictedNode, evictedAssignments.toString());

        checkInvokeDestroyedPartitionStorages(evictedNode, TABLE_NAME, 0);
    }

    @Test
    @UseTestTxStateStorage
    @UseRocksMetaStorage
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20187")
    void testDestroyPartitionStoragesOnRestartEvictedNode(TestInfo testInfo) throws Exception {
        Node node = getNode(0);

        createTableWithOnePartition(node, TABLE_NAME, ZONE_NAME, 3, true);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionClusterNodes(node, 0);

        nodes.forEach(n -> {
            prepareFinishHandleChangeStableAssignmentEventFuture(n, TABLE_NAME, 0);

            throwExceptionOnInvokeDestroyPartitionStorages(n, TABLE_NAME, 0);
        });

        changeTableReplicasForSinglePartition(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        Assignment evictedAssignment = first(getEvictedAssignments(assignmentsBeforeChangeReplicas, getPartitionClusterNodes(node, 0)));

        Node evictedNode = findNodeByConsistentId(evictedAssignment.consistentId());

        // Let's make sure that we handled the events (STABLE_ASSIGNMENTS_PREFIX) from the metastore correctly.
        assertThat(
                collectFinishHandleChangeStableAssignmentEventFuture(n -> !n.equals(evictedNode), TABLE_NAME, 0),
                willCompleteSuccessfully()
        );

        TablePartitionId tablePartitionId = evictedNode.getTablePartitionId(TABLE_NAME, 0);

        assertThat(evictedNode.finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId), willThrowFast(Exception.class));

        // Restart evicted node.
        int evictedNodeIndex = findNodeIndexByConsistentId(evictedAssignment.consistentId());

        evictedNode.stop();

        Node newNode = new Node(testInfo, evictedNode.networkAddress);

        newNode.finishHandleChangeStableAssignmentEventFutures.put(tablePartitionId, new CompletableFuture<>());

        newNode.start();

        newNode.waitWatches();

        nodes.set(evictedNodeIndex, newNode);

        // Let's make sure that we will destroy the partition again.
        assertThat(newNode.finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId), willSucceedIn(1, TimeUnit.MINUTES));

        checkInvokeDestroyedPartitionStorages(newNode, TABLE_NAME, 0);
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionClusterNodes(n, partNum).size() == replicasNum),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    private Node findNodeByConsistentId(String consistentId) {
        return nodes.stream().filter(n -> n.name.equals(consistentId)).findFirst().orElseThrow();
    }

    private int findNodeIndexByConsistentId(String consistentId) {
        return IntStream.range(0, nodes.size()).filter(i -> getNode(i).name.equals(consistentId)).findFirst().orElseThrow();
    }

    private static Set<Assignment> getPartitionClusterNodes(Node node, int partNum) {
        return Optional.ofNullable(getTableId(node, TABLE_NAME))
                .map(tableId -> partitionAssignments(node.metaStorageManager, tableId, partNum).join())
                .orElse(Set.of());
    }

    private class Node {
        private final String name;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final Loza raftManager;

        private final ReplicaManager replicaManager;

        private final MetaStorageManager metaStorageManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TableManager tableManager;

        private final DistributionZoneManager distributionZoneManager;

        private final BaselineManager baselineMgr;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        private final CatalogManager catalogManager;

        private final ClockWaiter clockWaiter;

        private List<IgniteComponent> nodeComponents;

        private final ConfigurationTreeGenerator nodeCfgGenerator;

        private final ConfigurationTreeGenerator clusterCfgGenerator;

        private final Map<TablePartitionId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
                = new ConcurrentHashMap<>();

        private final NetworkAddress networkAddress;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private CompletableFuture<Void> deployWatchesFut;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {
            networkAddress = addr;

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(name, dir);

            nodeCfgGenerator = new ConfigurationTreeGenerator(
                    NetworkConfiguration.KEY, RestConfiguration.KEY, ClientConnectorConfiguration.KEY
            );

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY),
                    new LocalFileConfigurationStorage(configPath, nodeCfgGenerator),
                    nodeCfgGenerator,
                    new TestConfigurationValidator()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            HybridClock hybridClock = new HybridClockImpl();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            raftManager = new Loza(clusterService, raftConfiguration, dir, hybridClock, raftGroupEventsClientListener);

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    nodeAttributes,
                    new TestConfigurationValidator());

            replicaManager = new ReplicaManager(
                    name,
                    clusterService,
                    cmgManager,
                    hybridClock,
                    Set.of(TableMessageGroup.class, TxMessageGroup.class)
            );

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock
            );

            txManager = new TxManagerImpl(replicaSvc, lockManager, hybridClock, new TransactionIdGenerator(addr.port()));

            String nodeName = clusterService.nodeName();

            LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            KeyValueStorage keyValueStorage = testInfo.getTestMethod().get().isAnnotationPresent(UseRocksMetaStorage.class)
                    ? new RocksDbKeyValueStorage(nodeName, resolveDir(dir, "metaStorage"))
                    : new SimpleInMemoryKeyValueStorage(nodeName);

            metaStorageManager = new MetaStorageManagerImpl(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    keyValueStorage,
                    hybridClock,
                    topologyAwareRaftGroupServiceFactory,
                    metaStorageConfiguration
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager);

            clusterCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(
                            PersistentPageMemoryStorageEngineConfiguration.KEY,
                            VolatilePageMemoryStorageEngineConfiguration.KEY,
                            TablesConfiguration.KEY,
                            DistributionZonesConfiguration.KEY,
                            GcConfiguration.KEY
                    ),
                    List.of(ExtendedTableConfigurationSchema.class),
                    List.of(
                            VolatilePageMemoryDataStorageConfigurationSchema.class,
                            UnsafeMemoryAllocatorConfigurationSchema.class,
                            PersistentPageMemoryDataStorageConfigurationSchema.class,
                            HashIndexConfigurationSchema.class,
                            ConstantValueDefaultConfigurationSchema.class,
                            FunctionCallDefaultConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class,
                            TestDataStorageConfigurationSchema.class
                    )
            );
            clusterCfgMgr = new ConfigurationManager(
                    List.of(
                            PersistentPageMemoryStorageEngineConfiguration.KEY,
                            VolatilePageMemoryStorageEngineConfiguration.KEY,
                            TablesConfiguration.KEY,
                            DistributionZonesConfiguration.KEY,
                            GcConfiguration.KEY
                    ),
                    cfgStorage,
                    clusterCfgGenerator,
                    new TestConfigurationValidator()
            );

            ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

            Consumer<LongFunction<CompletableFuture<?>>> registry = (LongFunction<CompletableFuture<?>> function) ->
                    metaStorageManager.registerRevisionUpdateListener(function::apply);

            TablesConfiguration tablesCfg = clusterConfigRegistry.getConfiguration(TablesConfiguration.KEY);

            DistributionZonesConfiguration zonesCfg = clusterConfigRegistry.getConfiguration(DistributionZonesConfiguration.KEY);

            GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new PersistentPageMemoryDataStorageModule(),
                    new VolatilePageMemoryDataStorageModule(),
                    new TestDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");

            dataStorageMgr = new DataStorageManager(
                    zonesCfg,
                    dataStorageModules.createStorageEngines(
                            name,
                            clusterConfigRegistry,
                            dir.resolve("storage"),
                            null));

            baselineMgr = new BaselineManager(
                    clusterCfgMgr,
                    metaStorageManager,
                    clusterService);

            clockWaiter = new ClockWaiter("test", hybridClock);

            catalogManager = new CatalogManagerImpl(
                    new UpdateLogImpl(metaStorageManager),
                    clockWaiter
            );

            schemaManager = new SchemaManager(registry, tablesCfg, metaStorageManager);

            distributionZoneManager = new DistributionZoneManager(
                    registry,
                    zonesCfg,
                    tablesCfg,
                    metaStorageManager,
                    logicalTopologyService,
                    vaultManager,
                    name
            );


            tableManager = new TableManager(
                    name,
                    registry,
                    tablesCfg,
                    zonesCfg,
                    gcConfig,
                    clusterService,
                    raftManager,
                    replicaManager,
                    Mockito.mock(LockManager.class),
                    replicaSvc,
                    baselineMgr,
                    clusterService.topologyService(),
                    txManager,
                    dataStorageMgr,
                    storagePath,
                    metaStorageManager,
                    schemaManager,
                    view -> new LocalLogStorageFactory(),
                    new HybridClockImpl(),
                    new OutgoingSnapshotsManager(clusterService.messagingService()),
                    topologyAwareRaftGroupServiceFactory,
                    vaultManager,
                    cmgManager,
                    distributionZoneManager
            ) {
                @Override
                protected TxStateTableStorage createTxStateTableStorage(
                        CatalogTableDescriptor tableDescriptor,
                        CatalogZoneDescriptor zoneDescriptor
                ) {
                    return testInfo.getTestMethod().get().isAnnotationPresent(UseTestTxStateStorage.class)
                            ? spy(new TestTxStateTableStorage())
                            : super.createTxStateTableStorage(tableDescriptor, zoneDescriptor);
                }

                @Override
                protected CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
                    TablePartitionId tablePartitionId = getTablePartitionId(evt);

                    return super.handleChangeStableAssignmentEvent(evt)
                            .whenComplete((v, e) -> {
                                if (tablePartitionId == null) {
                                    return;
                                }

                                CompletableFuture<Void> finishFuture = finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId);

                                if (finishFuture == null) {
                                    return;
                                }

                                if (e == null) {
                                    finishFuture.complete(null);
                                } else {
                                    finishFuture.completeExceptionally(e);
                                }
                            });
                }
            };
        }

        /**
         * Starts the created components.
         */
        void start() {
            nodeComponents = new CopyOnWriteArrayList<>();

            List<IgniteComponent> firstComponents = List.of(
                    vaultManager,
                    nodeCfgMgr,
                    clusterService,
                    raftManager,
                    cmgManager
            );

            firstComponents.forEach(IgniteComponent::start);

            nodeComponents.addAll(firstComponents);

            deployWatchesFut = CompletableFuture.supplyAsync(() -> {
                List<IgniteComponent> secondComponents = List.of(
                        metaStorageManager,
                        clusterCfgMgr,
                        clockWaiter,
                        catalogManager,
                        distributionZoneManager,
                        replicaManager,
                        txManager,
                        baselineMgr,
                        dataStorageMgr,
                        schemaManager,
                        tableManager
                );

                secondComponents.forEach(IgniteComponent::start);

                nodeComponents.addAll(secondComponents);

                var configurationNotificationFut = metaStorageManager.recoveryFinishedFuture().thenCompose(rev -> {
                    return allOf(
                            nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart()
                    );
                });

                assertThat(configurationNotificationFut, willSucceedIn(1, TimeUnit.MINUTES));

                return metaStorageManager.deployWatches();
            }).thenCompose(identity());
        }

        /**
         * Waits for watches deployed.
         */
        void waitWatches() {
            assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
        }

        /**
         * Stops the created components.
         */
        void stop() {
            new ReverseIterator<>(nodeComponents).forEachRemaining(component -> {
                try {
                    component.beforeNodeStop();
                } catch (Exception e) {
                    LOG.error("Unable to execute before node stop [component={}]", e, component);
                }
            });

            new ReverseIterator<>(nodeComponents).forEachRemaining(component -> {
                try {
                    component.stop();
                } catch (Exception e) {
                    LOG.error("Unable to stop component [component={}]", e, component);
                }
            });


            nodeCfgGenerator.close();
            clusterCfgGenerator.close();
        }

        @Nullable TablePartitionId getTablePartitionId(WatchEvent event) {
            assertTrue(event.single(), event.toString());

            Entry stableAssignmentsWatchEvent = event.entryEvent().newEntry();

            if (stableAssignmentsWatchEvent.value() == null) {
                return null;
            }

            int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
            int tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

            return new TablePartitionId(tableId, partitionId);
        }

        TablePartitionId getTablePartitionId(String tableName, int partitionId) {
            InternalTable internalTable = getInternalTable(this, tableName);

            return new TablePartitionId(internalTable.tableId(), partitionId);
        }
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(String nodeName, Path workDir) {
        return new VaultManager(new PersistentVaultService(nodeName, resolveDir(workDir, "vault")));
    }

    private static Path resolveDir(Path workDir, String dirName) {
        Path newDirPath = workDir.resolve(dirName);

        try {
            return Files.createDirectories(newDirPath);
        } catch (IOException e) {
            throw new IgniteInternalException(e);
        }
    }

    private void createTableWithOnePartition(
            Node node,
            String tableName,
            String zoneName,
            int replicas,
            boolean testDataStorage
    ) throws Exception {
        createZone(node, zoneName, 1, replicas, testDataStorage);

        createTable(node, zoneName, tableName);

        waitPartitionAssignmentsSyncedToExpected(0, replicas);

        checkPartitionNodes(0, replicas);
    }

    private void changeTableReplicasForSinglePartition(Node node, String zoneName, int replicas) throws Exception {
        alterZone(node, zoneName, replicas);

        waitPartitionAssignmentsSyncedToExpected(0, replicas);

        checkPartitionNodes(0, replicas);
    }

    private static Set<Assignment> getEvictedAssignments(Set<Assignment> beforeChange, Set<Assignment> afterChange) {
        Set<Assignment> result = new HashSet<>(beforeChange);

        result.removeAll(afterChange);

        return result;
    }

    private static InternalTable getInternalTable(Node node, String tableName) {
        Table table = node.tableManager.table(tableName);

        assertNotNull(table, tableName);

        return ((TableImpl) table).internalTable();
    }

    private static void checkInvokeDestroyedPartitionStorages(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyTxStateStorage(partitionId);
    }

    private static void throwExceptionOnInvokeDestroyPartitionStorages(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        doAnswer(answer -> CompletableFuture.failedFuture(new StorageException("From test")))
                .when(internalTable.storage())
                .destroyPartition(partitionId);

        doAnswer(answer -> CompletableFuture.failedFuture(new IgniteInternalException("From test")))
                .when(internalTable.txStateStorage())
                .destroyTxStateStorage(partitionId);
    }

    private void prepareFinishHandleChangeStableAssignmentEventFuture(Node node, String tableName, int partitionId) {
        TablePartitionId tablePartitionId = new TablePartitionId(getInternalTable(node, tableName).tableId(), partitionId);

        node.finishHandleChangeStableAssignmentEventFutures.put(tablePartitionId, new CompletableFuture<>());
    }

    private CompletableFuture<?> collectFinishHandleChangeStableAssignmentEventFuture(
            @Nullable Predicate<Node> nodeFilter,
            String tableName,
            int partitionId
    ) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Node node : nodes) {
            if (nodeFilter != null && !nodeFilter.test(node)) {
                continue;
            }

            TablePartitionId tablePartitionId = new TablePartitionId(getInternalTable(node, tableName).tableId(), partitionId);

            CompletableFuture<Void> future = node.finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId);

            assertNotNull(future, String.format("node=%s, table=%s, partitionId=%s", node.name, tableName, partitionId));

            futures.add(future);
        }

        assertThat(String.format("tableName=%s, partitionId=%s", tableName, partitionId), futures, not(empty()));

        return allOf(futures.toArray(CompletableFuture<?>[]::new));
    }

    private static void createZone(Node node, String zoneName, int partitions, int replicas) {
        createZone(node, zoneName, partitions, replicas, false);
    }

    private static void createZone(Node node, String zoneName, int partitions, int replicas, boolean testDataStorage) {
        DistributionZonesTestUtil.createZone(
                node.distributionZoneManager,
                zoneName,
                partitions,
                replicas,
                testDataStorage ? (dataStorageChange -> dataStorageChange.convert(TestDataStorageChange.class)) : null
        );
    }

    private static void alterZone(Node node, String zoneName, int replicas) {
        alterZoneReplicas(node.distributionZoneManager, zoneName, replicas);
    }

    private static void createTable(Node node, String zoneName, String tableName) {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, tableName).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        CompletableFuture<Table> createTableFuture = node.tableManager.createTableAsync(
                tableName,
                zoneName,
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
        );

        assertThat(createTableFuture, willCompleteSuccessfully());
    }

    private static @Nullable Integer getTableId(Node node, String tableName) {
        TableConfiguration tableConfig = node.clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables()
                .get(tableName);

        return tableConfig == null ? null : tableConfig.id().value();
    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    private void checkPartitionNodes(int partitionId, int expNodeCount) {
        for (Node node : nodes) {
            assertEquals(expNodeCount, getPartitionClusterNodes(node, partitionId).size(), node.name);
        }
    }
}
