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

import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZoneReplicas;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.DistributedConfigurationUpdater;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.SecurityConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
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
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.index.HashIndexConfigurationSchema;
import org.apache.ignite.internal.schema.configuration.storage.UnknownDataStorageConfigurationSchema;
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
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.util.ByteUtils;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * Test suite for rebalance process, when replicas' number changed.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItRebalanceDistributedTest {
    /** Ignite logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceDistributedTest.class);

    private static final String TABLE_1_NAME = "TBL1";

    private static final String ZONE_1_NAME = "zone1";

    public static final int BASE_PORT = 20_000;

    public static final String HOST = "localhost";

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static SecurityConfiguration securityConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

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

        for (int i = 0; i < 3; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);

        for (NetworkAddress addr : nodeAddresses) {
            var node = new Node(testInfo, addr);

            nodes.add(node);

            node.start();
        }

        nodes.get(0).cmgManager.initCluster(List.of(nodes.get(2).name), List.of(), "cluster");
    }

    @AfterEach
    void after() {
        for (Node node : nodes) {
            node.stop();
        }
    }

    @Test
    void testOneRebalance() throws Exception {
        int zoneId = createZone(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 1, 1).join();

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeZoneId(zoneId)));

        assertEquals(1, getPartitionClusterNodes(0, 0).size());

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getPartitionClusterNodes(0, 0).size());
        assertEquals(2, getPartitionClusterNodes(1, 0).size());
        assertEquals(2, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testTwoQueuedRebalances() {
        int zoneId = await(createZone(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 1, 1));

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeZoneId(zoneId)));

        assertEquals(1, getPartitionClusterNodes(0, 0).size());

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 2));
        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 3));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testThreeQueuedRebalances() throws Exception {
        int zoneId = await(createZone(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 1, 1));

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeZoneId(zoneId)));

        assertEquals(1, getPartitionClusterNodes(0, 0).size());

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 2));
        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 3));
        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 2));

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        assertEquals(2, getPartitionClusterNodes(0, 0).size());
        assertEquals(2, getPartitionClusterNodes(1, 0).size());
        assertEquals(2, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testOnLeaderElectedRebalanceRestart() throws Exception {
        String zoneName = "zone2";

        int zoneId = await(createZone(nodes.get(0).distributionZoneManager, zoneName, 1, 2));

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "TBL1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        TableImpl table = (TableImpl) await(nodes.get(1).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeZoneId(zoneId)));

        Set<String> partitionNodesConsistentIds = getPartitionClusterNodes(0, 0).stream()
                .map(Assignment::consistentId)
                .collect(Collectors.toSet());

        Node newNode = nodes.stream().filter(n -> !partitionNodesConsistentIds.contains(n.name)).findFirst().orElseThrow();

        Node leaderNode = findNodeByConsistentId(table.leaderAssignment(0).name());

        String nonLeaderNodeConsistentId = partitionNodesConsistentIds.stream()
                .filter(n -> !n.equals(leaderNode.name))
                .findFirst()
                .orElseThrow();

        TableImpl nonLeaderTable = (TableImpl) findNodeByConsistentId(nonLeaderNodeConsistentId).tableManager.table("TBL1");

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

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, zoneName, 3));

        countDownLatch.await();

        nonLeaderTable.internalTable().partitionRaftGroupService(0).transferLeadership(new Peer(nonLeaderNodeConsistentId)).get();

        ((JraftServerImpl) leaderNode.raftManager.server()).stopBlockMessages(partitionNodeId);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    void testRebalanceRetryWhenCatchupFailed() throws Exception {
        int zoneId = await(createZone(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 1, 1));

        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "tbl1").columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();

        await(nodes.get(0).tableManager.createTableAsync(
                "TBL1",
                tblChanger -> SchemaConfigurationConverter.convert(schTbl1, tblChanger)
                        .changeZoneId(zoneId)));

        assertEquals(1, getPartitionClusterNodes(0, 0).size());

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 1));

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        JraftServerImpl raftServer = (JraftServerImpl) nodes.stream()
                .filter(n -> n.raftManager.localNodes().stream().anyMatch(grp -> grp.toString().contains("_part_"))).findFirst()
                .get().raftManager.server();

        AtomicInteger counter = new AtomicInteger(0);

        RaftNodeId partitionNodeId = raftServer.localNodes().stream()
                .filter(grp -> grp.toString().contains("_part_"))
                .findFirst()
                .orElseThrow();

        raftServer.blockMessages(partitionNodeId, (msg, peerId) -> {
            if (msg instanceof RpcRequests.PingRequest) {
                // We block ping request to prevent starting replicator, hence we fail catch up and fail rebalance.
                assertEquals(1, getPartitionClusterNodes(0, 0).size());
                assertEquals(1, getPartitionClusterNodes(1, 0).size());
                assertEquals(1, getPartitionClusterNodes(2, 0).size());
                return counter.incrementAndGet() <= 5;
            }
            return false;
        });

        await(alterZoneReplicas(nodes.get(0).distributionZoneManager, ZONE_1_NAME, 3));

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        assertEquals(3, getPartitionClusterNodes(0, 0).size());
        assertEquals(3, getPartitionClusterNodes(1, 0).size());
        assertEquals(3, getPartitionClusterNodes(2, 0).size());
    }

    @Test
    @UseTestTxStateStorage
    void testDestroyPartitionStoragesOnEvictNode() {
        createTableWithOnePartition(TABLE_1_NAME, ZONE_1_NAME, 3, true);

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionClusterNodes(0, 0);

        nodes.forEach(node -> prepareFinishHandleChangeStableAssignmentEventFuture(node, TABLE_1_NAME, 0));

        changeTableReplicasForSinglePartition(ZONE_1_NAME, 2);

        Set<Assignment> assignmentsAfterChangeReplicas = getPartitionClusterNodes(0, 0);

        Set<Assignment> evictedAssignments = getEvictedAssignments(assignmentsBeforeChangeReplicas, assignmentsAfterChangeReplicas);

        assertThat(
                String.format("before=%s, after=%s", assignmentsBeforeChangeReplicas, assignmentsAfterChangeReplicas),
                evictedAssignments,
                hasSize(1)
        );

        assertThat(collectFinishHandleChangeStableAssignmentEventFuture(null, TABLE_1_NAME, 0), willCompleteSuccessfully());

        Node evictedNode = findNodeByConsistentId(first(evictedAssignments).consistentId());

        assertNotNull(evictedNode, evictedAssignments.toString());

        checkInvokeDestroyedPartitionStorages(evictedNode, TABLE_1_NAME, 0);
    }

    @Test
    @UseTestTxStateStorage
    @UseRocksMetaStorage
    void testDestroyPartitionStoragesOnRestartEvictedNode(TestInfo testInfo) throws Exception {
        createTableWithOnePartition(TABLE_1_NAME, ZONE_1_NAME, 3, true);

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionClusterNodes(0, 0);

        nodes.forEach(node -> {
            prepareFinishHandleChangeStableAssignmentEventFuture(node, TABLE_1_NAME, 0);

            throwExceptionOnInvokeDestroyPartitionStorages(node, TABLE_1_NAME, 0);
        });

        changeTableReplicasForSinglePartition(ZONE_1_NAME, 2);

        Assignment evictedAssignment = first(getEvictedAssignments(assignmentsBeforeChangeReplicas, getPartitionClusterNodes(0, 0)));

        Node evictedNode = findNodeByConsistentId(evictedAssignment.consistentId());

        // Let's make sure that we handled the events (STABLE_ASSIGNMENTS_PREFIX) from the metastore correctly.
        assertThat(
                collectFinishHandleChangeStableAssignmentEventFuture(node -> !node.equals(evictedNode), TABLE_1_NAME, 0),
                willCompleteSuccessfully()
        );

        TablePartitionId tablePartitionId = evictedNode.getTablePartitionId(TABLE_1_NAME, 0);

        assertThat(evictedNode.finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId), willThrowFast(Exception.class));

        // Restart evicted node.
        int evictedNodeIndex = findNodeIndexByConsistentId(evictedAssignment.consistentId());

        evictedNode.stop();

        Node newNode = new Node(testInfo, evictedNode.networkAddress);

        newNode.finishHandleChangeStableAssignmentEventFutures.put(tablePartitionId, new CompletableFuture<>());

        newNode.start();

        nodes.set(evictedNodeIndex, newNode);

        // Let's make sure that we will destroy the partition again.
        assertThat(newNode.finishHandleChangeStableAssignmentEventFutures.get(tablePartitionId), willSucceedIn(1, TimeUnit.MINUTES));

        checkInvokeDestroyedPartitionStorages(newNode, TABLE_1_NAME, 0);
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) {
        while (!IntStream.range(0, nodes.size()).allMatch(n -> getPartitionClusterNodes(n, partNum).size() == replicasNum)) {
            LockSupport.parkNanos(100_000_000);
        }
    }

    private Node findNodeByConsistentId(String consistentId) {
        return nodes.stream().filter(n -> n.name.equals(consistentId)).findFirst().orElseThrow();
    }

    private int findNodeIndexByConsistentId(String consistentId) {
        return IntStream.range(0, nodes.size()).filter(i -> nodes.get(i).name.equals(consistentId)).findFirst().orElseThrow();
    }

    private Set<Assignment> getPartitionClusterNodes(int nodeNum, int partNum) {
        var table = ((ExtendedTableConfiguration) nodes.get(nodeNum).clusterCfgMgr.configurationRegistry()
                .getConfiguration(TablesConfiguration.KEY).tables().get("TBL1"));

        if (table != null) {
            byte[] assignments = table.assignments().value();

            if (assignments != null) {
                return ((List<Set<Assignment>>) ByteUtils.fromBytes(assignments)).get(partNum);
            }
        }

        return Set.of();
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

        private final DistributedConfigurationUpdater distributedConfigurationUpdater;

        private List<IgniteComponent> nodeComponents;

        private final Map<TablePartitionId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
                = new ConcurrentHashMap<>();

        private final NetworkAddress networkAddress;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {
            networkAddress = addr;

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(name, dir);

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY),
                    Set.of(),
                    new LocalFileConfigurationStorage(configPath, List.of(NetworkConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY)),
                    List.of(),
                    List.of()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            raftManager = new Loza(clusterService, raftConfiguration, dir, new HybridClockImpl());

            replicaManager = new ReplicaManager(
                    clusterService,
                    new HybridClockImpl(),
                    Set.of(TableMessageGroup.class, TxMessageGroup.class)
            );

            HybridClock hybridClock = new HybridClockImpl();

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock
            );

            txManager = new TxManagerImpl(replicaSvc, lockManager, hybridClock);

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            distributedConfigurationUpdater = new DistributedConfigurationUpdater();
            distributedConfigurationUpdater.setClusterRestConfiguration(securityConfiguration);

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    distributedConfigurationUpdater,
                    nodeAttributes
            );

            String nodeName = clusterService.nodeName();

            metaStorageManager = new MetaStorageManagerImpl(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    new LogicalTopologyServiceImpl(logicalTopology, cmgManager),
                    raftManager,
                    testInfo.getTestMethod().get().isAnnotationPresent(UseRocksMetaStorage.class)
                            ? new RocksDbKeyValueStorage(nodeName, resolveDir(dir, "metaStorage"))
                            : new SimpleInMemoryKeyValueStorage(nodeName)
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(
                            PersistentPageMemoryStorageEngineConfiguration.KEY,
                            VolatilePageMemoryStorageEngineConfiguration.KEY,
                            TablesConfiguration.KEY,
                            DistributionZonesConfiguration.KEY
                    ),
                    Set.of(),
                    cfgStorage,
                    List.of(ExtendedTableConfigurationSchema.class),
                    List.of(
                            UnknownDataStorageConfigurationSchema.class,
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

            Consumer<Function<Long, CompletableFuture<?>>> registry = (Function<Long, CompletableFuture<?>> function) ->
                    clusterCfgMgr.configurationRegistry().listenUpdateStorageRevision(function::apply);

            TablesConfiguration tablesCfg = clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

            DistributionZonesConfiguration zonesCfg =
                    clusterCfgMgr.configurationRegistry().getConfiguration(DistributionZonesConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new PersistentPageMemoryDataStorageModule(),
                    new VolatilePageMemoryDataStorageModule(),
                    new TestDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");

            dataStorageMgr = new DataStorageManager(
                    tablesCfg,
                    dataStorageModules.createStorageEngines(
                            name,
                            clusterCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null));

            baselineMgr = new BaselineManager(
                    clusterCfgMgr,
                    metaStorageManager,
                    clusterService);

            schemaManager = new SchemaManager(registry, tablesCfg, metaStorageManager);

            LogicalTopologyService logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    new RaftGroupEventsClientListener()
            );

            distributionZoneManager = new DistributionZoneManager(
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
                    clusterService,
                    raftManager,
                    Mockito.mock(ReplicaManager.class),
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
                    topologyAwareRaftGroupServiceFactory
            ) {
                @Override
                protected TxStateTableStorage createTxStateTableStorage(TableConfiguration tableCfg,
                        DistributionZoneConfiguration   distributionZoneCfg) {
                    return testInfo.getTestMethod().get().isAnnotationPresent(UseTestTxStateStorage.class)
                            ? spy(new TestTxStateTableStorage())
                            : super.createTxStateTableStorage(tableCfg, distributionZoneCfg);
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
        void start() throws Exception {
            nodeComponents = List.of(
                    vaultManager,
                    nodeCfgMgr,
                    clusterService,
                    raftManager,
                    cmgManager,
                    metaStorageManager,
                    clusterCfgMgr,
                    distributionZoneManager,
                    replicaManager,
                    txManager,
                    baselineMgr,
                    dataStorageMgr,
                    schemaManager,
                    tableManager,
                    distributedConfigurationUpdater
            );

            nodeComponents.forEach(IgniteComponent::start);

            assertThat(
                    CompletableFuture.allOf(
                            nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners()
                    ),
                    willSucceedIn(1, TimeUnit.MINUTES)
            );

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();
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

        }

        NetworkAddress address() {
            return clusterService.topologyService().localMember().address();
        }

        @Nullable TablePartitionId getTablePartitionId(WatchEvent event) {
            assertTrue(event.single(), event.toString());

            Entry stableAssignmentsWatchEvent = event.entryEvent().newEntry();

            if (stableAssignmentsWatchEvent.value() == null) {
                return null;
            }

            int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
            UUID tableId = extractTableId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

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

    private static TableDefinition createTableDefinition(String tableName) {
        return SchemaBuilders.tableBuilder("PUBLIC", tableName).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT32).asNullable(true).build()
        ).withPrimaryKey("key").build();
    }

    private void createTableWithOnePartition(String tableName, String zoneName, int replicas, boolean testDataStorage) {
        int zoneId = await(createZone(nodes.get(0).distributionZoneManager, zoneName, 1, replicas));
        assertThat(
                nodes.get(0).tableManager.createTableAsync(
                        tableName,
                        tableChange -> {
                            SchemaConfigurationConverter.convert(createTableDefinition(tableName), tableChange)
                                    .changeZoneId(zoneId);

                            if (testDataStorage) {
                                tableChange.changeDataStorage(dataStorageChange -> dataStorageChange.convert(TestDataStorageChange.class));
                            }
                        }
                ),
                willCompleteSuccessfully()
        );

        assertEquals(replicas, getPartitionClusterNodes(0, 0).size());
        assertEquals(replicas, getPartitionClusterNodes(1, 0).size());
        assertEquals(replicas, getPartitionClusterNodes(2, 0).size());
    }

    private void changeTableReplicasForSinglePartition(String zoneName, int replicas) {
        assertThat(
                alterZoneReplicas(nodes.get(0).distributionZoneManager, zoneName, replicas),
                willCompleteSuccessfully()
        );

        waitPartitionAssignmentsSyncedToExpected(0, replicas);

        assertEquals(replicas, getPartitionClusterNodes(0, 0).size());
        assertEquals(replicas, getPartitionClusterNodes(1, 0).size());
        assertEquals(replicas, getPartitionClusterNodes(2, 0).size());
    }

    private static Set<Assignment> getEvictedAssignments(Set<Assignment> beforeChange, Set<Assignment> afterChange) {
        Set<Assignment> result = new HashSet<>(beforeChange);

        result.removeAll(afterChange);

        return result;
    }

    private static @Nullable InternalTable getInternalTable(Node node, String tableName) {
        Table table = node.tableManager.table(tableName);

        assertNotNull(table, tableName);

        return ((TableImpl) table).internalTable();
    }

    private static void checkInvokeDestroyedPartitionStorages(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), atLeast(1)).destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), atLeast(1)).destroyTxStateStorage(partitionId);
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

        return CompletableFuture.allOf(futures.toArray(CompletableFuture<?>[]::new));
    }
}
