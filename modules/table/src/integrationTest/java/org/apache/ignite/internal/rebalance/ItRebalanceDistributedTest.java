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

package org.apache.ignite.internal.rebalance;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTableId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.rest.configuration.RestConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.impl.TestDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableRaftService;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.ReverseIterator;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for rebalance process, when replicas' number changed.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
@Timeout(120)
public class ItRebalanceDistributedTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceDistributedTest.class);

    private static final String TABLE_NAME = "TBL1";

    private static final String TABLE_NAME_2 = "TBL2";

    private static final String TABLE_NAME_3 = "TBL3";

    private static final String ZONE_NAME = "zone1";

    private static final int BASE_PORT = 20_000;

    /** Filter to determine a primary node identically on any cluster node. */
    private static final Function<Collection<ClusterNode>, ClusterNode> PRIMARY_FILTER = nodes -> nodes.stream()
            .filter(n -> n.address().port() == BASE_PORT).findFirst().get();

    private static final String HOST = "localhost";

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    private static final int NODE_COUNT = 3;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = \"aipersist\", test.engine=\"test\"}")
    private static StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

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
                AWAIT_TIMEOUT_MILLIS
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
    void testOneRebalanceSeveralTables() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);
        createTable(node, ZONE_NAME, TABLE_NAME_2);
        createTable(node, ZONE_NAME, TABLE_NAME_3);

        assertTrue(waitForCondition(() -> getPartitionClusterNodes(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(TABLE_NAME, 0, 2);
        waitPartitionAssignmentsSyncedToExpected(TABLE_NAME_2, 0, 2);
        waitPartitionAssignmentsSyncedToExpected(TABLE_NAME_3, 0, 2);

        checkPartitionNodes(TABLE_NAME, 0, 2);
        checkPartitionNodes(TABLE_NAME_2, 0, 2);
        checkPartitionNodes(TABLE_NAME_3, 0, 2);
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

        Node leaderNode = findNodeByConsistentId(table.tableRaftService().leaderAssignment(0).name());

        String nonLeaderNodeConsistentId = partitionNodesConsistentIds.stream()
                .filter(n -> !n.equals(leaderNode.name))
                .findFirst()
                .orElseThrow();

        TableViewInternal nonLeaderTable =
                (TableViewInternal) findNodeByConsistentId(nonLeaderNodeConsistentId).tableManager.table(TABLE_NAME);

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

        assertTrue(countDownLatch.await(10, SECONDS));

        TableRaftService tableRaftService = nonLeaderTable.internalTable().tableRaftService();

        assertThat(
                tableRaftService.partitionRaftGroupService(0).transferLeadership(new Peer(nonLeaderNodeConsistentId)),
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19170")
    void testDestroyPartitionStoragesOnRestartEvictedNode(TestInfo testInfo) throws Exception {
        Node node = getNode(0);

        createTableWithOnePartition(node, TABLE_NAME, ZONE_NAME, 3, true);

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

    /**
     * Test checks rebances from [A,B,C] to [A,B] and then again to [A,B,C].
     * In this case the raft group node and {@link Replica} are started only once on each node.
     *
     * <p>1. We have an in-progress rebalance and current metastore keys:
     * ms.stable = a,b,c. ms.pending = a,b. ms.planned = a,b,c
     * so, the current active peers is the a,b,c.
     * 2. When the rebalance done, keys wil be updated:
     * ms.stable = a,b. ms.pending = a,b,c. ms.planned = empty
     * 3. Pending event handler receives the entry {old.pending = a,b; new.pending = a,b,c} and:
     * - it will receive the current stable a,b with the revision of current pending event
     * - compare it with new pending a,b,c and want to start node c
     * - but this node is still alive, because the stable handler is not stopped it yet (and we don't want to stop actually)
     * - so we don't need to start it again
     *
     * @throws Exception If failed.
     */
    @Test
    void testRebalanceWithTheSameNodes() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        clearSpyInvocations();

        createTable(node, ZONE_NAME, TABLE_NAME);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        alterZone(node, ZONE_NAME, 3);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        checkPartitionAssignmentsSyncedAndRebalanceKeysEmpty();

        directUpdateMetastoreRebalanceAssignmentKeys();

        checkPartitionAssignmentsSyncedAndRebalanceKeysEmpty();

        verifyThatRaftNodesAndReplicasWereStartedOnlyOnce();
    }

    @Test
    void testRaftClientsUpdatesAfterRebalance() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        Set<Assignment> assignmentsBeforeRebalance = getPartitionClusterNodes(node, 0);

        String newNodeNameForAssignment = nodes.stream()
                .map(n -> Assignment.forPeer(n.clusterService.nodeName()))
                .filter(assignment -> !assignmentsBeforeRebalance.contains(assignment))
                .findFirst()
                .orElseThrow()
                .consistentId();

        Set<Assignment> newAssignment = Set.of(Assignment.forPeer(newNodeNameForAssignment));

        // Write the new assignments to metastore as a pending assignments.
        {
            TablePartitionId partId = new TablePartitionId(getTableId(node, TABLE_NAME), 0);

            ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

            byte[] bytesPendingAssignments = Assignments.toBytes(newAssignment);

            node.metaStorageManager
                    .put(partAssignmentsPendingKey, bytesPendingAssignments)
                    .get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);
        }

        // Wait for rebalance to complete.
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionClusterNodes(n, 0).equals(newAssignment)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        // Check that raft clients on all nodes were updated with the new list of peers.
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n ->
                        n.tableManager
                                .startedTables()
                                .get(getTableId(node, TABLE_NAME))
                                .internalTable()
                                .tableRaftService()
                                .partitionRaftGroupService(0)
                                .peers()
                                .equals(List.of(new Peer(newNodeNameForAssignment)))),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

    }


    @Test
    void testClientsAreUpdatedAfterPendingRebalanceHandled() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        String assignmentsBeforeRebalance = getPartitionClusterNodes(node, 0).stream()
                .findFirst()
                .orElseThrow()
                .consistentId();

        String newNodeNameForAssignment = nodes.stream()
                .filter(n -> !assignmentsBeforeRebalance.equals(n.clusterService.nodeName()))
                .findFirst()
                .orElseThrow()
                .name;

        Set<Assignment> newAssignment = Set.of(Assignment.forPeer(newNodeNameForAssignment));

        // Write the new assignments to metastore as a pending assignments.
        TablePartitionId partId = new TablePartitionId(getTableId(node, TABLE_NAME), 0);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);

        byte[] bytesPendingAssignments = Assignments.toBytes(newAssignment);

        AtomicBoolean dropMessages = new AtomicBoolean(true);

        // Using this hack we pause rebalance on all nodes
        nodes.forEach(n -> ((DefaultMessagingService) n.clusterService.messagingService())
                .dropMessages((nodeName, msg) -> msg instanceof ChangePeersAsyncRequest && dropMessages.get())
        );

        node.metaStorageManager.put(partAssignmentsPendingKey, bytesPendingAssignments).get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);

        // Check that raft clients on all nodes were updated with the new list of peers.
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n ->
                        n.tableManager
                                .startedTables()
                                .get(getTableId(node, TABLE_NAME))
                                .internalTable()
                                .tableRaftService()
                                .partitionRaftGroupService(0)
                                .peers()
                                .stream()
                                .collect(toSet())
                                .equals(Set.of(new Peer(newNodeNameForAssignment), new Peer(assignmentsBeforeRebalance)))),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        dropMessages.set(false);
    }

    private void clearSpyInvocations() {
        for (int i = 0; i < NODE_COUNT; i++) {
            clearInvocations(getNode(i).raftManager);
            clearInvocations(getNode(i).replicaManager);
        }
    }

    private void checkPartitionAssignmentsSyncedAndRebalanceKeysEmpty() throws Exception {
        waitPartitionPlannedAssignmentsSyncedToExpected(0, 0);
        waitPartitionPendingAssignmentsSyncedToExpected(0, 0);
        waitPartitionAssignmentsSyncedToExpected(0, 3);
    }

    /**
     * Update pending and planned assignments to start a rebalance.
     *
     * @throws Exception If fail.
     */
    private void directUpdateMetastoreRebalanceAssignmentKeys() throws Exception {
        Collection<String> dataNodes = new HashSet<>();

        for (int i = 0; i < NODE_COUNT; i++) {
            dataNodes.add(getNode(i).name);
        }

        Set<Assignment> pendingAssignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, 0, 2);
        Set<Assignment> plannedAssignments = AffinityUtils.calculateAssignmentForPartition(dataNodes, 0, 3);

        byte[] bytesPendingAssignments = Assignments.toBytes(pendingAssignments);
        byte[] bytesPlannedAssignments = Assignments.toBytes(plannedAssignments);

        Node node0 = getNode(0);

        TablePartitionId partId = new TablePartitionId(getTableId(node0, TABLE_NAME), 0);

        ByteArray partAssignmentsPendingKey = pendingPartAssignmentsKey(partId);
        ByteArray partAssignmentsPlannedKey = plannedPartAssignmentsKey(partId);

        Map<ByteArray, byte[]> msEntries = new HashMap<>();

        msEntries.put(partAssignmentsPendingKey, bytesPendingAssignments);
        msEntries.put(partAssignmentsPlannedKey, bytesPlannedAssignments);

        node0.metaStorageManager.putAll(msEntries).get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);
    }

    private void verifyThatRaftNodesAndReplicasWereStartedOnlyOnce() throws Exception {
        for (int i = 0; i < NODE_COUNT; i++) {
            verify(getNode(i).raftManager, timeout(AWAIT_TIMEOUT_MILLIS).times(1))
                    .startRaftGroupNodeWithoutService(any(), any(), any(), any(), any(RaftGroupOptions.class));
            verify(getNode(i).replicaManager, timeout(AWAIT_TIMEOUT_MILLIS).times(1))
                    .startReplica(any(), any(ReplicaListener.class), any(), any());
        }
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        waitPartitionAssignmentsSyncedToExpected(TABLE_NAME, partNum, replicasNum);
    }

    private void waitPartitionAssignmentsSyncedToExpected(String tableName, int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionClusterNodes(n, tableName, partNum).size() == replicasNum),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        assertTrue(waitForCondition(
                () -> {
                    try {
                        return nodes.stream().allMatch(n ->
                                n.tableManager
                                        .cachedTable(getTableId(n, tableName))
                                        .internalTable()
                                        .tableRaftService()
                                        .partitionRaftGroupService(partNum) != null
                        );
                    } catch (IgniteInternalException e) {
                        // Raft group service not found.
                        return false;
                    }
                },
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    private void waitPartitionPendingAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionPendingClusterNodes(n, partNum).size() == replicasNum),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    private void waitPartitionPlannedAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionPlannedClusterNodes(n, partNum).size() == replicasNum),
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
        return getPartitionClusterNodes(node, TABLE_NAME, partNum);
    }

    private static Set<Assignment> getPartitionClusterNodes(Node node, String tableName, int partNum) {
        return Optional.ofNullable(getTableId(node, tableName))
                .map(tableId -> partitionAssignments(node.metaStorageManager, tableId, partNum).join())
                .orElse(Set.of());
    }

    private static Set<Assignment> getPartitionPendingClusterNodes(Node node, int partNum) {
        return Optional.ofNullable(getTableId(node, TABLE_NAME))
                .map(tableId -> partitionPendingAssignments(node.metaStorageManager, tableId, partNum).join())
                .orElse(Set.of());
    }

    private static Set<Assignment> getPartitionPlannedClusterNodes(Node node, int partNum) {
        return Optional.ofNullable(getTableId(node, TABLE_NAME))
                .map(tableId -> partitionPlannedAssignments(node.metaStorageManager, tableId, partNum).join())
                .orElse(Set.of());
    }

    private static CompletableFuture<Set<Assignment>> partitionPendingAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionNumber
    ) {
        return metaStorageManager
                .get(pendingPartAssignmentsKey(new TablePartitionId(tableId, partitionNumber)))
                .thenApply(e -> (e.value() == null) ? null : Assignments.fromBytes(e.value()).nodes());
    }

    private static CompletableFuture<Set<Assignment>> partitionPlannedAssignments(
            MetaStorageManager metaStorageManager,
            int tableId,
            int partitionNumber
    ) {
        return metaStorageManager
                .get(plannedPartAssignmentsKey(new TablePartitionId(tableId, partitionNumber)))
                .thenApply(e -> (e.value() == null) ? null : Assignments.fromBytes(e.value()).nodes());
    }

    private class Node {
        final String name;

        final Loza raftManager;

        final ThreadPoolsManager threadPoolsManager;

        final ReplicaManager replicaManager;

        final MetaStorageManager metaStorageManager;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final LockManager lockManager;

        private final TxManager txManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TableManager tableManager;

        private final DistributionZoneManager distributionZoneManager;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        private final CatalogManager catalogManager;

        private final SchemaSyncService schemaSyncService;

        private final ClockWaiter clockWaiter;

        private final List<IgniteComponent> nodeComponents = new CopyOnWriteArrayList<>();

        private final ConfigurationTreeGenerator nodeCfgGenerator;

        private final ConfigurationTreeGenerator clusterCfgGenerator;

        private final Map<TablePartitionId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
                = new ConcurrentHashMap<>();

        private final NetworkAddress networkAddress;

        private final LowWatermarkImpl lowWatermark;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private CompletableFuture<Void> deployWatchesFut;

        /** Hybrid clock. */
        private final HybridClock hybridClock = new HybridClockImpl();

        /** Index manager. */
        private final IndexManager indexManager;

        /** Failure processor. */
        private final FailureProcessor failureProcessor;

        private final ScheduledExecutorService rebalanceScheduler;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {
            networkAddress = addr;

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(dir);

            nodeCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(
                            NetworkConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY,
                            StorageConfiguration.KEY),
                    List.of(
                            PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                            VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class
                    ),
                    List.of(
                            PersistentPageMemoryProfileConfigurationSchema.class,
                            VolatilePageMemoryProfileConfigurationSchema.class,
                            UnsafeMemoryAllocatorConfigurationSchema.class
                    )
            );

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            TestIgnitionManager.addDefaultsToConfigurationFile(configPath);

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            StorageConfiguration.KEY,
                            RestConfiguration.KEY,
                            ClientConnectorConfiguration.KEY),
                    new LocalFileConfigurationStorage(configPath, nodeCfgGenerator, null),
                    nodeCfgGenerator,
                    new TestConfigurationValidator()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            raftManager = spy(new Loza(clusterService, raftConfiguration, dir, hybridClock, raftGroupEventsClientListener));

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration)
            );

            LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            KeyValueStorage keyValueStorage = testInfo.getTestMethod().get().isAnnotationPresent(UseRocksMetaStorage.class)
                    ? new RocksDbKeyValueStorage(name, resolveDir(dir, "metaStorage"), new NoOpFailureProcessor(name))
                    : new SimpleInMemoryKeyValueStorage(name);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    keyValueStorage,
                    hybridClock,
                    topologyAwareRaftGroupServiceFactory,
                    metaStorageConfiguration
            );

            var placementDriver = new TestPlacementDriver(() -> PRIMARY_FILTER.apply(clusterService.topologyService().allMembers()));

            threadPoolsManager = new ThreadPoolsManager(name);

            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock,
                    threadPoolsManager.partitionOperationsExecutor(),
                    replicationConfiguration
            );

            var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver);

            cfgStorage = new DistributedConfigurationStorage("test", metaStorageManager);

            clusterCfgGenerator = new ConfigurationTreeGenerator(GcConfiguration.KEY);

            clusterCfgMgr = new ConfigurationManager(
                    List.of(
                            GcConfiguration.KEY
                    ),
                    cfgStorage,
                    clusterCfgGenerator,
                    new TestConfigurationValidator()
            );

            ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

            Consumer<LongFunction<CompletableFuture<?>>> registry = (LongFunction<CompletableFuture<?>> function) ->
                    metaStorageManager.registerRevisionUpdateListener(function::apply);

            GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcConfiguration.KEY);

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new PersistentPageMemoryDataStorageModule(),
                    new VolatilePageMemoryDataStorageModule(),
                    new TestDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");

            failureProcessor = new FailureProcessor(name);

            dataStorageMgr = new DataStorageManager(
                    dataStorageModules.createStorageEngines(
                            name,
                            nodeCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null,
                            failureProcessor,
                            raftManager.getLogSyncer()
                    ),
                    storageConfiguration
            );

            clockWaiter = new ClockWaiter(name, hybridClock);

            ClockService clockService = new ClockServiceImpl(
                    hybridClock,
                    clockWaiter,
                    () -> TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS
            );

            lowWatermark = new LowWatermarkImpl(
                    name,
                    gcConfig.lowWatermark(),
                    clockService,
                    vaultManager,
                    failureProcessor,
                    clusterService.messagingService()
            );

            txManager = new TxManagerImpl(
                    txConfiguration,
                    clusterService,
                    replicaSvc,
                    lockManager,
                    clockService,
                    new TransactionIdGenerator(addr.port()),
                    placementDriver,
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new TestLocalRwTxCounter(),
                    resourcesRegistry,
                    transactionInflights,
                    lowWatermark
            );

            replicaManager = spy(new ReplicaManager(
                    name,
                    clusterService,
                    cmgManager,
                    clockService,
                    Set.of(TableMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    threadPoolsManager.partitionOperationsExecutor(),
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new NoOpFailureProcessor(),
                    new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()),
                    topologyAwareRaftGroupServiceFactory,
                    raftManager
            ));

            LongSupplier delayDurationMsSupplier = () -> 10L;

            catalogManager = new CatalogManagerImpl(
                    new UpdateLogImpl(metaStorageManager),
                    clockService,
                    delayDurationMsSupplier,
                    partitionIdleSafeTimePropagationPeriodMsSupplier
            );

            schemaManager = new SchemaManager(registry, catalogManager);

            schemaSyncService = new SchemaSyncServiceImpl(metaStorageManager.clusterTime(), delayDurationMsSupplier);

            rebalanceScheduler = new ScheduledThreadPoolExecutor(REBALANCE_SCHEDULER_POOL_SIZE,
                    NamedThreadFactory.create(name, "test-rebalance-scheduler", logger()));

            distributionZoneManager = new DistributionZoneManager(
                    name,
                    registry,
                    metaStorageManager,
                    logicalTopologyService,
                    catalogManager,
                    rebalanceScheduler
            );

            StorageUpdateConfiguration storageUpdateConfiguration = clusterConfigRegistry.getConfiguration(StorageUpdateConfiguration.KEY);

            HybridClockImpl clock = new HybridClockImpl();

            tableManager = new TableManager(
                    name,
                    registry,
                    gcConfig,
                    txConfiguration,
                    storageUpdateConfiguration,
                    clusterService.messagingService(),
                    clusterService.topologyService(),
                    clusterService.serializationRegistry(),
                    replicaManager,
                    mock(LockManager.class),
                    replicaSvc,
                    txManager,
                    dataStorageMgr,
                    storagePath,
                    metaStorageManager,
                    schemaManager,
                    view -> new LocalLogStorageFactory(),
                    threadPoolsManager.tableIoExecutor(),
                    threadPoolsManager.partitionOperationsExecutor(),
                    clock,
                    clockService,
                    new OutgoingSnapshotsManager(clusterService.messagingService()),
                    distributionZoneManager,
                    schemaSyncService,
                    catalogManager,
                    new HybridTimestampTracker(),
                    placementDriver,
                    () -> mock(IgniteSql.class),
                    resourcesRegistry,
                    rebalanceScheduler,
                    lowWatermark,
                    transactionInflights
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

            indexManager = new IndexManager(
                    schemaManager,
                    tableManager,
                    catalogManager,
                    threadPoolsManager.tableIoExecutor(),
                    registry,
                    lowWatermark
            );
        }

        private void waitForMetadataCompletenessAtNow() {
            assertThat(schemaSyncService.waitForMetadataCompleteness(hybridClock.now()), willCompleteSuccessfully());
        }

        /**
         * Starts the created components.
         */
        void start() {
            List<IgniteComponent> firstComponents = List.of(
                    threadPoolsManager,
                    vaultManager,
                    nodeCfgMgr,
                    failureProcessor,
                    clusterService,
                    raftManager,
                    cmgManager
            );

            List<CompletableFuture<?>> componentFuts = firstComponents.stream().map(IgniteComponent::start).collect(Collectors.toList());

            nodeComponents.addAll(firstComponents);

            deployWatchesFut = CompletableFuture.supplyAsync(() -> {
                List<IgniteComponent> secondComponents = List.of(
                        lowWatermark,
                        metaStorageManager,
                        clusterCfgMgr,
                        clockWaiter,
                        catalogManager,
                        distributionZoneManager,
                        replicaManager,
                        txManager,
                        dataStorageMgr,
                        schemaManager,
                        tableManager,
                        indexManager
                );

                componentFuts.addAll(secondComponents.stream().map(IgniteComponent::start).collect(Collectors.toList()));

                nodeComponents.addAll(secondComponents);

                var configurationNotificationFut = metaStorageManager.recoveryFinishedFuture().thenCompose(rev -> {
                    return allOf(
                            nodeCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            clusterCfgMgr.configurationRegistry().notifyCurrentConfigurationListeners(),
                            ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart()
                    );
                });

                assertThat(configurationNotificationFut, willSucceedIn(1, TimeUnit.MINUTES));

                lowWatermark.scheduleUpdates();

                return metaStorageManager.deployWatches();
            }).thenCombine(allOf(componentFuts.toArray(CompletableFuture[]::new)), (deployWatchesFut, unused) -> null);
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
    private static VaultManager createVault(Path workDir) {
        return new VaultManager(new PersistentVaultService(resolveDir(workDir, "vault")));
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

        return ((TableViewInternal) table).internalTable();
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

    private static void createZone(Node node, String zoneName, int partitions, int replicas, boolean testStorageProfile) {
        DistributionZonesTestUtil.createZoneWithStorageProfile(
                node.catalogManager,
                zoneName,
                partitions,
                replicas,
                testStorageProfile ? DEFAULT_TEST_PROFILE_NAME : DEFAULT_STORAGE_PROFILE
        );
    }

    private static void alterZone(Node node, String zoneName, int replicas) {
        node.waitForMetadataCompletenessAtNow();

        DistributionZonesTestUtil.alterZone(node.catalogManager, zoneName, replicas);
    }

    private static void createTable(Node node, String zoneName, String tableName) {
        node.waitForMetadataCompletenessAtNow();

        TableTestUtils.createTable(
                node.catalogManager,
                DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT64).build(),
                        ColumnParams.builder().name("val").type(INT32).nullable(true).build()
                ),
                List.of("key")
        );
    }

    private static @Nullable Integer getTableId(Node node, String tableName) {
        return TableTestUtils.getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());
    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    private void checkPartitionNodes(int partitionId, int expNodeCount) {
        for (Node node : nodes) {
            assertEquals(expNodeCount, getPartitionClusterNodes(node, partitionId).size(), node.name);
        }
    }

    private void checkPartitionNodes(String tableName, int partitionId, int expNodeCount) {
        for (Node node : nodes) {
            assertEquals(expNodeCount, getPartitionClusterNodes(node, tableName, partitionId).size(), node.name);
        }
    }
}
