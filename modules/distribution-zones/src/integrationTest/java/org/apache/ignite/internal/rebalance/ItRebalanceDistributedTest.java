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

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestRebalanceUtil.partitionReplicationGroupId;
import static org.apache.ignite.internal.TestRebalanceUtil.pendingPartitionAssignments;
import static org.apache.ignite.internal.TestRebalanceUtil.pendingPartitionAssignmentsKey;
import static org.apache.ignite.internal.TestRebalanceUtil.plannedPartitionAssignments;
import static org.apache.ignite.internal.TestRebalanceUtil.plannedPartitionAssignmentsKey;
import static org.apache.ignite.internal.TestRebalanceUtil.stablePartitionAssignments;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.configuration.IgnitePaths.cmgPath;
import static org.apache.ignite.internal.configuration.IgnitePaths.metastoragePath;
import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractTablePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getZoneIdByTableNameStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.framework;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.client.handler.configuration.ClientConnectorExtensionConfigurationSchema;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterIdHolder;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfigurationSchema;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalExtensionConfigurationSchema;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.configuration.validation.NonNegativeIntegerNumberSystemPropertyValueValidator;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfigurationSchema;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.raft.JraftGroupEventsListener;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicaTestUtils;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.VersionedAssignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfigurationSchema;
import org.apache.ignite.internal.rest.configuration.RestExtensionConfigurationSchema;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfigurationSchema;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.impl.TestDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncRequest;
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
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class, ExecutorServiceExtension.class})
@Timeout(120)
public class ItRebalanceDistributedTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceDistributedTest.class);

    private static final String TABLE_NAME = "TBL1";

    private static final String TABLE_NAME_2 = "TBL2";

    private static final String TABLE_NAME_3 = "TBL3";

    private static final String ZONE_NAME = "zone1";

    private static final int BASE_PORT = 20_000;

    /** Filter to determine a primary node identically on any cluster node. */
    private static final Function<Collection<InternalClusterNode>, InternalClusterNode> PRIMARY_FILTER = nodes -> nodes.stream()
            .filter(n -> n.address().port() == BASE_PORT).findFirst().orElse(null);

    private static final String HOST = "localhost";

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    private static final int NODE_COUNT = 3;

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemConfiguration;

    @InjectConfiguration
    private NodeAttributesConfiguration nodeAttributes;

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = \"aipersist\", test.engine=\"test\"}")
    private StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

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

    @InjectExecutorService
    private ScheduledExecutorService commonScheduledExecutorService;

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

        // TODO: IGNITE-23956 Move this line in the base class.
        // It is necessary to do after each test to prevent OOM in the middle of the test class execution.
        framework().clearInlineMocks();
    }

    @Test
    void testOneRebalance() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        electPrimaryReplica(node);

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

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        electPrimaryReplica(node);

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

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        electPrimaryReplica(node);

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

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        electPrimaryReplica(node);

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

        electPrimaryReplica(node0);

        Set<String> partitionNodesConsistentIds = getPartitionStableAssignments(node0, 0).stream()
                .map(Assignment::consistentId)
                .collect(toSet());

        Node newNode = nodes.stream().filter(n -> !partitionNodesConsistentIds.contains(n.name)).findFirst().orElseThrow();

        InternalClusterNode leaderClusterNode = ReplicaTestUtils.leaderAssignment(
                node1.replicaManager,
                node1.clusterService.topologyService(),
                colocationEnabled() ? table.zoneId() : table.tableId(),
                0
        );

        Node leaderNode = findNodeByConsistentId(leaderClusterNode.name());

        String nonLeaderNodeConsistentId = partitionNodesConsistentIds.stream()
                .filter(n -> !n.equals(leaderNode.name))
                .findFirst()
                .orElseThrow();

        Node nonLeaderNode = findNodeByConsistentId(nonLeaderNodeConsistentId);

        TableViewInternal nonLeaderTable = (TableViewInternal) nonLeaderNode.tableManager.table(TABLE_NAME);

        var countDownLatch = new CountDownLatch(1);

        RaftNodeId partitionNodeId = leaderNode.raftManager.server()
                .localNodes()
                .stream()
                .filter(nodeId -> nodeId.groupId().toString().contains("part"))
                .filter(nodeId -> !hasDefaultZone(leaderNode.catalogManager)
                        || (hasDefaultZone(leaderNode.catalogManager)
                        && !nodeId.groupId().toString().contains(defaultZoneId(leaderNode.catalogManager) + "_part")))
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

        // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId
        int tableOrZoneId = colocationEnabled() ? nonLeaderTable.zoneId() : nonLeaderTable.tableId();
        assertThat(
                ReplicaTestUtils.getRaftClient(nonLeaderNode.replicaManager, tableOrZoneId, 0)
                        .map(raftClient -> raftClient.transferLeadership(new Peer(nonLeaderNodeConsistentId)))
                        .orElse(null),
                willCompleteSuccessfully()
        );

        ((JraftServerImpl) leaderNode.raftManager.server()).stopBlockMessages(partitionNodeId);

        waitPartitionAssignmentsSyncedToExpected(0, 3);

        checkPartitionNodes(0, 3);
    }

    private static int defaultZoneId(CatalogManager catalog) {
        return catalog.catalog(catalog.latestCatalogVersion()).defaultZone().id();
    }

    private static boolean hasDefaultZone(CatalogManager catalog) {
        return catalog.catalog(catalog.latestCatalogVersion()).defaultZone() != null;
    }

    @Test
    void testRebalanceRetryWhenCatchupFailed() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        alterZone(node, ZONE_NAME, 1);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        electPrimaryReplica(node);

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

        // Later in the test, as part of the reaction to the replica factor reduction, the zone storage will be destroyed,
        // so to verify the correctness of the deletion, it is necessary to make sure that the storage was initially created.
        if (colocationEnabled()) {
            nodes.forEach(
                    n -> assertNotNull(
                            n.partitionReplicaLifecycleManager.txStatePartitionStorage(getInternalTable(node, TABLE_NAME).zoneId(), 0)));
        }

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionStableAssignments(node, 0);

        changeTableReplicasForSinglePartition(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        electPrimaryReplica(node);

        Set<Assignment> assignmentsAfterChangeReplicas = getPartitionStableAssignments(node, 0);

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

        Set<Assignment> assignmentsBeforeChangeReplicas = getPartitionStableAssignments(node, 0);

        nodes.forEach(n -> {
            prepareFinishHandleChangeStableAssignmentEventFuture(n, TABLE_NAME, 0);

            throwExceptionOnInvokeDestroyPartitionStorages(n, TABLE_NAME, 0);
        });

        changeTableReplicasForSinglePartition(node, ZONE_NAME, 2);

        waitPartitionAssignmentsSyncedToExpected(0, 2);

        Assignment evictedAssignment = first(getEvictedAssignments(
                assignmentsBeforeChangeReplicas,
                getPartitionStableAssignments(node, 0)));

        Node evictedNode = findNodeByConsistentId(evictedAssignment.consistentId());

        // Let's make sure that we handled the events (STABLE_ASSIGNMENTS_PREFIX) from the metastore correctly.
        assertThat(
                collectFinishHandleChangeStableAssignmentEventFuture(n -> !n.equals(evictedNode), TABLE_NAME, 0),
                willCompleteSuccessfully()
        );

        PartitionGroupId partitionGroupId = evictedNode.getPartitionGroupId(TABLE_NAME, 0);

        assertThat(evictedNode.finishHandleChangeStableAssignmentEventFutures.get(partitionGroupId), willThrowFast(Exception.class));

        // Restart evicted node.
        int evictedNodeIndex = findNodeIndexByConsistentId(evictedAssignment.consistentId());

        evictedNode.stop();

        Node newNode = new Node(testInfo, evictedNode.networkAddress);

        newNode.finishHandleChangeStableAssignmentEventFutures.put(partitionGroupId, new CompletableFuture<>());

        newNode.start();

        newNode.waitWatches();

        nodes.set(evictedNodeIndex, newNode);

        // Let's make sure that we will destroy the partition again.
        assertThat(newNode.finishHandleChangeStableAssignmentEventFutures.get(partitionGroupId), willSucceedIn(1, TimeUnit.MINUTES));

        checkInvokeDestroyedPartitionStorages(newNode, TABLE_NAME, 0);
    }

    /**
     * Test checks rebalances from [A,B,C] to [A,B] and then again to [A,B,C].
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

        clearSpyInvocations();

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        electPrimaryReplica(node);

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

        TableViewInternal table = unwrapTableViewInternal(node.tableManager.table(TABLE_NAME));

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        electPrimaryReplica(node);

        Set<Assignment> assignmentsBeforeRebalance = getPartitionStableAssignments(node, 0);

        List<String> newNodeNames = getNodeNames(notIn(assignmentsBeforeRebalance))
                .collect(toList());

        Set<Assignment> newAssignment = Set.of(Assignment.forPeer(newNodeNames.get(0)));
        PartitionGroupId partitionGroupId = partitionReplicationGroupId(table, 0);

        // Write the new assignments to metastore as a pending assignments.
        {
            ByteArray partAssignmentsPendingKey = pendingPartitionAssignmentsKey(partitionGroupId);

            int catalogVersion = node.catalogManager.latestCatalogVersion();
            long timestamp = node.catalogManager.catalog(catalogVersion).time();

            byte[] bytesPendingAssignments = AssignmentsQueue.toBytes(Assignments.of(newAssignment, timestamp));

            node.metaStorageManager
                    .put(partAssignmentsPendingKey, bytesPendingAssignments)
                    .get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);
        }

        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionStableAssignments(n, 0).equals(newAssignment)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        // Wait for rebalance to complete.
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionStableAssignments(n, 0).equals(newAssignment)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        // Check that raft clients on all nodes were updated with the new list of peers.
        Predicate<Node> isNodeInReplicationGroup = n -> isNodeInAssignments(n, newAssignment);
        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .filter(isNodeInReplicationGroup)
                        .allMatch(isNodeUpdatesPeersAndLearnersOnGroupService(newAssignment)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        // Checks that there no any replicas outside replication group
        Predicate<Node> isNodeOutsideReplicationGroup = n -> !isNodeInAssignments(n, newAssignment);
        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .filter(isNodeOutsideReplicationGroup)
                        .noneMatch(n -> isReplicationGroupStarted(n, partitionGroupId)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    @Test
    void testClientsAreUpdatedAfterPendingRebalanceHandled() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        TableViewInternal table = unwrapTableViewInternal(node.tableManager.table(TABLE_NAME));

        waitPartitionAssignmentsSyncedToExpected(0, 1);

        electPrimaryReplica(node);

        Set<Assignment> assignmentsBeforeRebalance = getPartitionStableAssignments(node, 0);

        List<String> newNodeNames = getNodeNames(notIn(assignmentsBeforeRebalance))
                .collect(toList());

        assertThat(newNodeNames, hasSize(greaterThanOrEqualTo(2)));

        Set<Assignment> newAssignment = Set.of(Assignment.forPeer(newNodeNames.get(0)), Assignment.forLearner(newNodeNames.get(1)));
        PartitionGroupId partId = partitionReplicationGroupId(table, 0);

        // Write the new assignments to metastore as a pending assignments.
        ByteArray partAssignmentsPendingKey = pendingPartitionAssignmentsKey(partId);

        int catalogVersion = node.catalogManager.latestCatalogVersion();
        long timestamp = node.catalogManager.catalog(catalogVersion).time();

        byte[] bytesPendingAssignments = AssignmentsQueue.toBytes(Assignments.of(newAssignment, timestamp));

        AtomicBoolean dropMessages = new AtomicBoolean(true);

        // Using this hack we pause rebalance on all nodes
        nodes.forEach(n -> ((DefaultMessagingService) n.clusterService.messagingService())
                .dropMessages((nodeName, msg) -> msg instanceof ChangePeersAndLearnersAsyncRequest && dropMessages.get())
        );

        node.metaStorageManager.put(partAssignmentsPendingKey, bytesPendingAssignments).get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);

        Set<Assignment> union = RebalanceUtil.union(assignmentsBeforeRebalance, newAssignment);

        // Check that raft clients on all nodes were updated with the new list of peers.
        Predicate<Node> isNodeInReplicationGroup = n -> isNodeInAssignments(n, union);
        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .filter(isNodeInReplicationGroup)
                        .allMatch(isNodeUpdatesPeersAndLearnersOnGroupService(union)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        // Checks that there no any replicas outside replication group
        Predicate<Node> isNodeOutsideReplicationGroup = n -> !isNodeInAssignments(n, union);

        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .filter(isNodeOutsideReplicationGroup)
                        .noneMatch(n -> isReplicationGroupStarted(n, partId)),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        dropMessages.set(false);
    }

    @Test
    void testRebalanceRetryDelayConfiguration() throws Exception {
        Node node = getNode(0);

        createZone(node, ZONE_NAME, 1, 1);

        createTable(node, ZONE_NAME, TABLE_NAME);

        assertTrue(waitForCondition(() -> getPartitionStableAssignments(node, 0).size() == 1, AWAIT_TIMEOUT_MILLIS));

        // Check default value
        checkRebalanceRetryDelay(1, REBALANCE_RETRY_DELAY_DEFAULT);

        SystemDistributedConfiguration configuration =
                node.clusterCfgMgr.configurationRegistry().getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();

        assertThat(updateRebalanceRetryDelay(configuration, REBALANCE_RETRY_DELAY_DEFAULT + 1), willCompleteSuccessfully());

        // Check delay after change
        checkRebalanceRetryDelay(1, REBALANCE_RETRY_DELAY_DEFAULT + 1);

        // Try invalid delay value
        assertThat(updateRebalanceRetryDelay(configuration, -1), willThrowWithCauseOrSuppressed(ConfigurationValidationException.class));
    }

    private static Set<Peer> assignmentsToPeersSet(Set<Assignment> assignments) {
        return assignments.stream()
                .map(Assignment::consistentId)
                .map(Peer::new)
                .collect(toSet());
    }

    private static boolean isNodeInAssignments(Node node, Set<Assignment> assignments) {
        return assignmentsToPeersSet(assignments).stream()
                .map(Peer::consistentId)
                .anyMatch(id -> id.equals(node.clusterService.nodeName()));
    }

    private static boolean isReplicationGroupStarted(Node node, ReplicationGroupId replicationGroupId) {
        return node.replicaManager.isReplicaStarted(replicationGroupId);
    }

    private static Predicate<Node> isNodeUpdatesPeersAndLearnersOnGroupService(Set<Assignment> desiredAssignments) {
        PeersAndLearners desired = PeersAndLearners.fromAssignments(desiredAssignments);
        return node -> ReplicaTestUtils.getRaftClient(node.replicaManager, getTableOrZoneId(node, TABLE_NAME), 0)
                .map(raftClient -> desired.peers().equals(new HashSet<>(raftClient.peers()))
                        && desired.learners().equals(new HashSet<>(raftClient.learners())))
                .orElse(false);
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

        Set<Assignment> pendingAssignments = calculateAssignmentForPartition(dataNodes, 0, 1, 2, 2);
        Set<Assignment> plannedAssignments = calculateAssignmentForPartition(dataNodes, 0, 1, 3, 3);

        Node node0 = getNode(0);
        TableViewInternal table = unwrapTableViewInternal(node0.tableManager.table(TABLE_NAME));
        PartitionGroupId partitionGroupId = partitionReplicationGroupId(table, 0);

        int catalogVersion = node0.catalogManager.latestCatalogVersion();
        long timestamp = node0.catalogManager.catalog(catalogVersion).time();

        byte[] bytesPendingAssignments = AssignmentsQueue.toBytes(Assignments.of(pendingAssignments, timestamp));
        byte[] bytesPlannedAssignments = Assignments.toBytes(plannedAssignments, timestamp);

        ByteArray partAssignmentsPendingKey = pendingPartitionAssignmentsKey(partitionGroupId);
        ByteArray partAssignmentsPlannedKey = plannedPartitionAssignmentsKey(partitionGroupId);

        Map<ByteArray, byte[]> msEntries = new HashMap<>();

        msEntries.put(partAssignmentsPendingKey, bytesPendingAssignments);
        msEntries.put(partAssignmentsPlannedKey, bytesPlannedAssignments);

        node0.metaStorageManager.putAll(msEntries).get(AWAIT_TIMEOUT_MILLIS, MILLISECONDS);
    }

    private void verifyThatRaftNodesAndReplicasWereStartedOnlyOnce() throws Exception {
        TableViewInternal table = unwrapTableViewInternal(getNode(0).tableManager.table(TABLE_NAME));

        ReplicationGroupId groupId = colocationEnabled()
                ? new ZonePartitionId(table.zoneId(), 0)
                : new TablePartitionId(table.tableId(), 0);

        for (int i = 0; i < NODE_COUNT; i++) {
            var node = getNode(i);
            verify(node.raftManager, timeout(AWAIT_TIMEOUT_MILLIS).times(1))
                    .startRaftGroupNode(eq(new RaftNodeId(groupId, new Peer(node.name))), any(), any(), any(), any(),
                            notNull(TopologyAwareRaftGroupServiceFactory.class));

            if (colocationEnabled()) {
                verify(getNode(i).replicaManager, timeout(AWAIT_TIMEOUT_MILLIS).times(1))
                        .startReplica(eq(groupId), any(), any(), any(), any(), any(), anyBoolean(), any(), any());
            } else {
                verify(getNode(i).replicaManager, timeout(AWAIT_TIMEOUT_MILLIS).times(1))
                        .startReplica(any(), any(), anyBoolean(), any(), any(), any(), eq(groupId), any());
            }
        }
    }

    private void waitPartitionAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        waitPartitionAssignmentsSyncedToExpected(TABLE_NAME, partNum, replicasNum);
    }

    private void waitPartitionAssignmentsSyncedToExpected(String tableName, int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionStableAssignments(n, tableName, partNum).size() == replicasNum
                        && getPartitionPendingAssignments(n, tableName, partNum).isEmpty()),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));

        Node anyNode = nodes.get(0);
        Set<Assignment> assignments = getPartitionStableAssignments(anyNode, tableName, replicasNum);

        assertTrue(waitForCondition(
                () -> nodes.stream()
                        .filter(n -> isNodeInAssignments(n, assignments))
                        .allMatch(n -> ReplicaTestUtils.getRaftClient(n.replicaManager, getTableOrZoneId(n, tableName), partNum)
                                .isPresent()),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 tableOrZoneId -> zoneId, remove.
    private static int getTableOrZoneId(Node node, String tableName) {
        return colocationEnabled() ? getZoneIdByTableNameStrict(node.catalogManager, tableName, node.hybridClock.nowLong())
                : getTableIdStrict(node.catalogManager, tableName, node.hybridClock.nowLong());
    }

    private void waitPartitionPendingAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionPendingAssignments(n, partNum).size() == replicasNum),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    private void waitPartitionPlannedAssignmentsSyncedToExpected(int partNum, int replicasNum) throws Exception {
        assertTrue(waitForCondition(
                () -> nodes.stream().allMatch(n -> getPartitionPlannedAssignments(n, partNum).size() == replicasNum),
                (long) AWAIT_TIMEOUT_MILLIS * nodes.size()
        ));
    }

    private Node findNodeByConsistentId(String consistentId) {
        return nodes.stream().filter(n -> n.name.equals(consistentId)).findFirst().orElseThrow();
    }

    private int findNodeIndexByConsistentId(String consistentId) {
        return IntStream.range(0, nodes.size()).filter(i -> getNode(i).name.equals(consistentId)).findFirst().orElseThrow();
    }

    private void electPrimaryReplica(Node primaryReplicaNode) throws InterruptedException {
        Node leaseholderNode = getLeaseholderNodeForPartition(primaryReplicaNode, 0);

        TableViewInternal table = unwrapTableViewInternal(leaseholderNode.tableManager.table(TABLE_NAME));

        PartitionGroupId groupId = partitionReplicationGroupId(table, 0);

        assertTrue(waitForCondition(() -> isReplicationGroupStarted(leaseholderNode, groupId), AWAIT_TIMEOUT_MILLIS));

        InternalClusterNode leaseholder = leaseholderNode.clusterService
                .topologyService()
                .localMember();

        nodes.forEach(node -> node.placementDriver.setPrimaryReplicaSupplier(() -> new TestReplicaMetaImpl(
                leaseholder.name(),
                leaseholder.id(),
                groupId
        )));
    }

    private Node getLeaseholderNodeForPartition(Node node, int partId) {
        Set<Assignment> assignments = getPartitionStableAssignments(node, partId);

        String leaseholderConsistentId = assignments.stream().findFirst().get().consistentId();

        return nodes.stream()
                .filter(n -> n.clusterService.topologyService().localMember().name().equals(leaseholderConsistentId))
                .findFirst()
                .get();
    }

    private static Set<Assignment> getPartitionStableAssignments(Node node, int partNum) {
        return getPartitionStableAssignments(node, TABLE_NAME, partNum);
    }

    private static Set<Assignment> getPartitionStableAssignments(Node node, String tableName, int partNum) {
        TableViewInternal table = unwrapTableViewInternal(node.tableManager.table(tableName));

        var stableAssignmentsFuture = stablePartitionAssignments(node.metaStorageManager, table, partNum);

        assertThat(stableAssignmentsFuture, willCompleteSuccessfully());

        return Optional
                .ofNullable(stableAssignmentsFuture.join())
                .orElse(Set.of());
    }

    private static Set<Assignment> getDefaultZonePartitionStableAssignments(Node node, int partitionIndex) {
        var stableAssignmentsFuture = ZoneRebalanceUtil.zonePartitionAssignments(
                node.metaStorageManager,
                defaultZoneId(node.catalogManager),
                partitionIndex
        );

        assertThat(stableAssignmentsFuture, willCompleteSuccessfully());

        return Optional
                .ofNullable(stableAssignmentsFuture.join())
                .orElse(Set.of());
    }

    private static Set<Assignment> getPartitionPendingAssignments(Node node, String tableName, int partNum) {
        TableViewInternal table = unwrapTableViewInternal(node.tableManager.table(tableName));

        var pendingAssignmentsFuture =  pendingPartitionAssignments(node.metaStorageManager, table, partNum);

        assertThat(pendingAssignmentsFuture, willCompleteSuccessfully());

        return Optional
                .ofNullable(pendingAssignmentsFuture.join())
                .orElse(Set.of());
    }

    private static Set<Assignment> getPartitionPendingAssignments(Node node, int partNum) {
        return getPartitionPendingAssignments(node, TABLE_NAME, partNum);
    }

    private static Set<Assignment> getPartitionPlannedAssignments(Node node, int partNum) {
        TableViewInternal table = unwrapTableViewInternal(node.tableManager.table(TABLE_NAME));

        var plannedAssignmentsFuture = plannedPartitionAssignments(node.metaStorageManager, table, partNum);

        assertThat(plannedAssignmentsFuture, willCompleteSuccessfully());

        return Optional
                .ofNullable(plannedAssignmentsFuture.join())
                .orElse(Set.of());
    }

    private class Node {
        final String name;

        final Loza raftManager;

        final ThreadPoolsManager threadPoolsManager;

        final ReplicaManager replicaManager;

        final MetaStorageManagerImpl metaStorageManager;

        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final HeapLockManager lockManager;

        private final TxManager txManager;

        private final DistributedConfigurationStorage cfgStorage;

        private final DataStorageManager dataStorageMgr;

        private final TxStateRocksDbSharedStorage sharedTxStateStorage;

        private final TableManager tableManager;

        private final DistributionZoneManager distributionZoneManager;

        private final ConfigurationManager nodeCfgMgr;

        private final ConfigurationManager clusterCfgMgr;

        private final ClusterManagementGroupManager cmgManager;

        private final SchemaManager schemaManager;

        private final SchemaSafeTimeTrackerImpl schemaSafeTimeTracker;

        private final CatalogManager catalogManager;

        final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

        private final SchemaSyncService schemaSyncService;

        private final ClockWaiter clockWaiter;

        private final List<IgniteComponent> nodeComponents = new CopyOnWriteArrayList<>();

        private final ConfigurationTreeGenerator nodeCfgGenerator;

        private final ConfigurationTreeGenerator clusterCfgGenerator;

        private final Map<PartitionGroupId, CompletableFuture<Void>> finishHandleChangeStableAssignmentEventFutures
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
        private final FailureManager failureManager;

        private final LogStorageFactory logStorageFactory;

        private final LogStorageFactory cmgLogStorageFactory;

        private final LogStorageFactory msLogStorageFactory;

        final TestPlacementDriver placementDriver;

        private final IndexMetaStorage indexMetaStorage;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, NetworkAddress addr) {
            networkAddress = addr;

            name = testNodeName(testInfo, addr.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(dir);

            NodeProperties nodeProperties = new SystemPropertiesNodeProperties();

            var clusterIdService = new ClusterIdHolder();

            nodeCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(NodeConfiguration.KEY),
                    List.of(
                            NetworkExtensionConfigurationSchema.class,
                            RestExtensionConfigurationSchema.class,
                            ClientConnectorExtensionConfigurationSchema.class,
                            StorageExtensionConfigurationSchema.class,
                            PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                            VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class,
                            RocksDbStorageEngineExtensionConfigurationSchema.class,
                            SystemLocalExtensionConfigurationSchema.class
                    ),
                    List.of(
                            PersistentPageMemoryProfileConfigurationSchema.class,
                            VolatilePageMemoryProfileConfigurationSchema.class,
                            RocksDbProfileConfigurationSchema.class,
                            StaticNodeFinderConfigurationSchema.class,
                            MulticastNodeFinderConfigurationSchema.class
                    )
            );

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            TestIgnitionManager.writeConfigurationFileApplyingTestDefaults(configPath);

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NodeConfiguration.KEY),
                    new LocalFileConfigurationStorage(configPath, nodeCfgGenerator, null),
                    nodeCfgGenerator,
                    new TestConfigurationValidator()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    addr.port(),
                    finder,
                    new InMemoryStaleIds(),
                    clusterIdService
            );

            lockManager = new HeapLockManager(systemConfiguration);

            MetricManager metricManager = new NoOpMetricManager();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            ComponentWorkingDir partitionsBasePath = partitionsPath(systemConfiguration, dir);

            logStorageFactory = SharedLogStorageFactoryUtils.create(clusterService.nodeName(), partitionsBasePath.raftLogPath());

            RaftGroupOptionsConfigurer partitionRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(logStorageFactory, partitionsBasePath.metaPath());

            raftManager = spy(new Loza(
                    clusterService,
                    metricManager,
                    raftConfiguration,
                    hybridClock,
                    raftGroupEventsClientListener,
                    new NoOpFailureManager()
            ));

            failureManager = new NoOpFailureManager();

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage, failureManager);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator(),
                    new SystemPropertiesNodeProperties()
            );

            ComponentWorkingDir cmgWorkDir = cmgPath(systemConfiguration, dir);

            cmgLogStorageFactory =
                    SharedLogStorageFactoryUtils.create(clusterService.nodeName(), cmgWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer cmgRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageFactory, cmgWorkDir.metaPath());

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    new SystemDisasterRecoveryStorage(vaultManager),
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    new NodeAttributesCollector(nodeAttributes, storageConfiguration),
                    failureManager,
                    clusterIdService,
                    cmgRaftConfigurer,
                    metricManager
            );

            LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

            KeyValueStorage keyValueStorage = !testInfo.getTestMethod().get().isAnnotationPresent(UseRocksMetaStorage.class)
                    ? new SimpleInMemoryKeyValueStorage(name, readOperationForCompactionTracker)
                    : new RocksDbKeyValueStorage(
                            name,
                            resolveDir(dir, "metaStorage"),
                            failureManager,
                            readOperationForCompactionTracker,
                            commonScheduledExecutorService
                    );

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            ComponentWorkingDir metastorageWorkDir = metastoragePath(systemConfiguration, dir);

            msLogStorageFactory =
                    SharedLogStorageFactoryUtils.create(clusterService.nodeName(), metastorageWorkDir.raftLogPath());

            RaftGroupOptionsConfigurer msRaftConfigurer =
                    RaftGroupOptionsConfigHelper.configureProperties(msLogStorageFactory, metastorageWorkDir.metaPath());

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    keyValueStorage,
                    hybridClock,
                    topologyAwareRaftGroupServiceFactory,
                    metricManager,
                    systemDistributedConfiguration,
                    msRaftConfigurer,
                    readOperationForCompactionTracker
            );

            placementDriver = new TestPlacementDriver(() -> PRIMARY_FILTER.apply(clusterService.topologyService().allMembers()));

            threadPoolsManager = new ThreadPoolsManager(name, metricManager);

            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

            clockWaiter = new ClockWaiter(name, hybridClock, threadPoolsManager.commonScheduler());

            ClockService clockService = new ClockServiceImpl(
                    hybridClock,
                    clockWaiter,
                    () -> DEFAULT_MAX_CLOCK_SKEW_MS,
                    skew -> {}
            );

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    clockService,
                    threadPoolsManager.partitionOperationsExecutor(),
                    replicationConfiguration,
                    threadPoolsManager.commonScheduler()
            );

            var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService);

            cfgStorage = new DistributedConfigurationStorage("test", metaStorageManager);

            clusterCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(ClusterConfiguration.KEY),
                    List.of(
                            GcExtensionConfigurationSchema.class,
                            ReplicationExtensionConfigurationSchema.class,
                            SystemDistributedExtensionConfigurationSchema.class
                    ),
                    List.of()
            );

            clusterCfgMgr = new ConfigurationManager(
                    List.of(ClusterConfiguration.KEY),
                    cfgStorage,
                    clusterCfgGenerator,
                    ConfigurationValidatorImpl.withDefaultValidators(
                            clusterCfgGenerator, Set.of(new NonNegativeIntegerNumberSystemPropertyValueValidator(REBALANCE_RETRY_DELAY_MS))
                    )
            );

            ConfigurationRegistry clusterConfigRegistry = clusterCfgMgr.configurationRegistry();

            var registry = new MetaStorageRevisionListenerRegistry(metaStorageManager);

            GcConfiguration gcConfig = clusterConfigRegistry.getConfiguration(GcExtensionConfiguration.KEY).gc();

            DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                    new PersistentPageMemoryDataStorageModule(),
                    new VolatilePageMemoryDataStorageModule(),
                    new TestDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");

            dataStorageMgr = new DataStorageManager(
                    dataStorageModules.createStorageEngines(
                            name,
                            metricManager,
                            nodeCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null,
                            failureManager,
                            logStorageFactory,
                            hybridClock,
                            commonScheduledExecutorService
                    ),
                    storageConfiguration
            );

            lowWatermark = new LowWatermarkImpl(
                    name,
                    gcConfig.lowWatermark(),
                    clockService,
                    vaultManager,
                    failureManager,
                    clusterService.messagingService()
            );

            txManager = new TxManagerImpl(
                    txConfiguration,
                    systemDistributedConfiguration,
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
                    lowWatermark,
                    commonScheduledExecutorService,
                    metricManager
            );

            replicaManager = spy(new ReplicaManager(
                    name,
                    clusterService,
                    cmgManager,
                    clockService,
                    Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    threadPoolsManager.partitionOperationsExecutor(),
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new NoOpFailureManager(),
                    new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()),
                    topologyAwareRaftGroupServiceFactory,
                    raftManager,
                    partitionRaftConfigurer,
                    view -> new LocalLogStorageFactory(),
                    threadPoolsManager.tableIoExecutor(),
                    replicaGrpId -> metaStorageManager.get(pendingPartAssignmentsQueueKey((TablePartitionId) replicaGrpId))
                            .thenApply(entry -> new VersionedAssignments(entry.value(), entry.revision())),
                    threadPoolsManager.commonScheduler()
            ));

            LongSupplier delayDurationMsSupplier = () -> 10L;

            catalogManager = new CatalogManagerImpl(
                    new UpdateLogImpl(metaStorageManager, failureManager),
                    clockService,
                    failureManager,
                    delayDurationMsSupplier
            );

            indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageManager);

            schemaManager = new SchemaManager(registry, catalogManager);

            schemaSafeTimeTracker = new SchemaSafeTimeTrackerImpl(metaStorageManager.clusterTime());
            metaStorageManager.registerNotificationEnqueuedListener(schemaSafeTimeTracker);

            schemaSyncService = new SchemaSyncServiceImpl(schemaSafeTimeTracker, delayDurationMsSupplier);

            SystemDistributedConfiguration systemDistributedConfiguration =
                    clusterConfigRegistry.getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();

            distributionZoneManager = new DistributionZoneManager(
                    name,
                    () -> clusterService.topologyService().localMember().id(),
                    registry,
                    metaStorageManager,
                    logicalTopologyService,
                    catalogManager,
                    systemDistributedConfiguration,
                    clockService,
                    metricManager,
                    gcConfig
            );

            MinimumRequiredTimeCollectorService minTimeCollectorService = new MinimumRequiredTimeCollectorServiceImpl();

            sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                    name,
                    storagePath.resolve("tx-state"),
                    threadPoolsManager.commonScheduler(),
                    threadPoolsManager.tableIoExecutor(),
                    logStorageFactory,
                    failureManager
            );

            var outgoingSnapshotManager = new OutgoingSnapshotsManager(name, clusterService.messagingService(), failureManager);

            partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                    catalogManager,
                    replicaManager,
                    distributionZoneManager,
                    metaStorageManager,
                    clusterService.topologyService(),
                    lowWatermark,
                    failureManager,
                    nodeProperties,
                    threadPoolsManager.tableIoExecutor(),
                    threadPoolsManager.rebalanceScheduler(),
                    threadPoolsManager.partitionOperationsExecutor(),
                    clockService,
                    placementDriver,
                    schemaSyncService,
                    systemDistributedConfiguration,
                    sharedTxStateStorage,
                    txManager,
                    schemaManager,
                    dataStorageMgr,
                    outgoingSnapshotManager
            );

            tableManager = new TableManager(
                    name,
                    registry,
                    gcConfig,
                    txConfiguration,
                    replicationConfiguration,
                    clusterService.messagingService(),
                    clusterService.topologyService(),
                    clusterService.serializationRegistry(),
                    replicaManager,
                    mock(LockManager.class),
                    replicaSvc,
                    txManager,
                    dataStorageMgr,
                    sharedTxStateStorage,
                    metaStorageManager,
                    schemaManager,
                    threadPoolsManager.tableIoExecutor(),
                    threadPoolsManager.partitionOperationsExecutor(),
                    threadPoolsManager.rebalanceScheduler(),
                    threadPoolsManager.commonScheduler(),
                    clockService,
                    outgoingSnapshotManager,
                    distributionZoneManager,
                    schemaSyncService,
                    catalogManager,
                    failureManager,
                    HybridTimestampTracker.atomicTracker(null),
                    placementDriver,
                    () -> mock(IgniteSql.class),
                    resourcesRegistry,
                    lowWatermark,
                    transactionInflights,
                    indexMetaStorage,
                    logStorageFactory,
                    partitionReplicaLifecycleManager,
                    nodeProperties,
                    minTimeCollectorService,
                    systemDistributedConfiguration,
                    metricManager,
                    TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER_FACTORY
            ) {
                @Override
                protected TxStateStorage createTxStateTableStorage(
                        CatalogTableDescriptor tableDescriptor,
                        CatalogZoneDescriptor zoneDescriptor
                ) {
                    return testInfo.getTestMethod().get().isAnnotationPresent(UseTestTxStateStorage.class)
                            ? spy(new TestTxStateStorage())
                            : super.createTxStateTableStorage(tableDescriptor, zoneDescriptor);
                }

                @Override
                protected CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
                    PartitionGroupId partitionGroupId = getPartitionGroupId(evt);

                    return super.handleChangeStableAssignmentEvent(evt)
                            .whenComplete((v, e) -> {
                                if (partitionGroupId == null) {
                                    return;
                                }

                                CompletableFuture<Void> finishFuture = finishHandleChangeStableAssignmentEventFutures.get(partitionGroupId);

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

            tableManager.setStreamerReceiverRunner(mock(StreamerReceiverRunner.class));

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
            ComponentContext componentContext = new ComponentContext();

            deployWatchesFut = startComponentsAsync(
                    componentContext,
                    threadPoolsManager,
                    vaultManager,
                    nodeCfgMgr,
                    failureManager,
                    clusterService,
                    logStorageFactory,
                    cmgLogStorageFactory,
                    msLogStorageFactory,
                    raftManager,
                    cmgManager,
                    lowWatermark
            ).thenComposeAsync(
                    v -> cmgManager.joinFuture()
            ).thenApplyAsync(v -> startComponentsAsync(
                    componentContext,
                    metaStorageManager,
                    clusterCfgMgr,
                    clockWaiter,
                    catalogManager,
                    indexMetaStorage,
                    distributionZoneManager,
                    replicaManager,
                    txManager,
                    dataStorageMgr,
                    schemaSafeTimeTracker,
                    schemaManager,
                    sharedTxStateStorage,
                    partitionReplicaLifecycleManager,
                    tableManager,
                    indexManager
            )).thenComposeAsync(componentFuts -> {
                CompletableFuture<Void> configurationNotificationFut = metaStorageManager.recoveryFinishedFuture()
                        .thenCompose(rev -> allOf(
                                ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart(),
                                componentFuts
                        ));

                assertThat(configurationNotificationFut, willSucceedIn(1, TimeUnit.MINUTES));

                lowWatermark.scheduleUpdates();

                return metaStorageManager.deployWatches();
            });
        }

        private CompletableFuture<Void> startComponentsAsync(ComponentContext componentContext, IgniteComponent... components) {
            var componentStartFutures = new CompletableFuture[components.length];

            for (int compIdx = 0; compIdx < components.length; compIdx++) {
                IgniteComponent component = components[compIdx];
                componentStartFutures[compIdx] = component.startAsync(componentContext);
                nodeComponents.add(component);
            }

            return allOf(componentStartFutures);
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
            List<IgniteComponent> components = new ArrayList<>(nodeComponents);
            reverse(components);

            for (IgniteComponent component : components) {
                try {
                    component.beforeNodeStop();
                } catch (Exception e) {
                    LOG.error("Unable to execute before node stop [component={}]", e, component);
                }
            }

            assertThat(stopAsync(new ComponentContext(), components), willCompleteSuccessfully());

            nodeCfgGenerator.close();
            clusterCfgGenerator.close();
        }

        @Nullable PartitionGroupId getPartitionGroupId(WatchEvent event) {
            assertTrue(event.single(), event.toString());

            Entry stableAssignmentsWatchEvent = event.entryEvent().newEntry();

            if (stableAssignmentsWatchEvent.value() == null) {
                return null;
            }

            if (colocationEnabled()) {
                return ZoneRebalanceUtil.extractZonePartitionId(
                        stableAssignmentsWatchEvent.key(),
                        ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES);
            } else {
                return extractTablePartitionId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX_BYTES);
            }
        }

        PartitionGroupId getPartitionGroupId(String tableName, int partitionId) {
            InternalTable internalTable = getInternalTable(this, tableName);

            return partitionReplicationGroupId(internalTable, partitionId);
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

    private static void checkInvokeDestroyedPartitionStorages(Node node, String tableName, int partitionId) throws Exception {
        InternalTable internalTable = getInternalTable(node, tableName);

        if (colocationEnabled()) {
            // Assert that zone tx state storage was removed. Wait for the async destroy operation to complete.
            assertTrue(waitForCondition(
                    () -> node.partitionReplicaLifecycleManager.txStatePartitionStorage(internalTable.zoneId(), partitionId) == null,
                    AWAIT_TIMEOUT_MILLIS
            ), "Zone tx state storage was not destroyed within timeout");
        } else {
            verify(internalTable.storage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                    .destroyPartition(partitionId);
            verify(internalTable.txStateStorage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                    .destroyPartitionStorage(partitionId);
        }
    }

    private static void throwExceptionOnInvokeDestroyPartitionStorages(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        doAnswer(answer -> CompletableFuture.failedFuture(new StorageException("From test")))
                .when(internalTable.storage())
                .destroyPartition(partitionId);

        doAnswer(answer -> CompletableFuture.failedFuture(new IgniteInternalException("From test")))
                .when(internalTable.txStateStorage())
                .destroyPartitionStorage(partitionId);
    }

    private void prepareFinishHandleChangeStableAssignmentEventFuture(Node node, String tableName, int partitionId) {
        InternalTable table = getInternalTable(node, tableName);

        PartitionGroupId partitionGroupId = partitionReplicationGroupId(table, partitionId);

        node.finishHandleChangeStableAssignmentEventFutures.put(partitionGroupId, new CompletableFuture<>());
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

            InternalTable table = getInternalTable(node, tableName);

            PartitionGroupId partitionGroupId = partitionReplicationGroupId(table, partitionId);

            CompletableFuture<Void> future = node.finishHandleChangeStableAssignmentEventFutures.get(partitionGroupId);

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
                SqlCommon.DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT64).build(),
                        ColumnParams.builder().name("val").type(INT32).nullable(true).build()
                ),
                List.of("key")
        );
    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    private Stream<String> getNodeNames(Predicate<Node> filter) {
        return getNodes(filter)
                .map(n -> n.clusterService.nodeName());
    }

    private Stream<Node> getNodes(Predicate<Node> filter) {
        return nodes.stream().filter(filter).distinct();
    }

    private Predicate<Node> notIn(Set<Assignment> assignments) {
        return in(assignments).negate();
    }

    private Predicate<Node> in(Set<Assignment> assignments) {
        return node -> {
            String nodeName = node.clusterService.nodeName();
            return assignments.stream().anyMatch(a -> a.consistentId().equals(nodeName));
        };
    }

    private void checkPartitionNodes(int partitionId, int expNodeCount) {
        for (Node node : nodes) {
            assertEquals(expNodeCount, getPartitionStableAssignments(node, partitionId).size(), node.name);
        }
    }

    private void checkPartitionNodes(String tableName, int partitionId, int expNodeCount) {
        for (Node node : nodes) {
            assertEquals(expNodeCount, getPartitionStableAssignments(node, tableName, partitionId).size(), node.name);
        }
    }

    private void checkRebalanceRetryDelay(int expectedRaftNodesCount, int delay) throws InterruptedException {
        Supplier<List<Integer>> eventListenerRebalanceDelayValues = () -> nodes
                .stream()
                // Collect all raft group events listeners adapters
                .flatMap((n) -> {
                    List<JraftGroupEventsListener> nodeRaftGroupServices = new ArrayList<>();
                    n.raftManager.forEach((nodeId, raftGroupService) -> {
                        CatalogManager catalogManager = nodes.get(0).catalogManager;
                        // Excluded default zone raft services.
                        if (!hasDefaultZone(catalogManager)
                                || (hasDefaultZone(catalogManager)
                                && !raftGroupService.getGroupId().startsWith("" + defaultZoneId(catalogManager)))) {
                            nodeRaftGroupServices.add(raftGroupService.getNodeOptions().getRaftGrpEvtsLsnr());
                        }
                    });

                    return nodeRaftGroupServices.stream();
                })
                // Get the real raft group events listeners
                .map(l -> IgniteTestUtils.getFieldValue(l, "delegate"))
                .map(listener -> {
                    if (listener instanceof ZoneRebalanceRaftGroupEventsListener) {
                        return ((ZoneRebalanceRaftGroupEventsListener) listener).currentRetryDelay();
                    } else if (listener instanceof RebalanceRaftGroupEventsListener) {
                        return ((RebalanceRaftGroupEventsListener) listener).currentRetryDelay();
                    } else {
                        // This value can be used, because configuration framework checks delay for positive value.
                        return -1;
                    }
                })
                .filter(d -> !d.equals(-1))
                .collect(Collectors.toUnmodifiableList());

        assertTrue(waitForCondition(
                () -> {
                    List<Integer> delays = eventListenerRebalanceDelayValues.get();
                    return delays.size() == expectedRaftNodesCount && delays.stream().allMatch(d -> d.equals(delay));
                },
                10_000
        ));
    }

    private static CompletableFuture<Void> updateRebalanceRetryDelay(SystemDistributedConfiguration systemDistributedConfiguration,
            int delay) {
        return systemDistributedConfiguration
                .change(c0 -> c0
                        .changeProperties()
                        .createOrUpdate(
                                REBALANCE_RETRY_DELAY_MS,
                                c1 -> c1.changePropertyValue(String.valueOf(delay))
                        )
                );
    }
}
