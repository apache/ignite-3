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

package org.apache.ignite.internal.partition.replicator;

import static java.util.Collections.reverse;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.BaseIgniteRestartTest.createVault;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.REBALANCE_SCHEDULER_POOL_SIZE;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.FEATURE_FLAG_NAME;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
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
import org.apache.ignite.internal.components.LogSyncer;
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
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.utils.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.raft.util.SharedLogStorageFactoryUtils;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
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
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Replica lifecycle test.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
@Timeout(60)
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
public class ItReplicaLifecycleTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItReplicaLifecycleTest.class);

    private static final int NODE_COUNT = 3;

    private static final int BASE_PORT = 20_000;

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration clusterManagementConfiguration;

    @InjectConfiguration("mock.nodeAttributes: {region.attribute = US, storage.attribute = SSD}")
    private static NodeAttributesConfiguration nodeAttributes1;

    @InjectConfiguration("mock.nodeAttributes: {region.attribute = EU, storage.attribute = SSD}")
    private static NodeAttributesConfiguration nodeAttributes2;

    @InjectConfiguration("mock.nodeAttributes: {region.attribute = UK, storage.attribute = SSD}")
    private static NodeAttributesConfiguration nodeAttributes3;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    private StaticNodeFinder finder;

    private static final String HOST = "localhost";

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = \"aipersist\", test.engine=\"test\"}")
    private static StorageConfiguration storageConfiguration;

    private final List<NetworkAddress> nodeAddresses = new ArrayList<>();

    private final List<NodeAttributesConfiguration> nodeAttributesConfigurations = new ArrayList<>(
            List.of(nodeAttributes1, nodeAttributes2, nodeAttributes3)
    );

    /**
     * Interceptor of {@link MetaStorageManager#invoke(Condition, Collection, Collection)}.
     */
    private final Map<Integer, InvokeInterceptor> metaStorageInvokeInterceptorByNode = new ConcurrentHashMap<>();

    @WorkDirectory
    private Path workDir;

    private Map<Integer, Node> nodes;

    private TestPlacementDriver placementDriver;

    private static String featureFlagOldValue = System.getProperty(FEATURE_FLAG_NAME);

    @BeforeAll
    static void beforeAll() {
        System.setProperty(FEATURE_FLAG_NAME, "true");
    }

    @AfterAll
    static void afterAll() {
        if (featureFlagOldValue == null) {
            System.clearProperty(FEATURE_FLAG_NAME);
        } else {
            System.setProperty(FEATURE_FLAG_NAME, featureFlagOldValue);
        }
    }

    @BeforeEach
    void before(TestInfo testInfo) {
        nodes = new HashMap<>();

        placementDriver = new TestPlacementDriver();

        for (int i = 0; i < NODE_COUNT; i++) {
            nodeAddresses.add(new NetworkAddress(HOST, BASE_PORT + i));
        }

        finder = new StaticNodeFinder(nodeAddresses);
    }

    @AfterEach
    void after() {
        metaStorageInvokeInterceptorByNode.clear();
        nodes.values().forEach(Node::stop);
    }

    private void startNodes(TestInfo testInfo, int amount) throws NodeStoppingException, InterruptedException {
        for (int i = 0; i < amount; i++) {
            var node = new Node(testInfo, i);

            nodes.put(i, node);

            node.start();
        }

        Node node0 = getNode(0);

        node0.cmgManager.initCluster(List.of(node0.name), List.of(node0.name), "cluster");

        nodes.values().forEach(Node::waitWatches);

        assertThat(
                allOf(nodes.values().stream().map(n -> n.cmgManager.onJoinReady()).toArray(CompletableFuture[]::new)),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(
                () -> {
                    CompletableFuture<LogicalTopologySnapshot> logicalTopologyFuture = node0.cmgManager.logicalTopology();

                    assertThat(logicalTopologyFuture, willCompleteSuccessfully());

                    return logicalTopologyFuture.join().nodes().size() == amount;
                },
                AWAIT_TIMEOUT_MILLIS
        ));
    }

    private Node startNode(TestInfo testInfo, int idx) {
        var node = new Node(testInfo, idx);

        nodes.put(idx, node);

        node.start();

        node.waitWatches();

        assertThat(node.cmgManager.onJoinReady(), willCompleteSuccessfully());

        return node;
    }

    private void stopNode(int idx) {
        Node node = getNode(idx);

        node.stop();

        nodes.remove(idx);
    }

    @Test
    public void testEmptyReplicaListener(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) AffinityUtils.calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(Collectors.toList()), 0, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 1, 1);
        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        createTable(node, "test_zone", "test_table");
        int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

        node.converter.put(new TablePartitionId(tableId, 0), new ZonePartitionId(zoneId, 0));

        KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

        assertDoesNotThrow(() -> keyValueView.put(null, 1L, 1));

        // Actually we are testing not the fair put value, but the hardcoded one from temporary noop replica listener
        assertEquals(-1, keyValueView.get(null, 1L));
    }

    @Test
    void testAlterReplicaTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        createZone(node, "test_zone", 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        alterZone(catalogManager, "test_zone", 2);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );
    }

    @Test
    void testAlterReplicaTriggerDefaultZone(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        CatalogManager catalogManager = node.catalogManager;

        int zoneId = defaultZoneIdOpt(catalogManager.catalog(catalogManager.latestCatalogVersion()));

        String defaultZoneName = catalogManager.zone(zoneId, catalogManager.latestCatalogVersion()).name();

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                1,
                20_000L
        );

        alterZone(catalogManager, defaultZoneName, 2);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );
    }

    @Test
    void testAlterReplicaExtensionTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        assertTrue(waitForCondition(() -> node.distributionZoneManager.logicalTopology().size() == 3, 10_000L));

        createZone(node, "test_zone", 2, 2);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                2,
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        alterZone(catalogManager, "test_zone", 3);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );
    }

    @Test
    void testAlterFilterTrigger(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node = getNode(0);

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        CatalogManager catalogManager = node.catalogManager;

        String newFilter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        alterZone(catalogManager, "test_zone", null, null, newFilter);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(nodes.get(0).name),
                20_000L
        );
    }

    @Test
    void testReplicaIsStartedOnNodeStart(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Node node0 = getNode(0);

        createZone(node0, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        stopNode(2);

        Node node2 = startNode(testInfo, 2);

        assertTrue(waitForCondition(() -> node2.replicaManager.isReplicaStarted(partId), 10_000L));
    }

    @Test
    void testStableAreWrittenAfterRestart(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 1);

        Node node0 = getNode(0);

        AtomicBoolean reached = new AtomicBoolean();

        metaStorageInvokeInterceptorByNode.put(0, (condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, STABLE_ASSIGNMENTS_PREFIX)) {
                reached.set(true);

                return true;
            }

            return null;
        });

        createZone(node0, "test_zone", 2, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertTrue(reached.get());

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                Assignments::fromBytes,
                null,
                20_000L
        );

        stopNode(0);

        metaStorageInvokeInterceptorByNode.clear();

        startNodes(testInfo, 1);

        node0 = getNode(0);

        metaStorageManager = node0.metaStorageManager;

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        assertTrue(waitForCondition(() -> getNode(0).replicaManager.isReplicaStarted(partId), 10_000L));
    }

    @Test
    void testStableAreWrittenAfterRestartAndConcurrentStableUpdate(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 1);

        Node node0 = getNode(0);

        AtomicBoolean reached = new AtomicBoolean();

        metaStorageInvokeInterceptorByNode.put(0, (condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, STABLE_ASSIGNMENTS_PREFIX)) {
                reached.set(true);

                return true;
            }

            return null;
        });

        createZone(node0, "test_zone", 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node0.catalogManager, "test_zone", node0.hybridClock.nowLong());

        MetaStorageManager metaStorageManager = node0.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        assertTrue(reached.get());

        reached.set(false);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                Assignments::fromBytes,
                null,
                20_000L
        );

        stopNode(0);

        CountDownLatch latch = new CountDownLatch(1);

        metaStorageInvokeInterceptorByNode.put(0, (condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, stablePartAssignmentsKey(partId).toString())) {
                reached.set(true);

                Node node = nodes.get(0);

                node.metaStorageManager.put(stablePartAssignmentsKey(partId), Assignments.of(Assignment.forPeer(node.name)).toBytes());
            }

            return null;
        });

        startNodes(testInfo, 1);

        node0 = getNode(0);

        metaStorageManager = node0.metaStorageManager;

        assertTrue(reached.get());

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                nodes.values().stream().map(n -> n.name).collect(Collectors.toSet()),
                20_000L
        );

        assertTrue(waitForCondition(() -> getNode(0).replicaManager.isReplicaStarted(partId), 10_000L));
    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    private Node getNode(String nodeName) {
        return nodes.values().stream().filter(n -> n.name.equals(nodeName)).findFirst().get();
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

    class Node {
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

        private final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

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

        private final Map<ReplicationGroupId, ReplicationGroupId> converter = new ConcurrentHashMap<>();

        private final LogStorageFactory logStorageFactory;

        private final IndexMetaStorage indexMetaStorage;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(TestInfo testInfo, int idx) {
            networkAddress = nodeAddresses.get(idx);

            name = testNodeName(testInfo, networkAddress.port());

            Path dir = workDir.resolve(name);

            vaultManager = createVault(dir);

            nodeCfgGenerator = new ConfigurationTreeGenerator(
                    List.of(
                            NetworkConfiguration.KEY,
                            StorageConfiguration.KEY
                    ),
                    List.of(
                            PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                            VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class
                    ),
                    List.of(
                            PersistentPageMemoryProfileConfigurationSchema.class,
                            VolatilePageMemoryProfileConfigurationSchema.class
                    )
            );

            Path configPath = workDir.resolve(testInfo.getDisplayName());
            TestIgnitionManager.addDefaultsToConfigurationFile(configPath);

            nodeCfgMgr = new ConfigurationManager(
                    List.of(NetworkConfiguration.KEY,
                            StorageConfiguration.KEY),
                    new LocalFileConfigurationStorage(configPath, nodeCfgGenerator, null),
                    nodeCfgGenerator,
                    new TestConfigurationValidator()
            );

            clusterService = ClusterServiceTestUtils.clusterService(
                    testInfo,
                    networkAddress.port(),
                    finder
            );

            lockManager = new HeapLockManager();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            logStorageFactory = SharedLogStorageFactoryUtils.create(clusterService.nodeName(), dir, raftConfiguration);

            raftManager = new Loza(
                    clusterService,
                    new NoOpMetricManager(),
                    raftConfiguration,
                    dir,
                    hybridClock,
                    raftGroupEventsClientListener,
                    logStorageFactory
            );

            var clusterStateStorage = new TestClusterStateStorage();
            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            var clusterInitializer = new ClusterInitializer(
                    clusterService,
                    hocon -> hocon,
                    new TestConfigurationValidator()
            );

            failureProcessor = new NoOpFailureProcessor();

            cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    clusterInitializer,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    clusterManagementConfiguration,
                    new NodeAttributesCollector(nodeAttributesConfigurations.get(idx), storageConfiguration),
                    failureProcessor
            );

            LogicalTopologyServiceImpl logicalTopologyService = new LogicalTopologyServiceImpl(logicalTopology, cmgManager);

            KeyValueStorage keyValueStorage = new SimpleInMemoryKeyValueStorage(name);

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
                    new NoOpMetricManager(),
                    metaStorageConfiguration,
                    raftConfiguration.retryTimeout(),
                    completedFuture(() -> DEFAULT_MAX_CLOCK_SKEW_MS)
            ) {
                @Override
                public CompletableFuture<Boolean> invoke(
                        Condition condition,
                        Collection<Operation> success,
                        Collection<Operation> failure
                ) {
                    InvokeInterceptor invokeInterceptor = metaStorageInvokeInterceptorByNode.get(idx);

                    if (invokeInterceptor != null) {
                        var res = invokeInterceptor.invoke(condition, success, failure);

                        if (res != null) {
                            return completedFuture(res);
                        }
                    }

                    return super.invoke(condition, success, failure);
                }
            };

            threadPoolsManager = new ThreadPoolsManager(name);

            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier = () -> 10L;

            ReplicaService replicaSvc = new ReplicaService(
                    clusterService.messagingService(),
                    hybridClock,
                    threadPoolsManager.partitionOperationsExecutor(),
                    replicationConfiguration,
                    threadPoolsManager.commonScheduler()
            );

            var resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            clockWaiter = new ClockWaiter(name, hybridClock);

            ClockService clockService = new ClockServiceImpl(
                    hybridClock,
                    clockWaiter,
                    () -> TestIgnitionManager.DEFAULT_MAX_CLOCK_SKEW_MS
            );

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService);

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
                    new PersistentPageMemoryDataStorageModule()
            ));

            Path storagePath = dir.resolve("storage");

            LogSyncer logSyncer = logStorageFactory;

            dataStorageMgr = new DataStorageManager(
                    dataStorageModules.createStorageEngines(
                            name,
                            nodeCfgMgr.configurationRegistry(),
                            dir.resolve("storage"),
                            null,
                            failureProcessor,
                            logSyncer
                    ),
                    storageConfiguration
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
                    new TransactionIdGenerator(networkAddress.port()),
                    placementDriver,
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new TestLocalRwTxCounter(),
                    resourcesRegistry,
                    transactionInflights,
                    lowWatermark
            );

            replicaManager = new ReplicaManager(
                    name,
                    clusterService,
                    cmgManager,
                    clockService,
                    Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                    placementDriver,
                    threadPoolsManager.partitionOperationsExecutor(),
                    partitionIdleSafeTimePropagationPeriodMsSupplier,
                    new NoOpFailureProcessor(),
                    new ThreadLocalPartitionCommandsMarshaller(clusterService.serializationRegistry()),
                    topologyAwareRaftGroupServiceFactory,
                    raftManager,
                    view -> new LocalLogStorageFactory(),
                    ForkJoinPool.commonPool(),
                    t -> (converter.get(t) != null) ? converter.get(t) : t
            );

            LongSupplier delayDurationMsSupplier = () -> 10L;

            catalogManager = new CatalogManagerImpl(
                    new UpdateLogImpl(metaStorageManager),
                    clockService,
                    delayDurationMsSupplier,
                    partitionIdleSafeTimePropagationPeriodMsSupplier
            );

            indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageManager);

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

            partitionReplicaLifecycleManager = new PartitionReplicaLifecycleManager(
                    catalogManager,
                    replicaManager,
                    distributionZoneManager,
                    metaStorageManager,
                    clusterService.topologyService(),
                    lowWatermark,
                    threadPoolsManager.tableIoExecutor(),
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
                    threadPoolsManager.tableIoExecutor(),
                    threadPoolsManager.partitionOperationsExecutor(),
                    rebalanceScheduler,
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
                    lowWatermark,
                    transactionInflights,
                    indexMetaStorage,
                    logSyncer
            );

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
                    logStorageFactory,
                    raftManager,
                    cmgManager
            );

            ComponentContext componentContext = new ComponentContext();

            List<CompletableFuture<?>> componentFuts =
                    firstComponents.stream()
                            .map(component -> component.startAsync(componentContext))
                            .collect(Collectors.toList());

            nodeComponents.addAll(firstComponents);

            deployWatchesFut = CompletableFuture.supplyAsync(() -> {
                List<IgniteComponent> secondComponents = List.of(
                        lowWatermark,
                        metaStorageManager,
                        clusterCfgMgr,
                        clockWaiter,
                        catalogManager,
                        indexMetaStorage,
                        distributionZoneManager,
                        replicaManager,
                        txManager,
                        dataStorageMgr,
                        schemaManager,
                        partitionReplicaLifecycleManager,
                        tableManager,
                        indexManager
                );

                componentFuts.addAll(secondComponents.stream()
                        .map(component -> component.startAsync(componentContext)).collect(Collectors.toList()));

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
    }

    @FunctionalInterface
    private interface InvokeInterceptor {
        Boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure);
    }

    private static boolean skipMetaStorageInvoke(Collection<Operation> ops, String prefix) {
        return ops.stream().anyMatch(op -> new String(toByteArray(op.key()), StandardCharsets.UTF_8).startsWith(prefix));
    }
}
