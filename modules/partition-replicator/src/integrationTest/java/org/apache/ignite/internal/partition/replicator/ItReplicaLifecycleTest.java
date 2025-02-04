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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.partition.replicator.fixtures.Node.InvokeInterceptor;
import org.apache.ignite.internal.partition.replicator.fixtures.TestPlacementDriver;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Replica lifecycle test.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
@Timeout(60)
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
public class ItReplicaLifecycleTest extends IgniteAbstractTest {
    private static final int NODE_COUNT = 3;

    private static final int BASE_PORT = 20_000;

    private static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static SystemLocalConfiguration systemConfiguration;

    @InjectConfiguration("mock.nodeAttributes: {region = US, storage = SSD}")
    private static NodeAttributesConfiguration nodeAttributes1;

    @InjectConfiguration("mock.nodeAttributes: {region = EU, storage = SSD}")
    private static NodeAttributesConfiguration nodeAttributes2;

    @InjectConfiguration("mock.nodeAttributes: {region = UK, storage = SSD}")
    private static NodeAttributesConfiguration nodeAttributes3;

    @InjectConfiguration
    private static ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = aipersist, test.engine=test}")
    private static StorageConfiguration storageConfiguration;

    @InjectExecutorService
    private static ScheduledExecutorService scheduledExecutorService;

    private static final String HOST = "localhost";

    private static final List<NetworkAddress> NODE_ADDRESSES = IntStream.range(0, NODE_COUNT)
            .mapToObj(i -> new NetworkAddress(HOST, BASE_PORT + i))
            .collect(toList());

    private static final StaticNodeFinder NODE_FINDER = new StaticNodeFinder(NODE_ADDRESSES);

    private static List<NodeAttributesConfiguration> NODE_ATTRIBUTES_CONFIGURATIONS;

    private final Map<Integer, Node> nodes = new HashMap<>();

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    @BeforeAll
    static void beforeAll() {
        NODE_ATTRIBUTES_CONFIGURATIONS = List.of(nodeAttributes1, nodeAttributes2, nodeAttributes3);
    }

    @AfterEach
    void after() throws Exception {
        closeAll(nodes.values().parallelStream().map(node -> node::stop));
    }

    private void startNodes(TestInfo testInfo, int amount) throws NodeStoppingException, InterruptedException {
        startNodes(testInfo, amount, null);
    }

    private void startNodes(
            TestInfo testInfo,
            int amount,
            @Nullable InvokeInterceptor invokeInterceptor
    ) throws NodeStoppingException, InterruptedException {
        IntStream.range(0, amount).forEach(i -> newNode(testInfo, i, invokeInterceptor));

        assert nodes.size() == amount : "Not all amount of nodes were created.";

        nodes.values().stream().parallel().forEach(Node::start);

        Node node0 = getNode(0);

        node0.cmgManager.initCluster(List.of(node0.name), List.of(node0.name), "cluster");

        placementDriver.setPrimary(node0.clusterService.topologyService().localMember());

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
        return startNode(testInfo, idx, null);
    }

    private Node startNode(TestInfo testInfo, int idx, @Nullable InvokeInterceptor invokeInterceptor) {
        Node node = newNode(testInfo, idx, invokeInterceptor);

        node.start();

        node.waitWatches();

        assertThat(node.cmgManager.onJoinReady(), willCompleteSuccessfully());

        return node;
    }

    private Node newNode(TestInfo testInfo, int idx, @Nullable InvokeInterceptor invokeInterceptor) {
        var node = new Node(
                testInfo,
                NODE_ADDRESSES.get(idx),
                NODE_FINDER,
                workDir,
                placementDriver,
                systemConfiguration,
                raftConfiguration,
                NODE_ATTRIBUTES_CONFIGURATIONS.get(idx),
                storageConfiguration,
                metaStorageConfiguration,
                replicationConfiguration,
                txConfiguration,
                scheduledExecutorService,
                invokeInterceptor
        );

        nodes.put(idx, node);

        return node;
    }

    private void stopNode(int idx) {
        Node node = getNode(idx);

        node.stop();

        nodes.remove(idx);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24374")
    @Test
    public void testZoneReplicaListener(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 1, 1);
        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        long key = 1;

        {
            createTable(node, "test_zone", "test_table");
            int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

            prepareTableIdToZoneIdConverter(
                    node,
                    new TablePartitionId(tableId, 0),
                    new ZonePartitionId(zoneId, 0)
            );

            KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

            int val = 100;

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView.put(tx, key, val));

                assertEquals(val, keyValueView.get(tx, key));
            });

            node.transactions().runInTransaction(tx -> {
                // Check the replica read inside the another transaction
                assertEquals(val, keyValueView.get(tx, key));
            });
        }

        {
            createTable(node, "test_zone", "test_table1");
            int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table1", node.hybridClock.nowLong());

            prepareTableIdToZoneIdConverter(
                    node,
                    new TablePartitionId(tableId, 0),
                    new ZonePartitionId(zoneId, 0)
            );

            KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

            int val = 200;

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView.put(tx, key, val));

                assertEquals(val, keyValueView.get(tx, key));
            });
        }
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

        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        MetaStorageManager metaStorageManager = node.metaStorageManager;

        ZonePartitionId partId = new ZonePartitionId(defaultZone.id(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()).size(),
                1,
                20_000L
        );

        alterZone(catalogManager, defaultZone.name(), 2);

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
    void testTableReplicaListenersCreationAfterRebalance(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        DistributionZonesTestUtil.createZone(node.catalogManager, "test_zone", 1, 1);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 0), 10_000L));

        createTable(node, "test_zone", "test_table");

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 1), 10_000L));

        alterZone(node.catalogManager, "test_zone", 3);

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> assertTableListenersCount(getNode(i), zoneId, 1)),
                30_000L
        ));
    }

    @Test
    void testTableReplicaListenersRemoveAfterRebalance(TestInfo testInfo) throws Exception {
        String zoneName = "TEST_ZONE";
        String tableName = "TEST_TABLE";

        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 3).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        DistributionZonesTestUtil.createZone(node.catalogManager, zoneName, 1, 3);

        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());

        assertTrue(waitForCondition(() -> assertTableListenersCount(node, zoneId, 0), 10_000L));

        createTable(node, zoneName, tableName);

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> getNode(i).tableManager.table(tableName) != null),
                30_000L
        ));

        assertTrue(waitForCondition(
                () -> IntStream.range(0, 3).allMatch(i -> assertTableListenersCount(getNode(i), zoneId, 1)),
                30_000L
        ));

        nodes.values().forEach(n -> checkNoDestroyPartitionStoragesInvokes(n, tableName, 0));

        alterZone(node.catalogManager, zoneName, 1);

        nodes.values().stream().filter(n -> !replicaAssignment.consistentId().equals(n.name)).forEach(n -> {
            checkDestroyPartitionStoragesInvokes(n, tableName, 0);
        });

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

        node0.setInvokeInterceptor((condition, success, failure) -> {
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

        node0.setInvokeInterceptor((condition, success, failure) -> {
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

        startNodes(testInfo, 1, (condition, success, failure) -> {
            if (skipMetaStorageInvoke(success, stablePartAssignmentsKey(partId).toString())) {
                reached.set(true);

                Node node = nodes.get(0);

                int catalogVersion = node.catalogManager.latestCatalogVersion();
                long timestamp = node.catalogManager.catalog(catalogVersion).time();

                node.metaStorageManager.put(
                        stablePartAssignmentsKey(partId),
                        Assignments.of(timestamp, Assignment.forPeer(node.name)).toBytes()
                );
            }

            return null;
        });

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

    @Test
    public void testTableEstimatedSize(TestInfo testInfo) throws Exception {
        startNodes(testInfo, 3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                nodes.values().stream().map(n -> n.name).collect(toList()), 0, 1, 1).toArray()[0];

        Node node = getNode(replicaAssignment.consistentId());

        placementDriver.setPrimary(node.clusterService.topologyService().localMember());

        createZone(node, "test_zone", 1, 1);
        int zoneId = DistributionZonesTestUtil.getZoneId(node.catalogManager, "test_zone", node.hybridClock.nowLong());

        long key = 1;

        {
            createTable(node, "test_zone", "test_table");
            int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

            prepareTableIdToZoneIdConverter(
                    node,
                    new TablePartitionId(tableId, 0),
                    new ZonePartitionId(zoneId, 0)
            );

            KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

            int val = 100;

            node.transactions().runInTransaction(tx -> {
                assertDoesNotThrow(() -> keyValueView.put(tx, key, val));

                assertEquals(val, keyValueView.get(tx, key));
            });

            CompletableFuture<Long> sizeFuture = node.tableManager.table(tableId).internalTable().estimatedSize();

            assertEquals(1, sizeFuture.get());
        }
    }

    private void prepareTableIdToZoneIdConverter(Node node, TablePartitionId tablePartitionId, ZonePartitionId zonePartitionId) {
        node.setRequestConverter(request ->  {
            if (request.groupId().asReplicationGroupId().equals(tablePartitionId)
                    && !(request instanceof WriteIntentSwitchReplicaRequest)) {
                return zonePartitionId;
            } else {
                return request.groupId().asReplicationGroupId();
            }
        });

    }

    private Node getNode(int nodeIndex) {
        return nodes.get(nodeIndex);
    }

    private Node getNode(String nodeName) {
        return nodes.values().stream().filter(n -> n.name.equals(nodeName)).findFirst().orElseThrow();
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

    private static boolean skipMetaStorageInvoke(Collection<Operation> ops, String prefix) {
        return ops.stream().anyMatch(op -> new String(toByteArray(op.key()), StandardCharsets.UTF_8).startsWith(prefix));
    }

    private boolean assertTableListenersCount(Node node, int zoneId, int count) {
        try {
            CompletableFuture<Replica> replicaFut = node.replicaManager.replica(new ZonePartitionId(zoneId, 0));

            if (replicaFut == null) {
                return false;
            }

            Replica replica = replicaFut.get(1, TimeUnit.SECONDS);

            return replica != null && (((ZonePartitionReplicaListener) replica.listener()).tableReplicaListeners().size() == count);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static InternalTable getInternalTable(Node node, String tableName) {
        Table table = node.tableManager.table(tableName);

        assertNotNull(table, tableName);

        return ((TableViewInternal) table).internalTable();
    }

    private static void checkNoDestroyPartitionStoragesInvokes(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), never())
                .destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), never())
                .destroyTxStateStorage(partitionId);
    }

    private static void checkDestroyPartitionStoragesInvokes(Node node, String tableName, int partitionId) {
        InternalTable internalTable = getInternalTable(node, tableName);

        verify(internalTable.storage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyPartition(partitionId);
        verify(internalTable.txStateStorage(), timeout(AWAIT_TIMEOUT_MILLIS).atLeast(1))
                .destroyTxStateStorage(partitionId);
    }
}
