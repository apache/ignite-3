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
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZoneWithStorageProfile;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getZoneId;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.partition.replicator.fixtures.TestPlacementDriver;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.Member;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
public class ItZoneDataReplicationTest extends IgniteAbstractTest {
    private static final int BASE_PORT = 20_000;

    private static final String TEST_ZONE_NAME = "TEST_ZONE";

    private static final String TEST_TABLE_NAME1 = "TEST_TABLE_1";

    private static final String TEST_TABLE_NAME2 = "TEST_TABLE_2";

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    @InjectConfiguration
    private static TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static SystemLocalConfiguration systemConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributesConfiguration;

    @InjectConfiguration
    private static ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private static MetaStorageConfiguration metaStorageConfiguration;

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = aipersist, test.engine=test}")
    private static StorageConfiguration storageConfiguration;

    @InjectExecutorService
    private static ScheduledExecutorService scheduledExecutorService;

    private final List<Node> cluster = new ArrayList<>();

    private final TestPlacementDriver placementDriver = new TestPlacementDriver();

    private NodeFinder nodeFinder;

    private TestInfo testInfo;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(cluster.parallelStream().map(node -> node::stop));
    }

    private void startCluster(int size) throws Exception {
        List<NetworkAddress> addresses = IntStream.range(0, size)
                .mapToObj(i -> new NetworkAddress("localhost", BASE_PORT + i))
                .collect(toList());

        nodeFinder = new StaticNodeFinder(addresses);

        IntStream.range(0, size)
                .mapToObj(i -> newNode(addresses.get(i), nodeFinder))
                .forEach(cluster::add);

        cluster.parallelStream().forEach(Node::start);

        Node node0 = cluster.get(0);

        node0.cmgManager.initCluster(List.of(node0.name), List.of(node0.name), "cluster");

        setPrimaryReplica(node0, null);

        cluster.forEach(Node::waitWatches);

        assertThat(
                allOf(cluster.stream().map(n -> n.cmgManager.onJoinReady()).toArray(CompletableFuture[]::new)),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(
                () -> {
                    CompletableFuture<LogicalTopologySnapshot> logicalTopologyFuture = node0.cmgManager.logicalTopology();

                    assertThat(logicalTopologyFuture, willCompleteSuccessfully());

                    return logicalTopologyFuture.join().nodes().size() == cluster.size();
                },
                30_000
        ));
    }

    private Node addNodeToCluster(Function<ReplicaRequest, ReplicationGroupId> requestConverter) {
        Node node = newNode(new NetworkAddress("localhost", BASE_PORT + cluster.size()), nodeFinder);

        node.setRequestConverter(requestConverter);

        cluster.add(node);

        node.start();

        node.waitWatches();

        assertThat(node.cmgManager.onJoinReady(), willCompleteSuccessfully());

        return node;
    }

    private Node newNode(NetworkAddress address, NodeFinder nodeFinder) {
        return new Node(
                testInfo,
                address,
                nodeFinder,
                workDir,
                placementDriver,
                systemConfiguration,
                raftConfiguration,
                nodeAttributesConfiguration,
                storageConfiguration,
                metaStorageConfiguration,
                replicationConfiguration,
                txConfiguration,
                scheduledExecutorService,
                null
        );
    }

    private int createZone(String zoneName, int partitions, int replicas) {
        Node node = cluster.get(0);

        createZoneWithStorageProfile(
                node.catalogManager,
                zoneName,
                partitions,
                replicas,
                DEFAULT_STORAGE_PROFILE
        );

        return getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());
    }

    private int createTable(String zoneName, String tableName) {
        Node node = cluster.get(0);

        TableTestUtils.createTable(
                node.catalogManager,
                DEFAULT_SCHEMA_NAME,
                zoneName,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT32).build(),
                        ColumnParams.builder().name("val").type(INT32).nullable(true).build()
                ),
                List.of("key")
        );

        return getTableId(node.catalogManager, tableName, node.hybridClock.nowLong());
    }

    /**
     * Tests that inserted data is replicated to all replica nodes.
     */
    @ParameterizedTest(name = "useExplicitTx={0}")
    @ValueSource(booleans = {false, true})
    void testReplicationOnAllNodes(boolean useExplicitTx) throws Exception {
        startCluster(3);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size());

        int tableId1 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        int tableId2 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        setupTableIdToZoneIdConverter(zonePartitionId, new TablePartitionId(tableId1, 0), new TablePartitionId(tableId2, 0));

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        setPrimaryReplica(node, zonePartitionId);

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        // Test single insert.
        if (useExplicitTx) {
            node.transactions().runInTransaction(tx -> {
                kvView1.put(tx, 42, 69);
                kvView2.put(tx, 142, 169);
            });
        } else {
            kvView1.put(null, 42, 69);
            kvView2.put(null, 142, 169);
        }

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            if (useExplicitTx) {
                node.transactions().runInTransaction(tx -> {
                    assertThat(n.name, kvView1.get(tx, 42), is(69));
                    assertThat(n.name, kvView1.get(tx, 142), is(nullValue()));

                    assertThat(n.name, kvView2.get(tx, 42), is(nullValue()));
                    assertThat(n.name, kvView2.get(tx, 142), is(169));
                });
            } else {
                assertThat(n.name, kvView1.get(null, 42), is(69));
                assertThat(n.name, kvView1.get(null, 142), is(nullValue()));

                assertThat(n.name, kvView2.get(null, 42), is(nullValue()));
                assertThat(n.name, kvView2.get(null, 142), is(169));
            }
        }

        // Test batch insert.
        Map<Integer, Integer> data1 = IntStream.range(0, 10).boxed().collect(toMap(Function.identity(), Function.identity()));
        Map<Integer, Integer> data2 = IntStream.range(10, 20).boxed().collect(toMap(Function.identity(), Function.identity()));

        if (useExplicitTx) {
            node.transactions().runInTransaction(tx -> {
                kvView1.putAll(tx, data1);
                kvView2.putAll(tx, data2);
            });
        } else {
            kvView1.putAll(null, data1);
            kvView2.putAll(null, data2);
        }

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            if (useExplicitTx) {
                node.transactions().runInTransaction(tx -> {
                    assertThat(n.name, kvView1.getAll(tx, data1.keySet()), is(data1));
                    assertThat(n.name, kvView1.getAll(tx, data2.keySet()), is(anEmptyMap()));

                    assertThat(n.name, kvView2.getAll(tx, data1.keySet()), is(anEmptyMap()));
                    assertThat(n.name, kvView2.getAll(tx, data2.keySet()), is(data2));
                });
            } else {
                assertThat(n.name, kvView1.getAll(null, data1.keySet()), is(data1));
                assertThat(n.name, kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

                assertThat(n.name, kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
                assertThat(n.name, kvView2.getAll(null, data2.keySet()), is(data2));
            }
        }
    }

    /**
     * Tests that inserted data is replicated to a newly joined replica node.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24394")
    @ParameterizedTest(name = "truncateRaftLog={0}")
    @ValueSource(booleans = {false, true})
    void testDataRebalance(boolean truncateRaftLog) throws Exception {
        assumeFalse(truncateRaftLog, "https://issues.apache.org/jira/browse/IGNITE-22416");

        startCluster(2);

        // Create a zone with a single partition on every node + one extra replica for the upcoming node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size() + 1);

        int tableId1 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        int tableId2 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        Function<ReplicaRequest, ReplicationGroupId> requestConverter =
                requestConverter(zonePartitionId, new TablePartitionId(tableId1, 0), new TablePartitionId(tableId2, 0));

        cluster.forEach(node -> {
            node.setRequestConverter(requestConverter);
            node.waitForMetadataCompletenessAtNow();
        });

        Node node = cluster.get(0);

        setPrimaryReplica(node, zonePartitionId);

        Map<Integer, Integer> data1 = IntStream.range(0, 10).boxed().collect(toMap(Function.identity(), Function.identity()));
        Map<Integer, Integer> data2 = IntStream.range(10, 20).boxed().collect(toMap(Function.identity(), Function.identity()));

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        kvView1.putAll(null, data1);
        kvView2.putAll(null, data2);

        if (truncateRaftLog) {
            truncateLogOnEveryNode(zonePartitionId);
        }

        Node newNode = addNodeToCluster(requestConverter);

        // Wait for the rebalance to kick in.
        assertTrue(waitForCondition(() -> newNode.replicaManager.isReplicaStarted(zonePartitionId), 10_000L));

        setPrimaryReplica(newNode, zonePartitionId);

        assertThat(kvView1.getAll(null, data1.keySet()), is(data1));
        assertThat(kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

        assertThat(kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
        assertThat(kvView2.getAll(null, data2.keySet()), is(data2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void txFinishCommandGetsReplicated(boolean commit) throws Exception {
        startCluster(3);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size());

        int tableId1 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        int tableId2 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        setupTableIdToZoneIdConverter(zonePartitionId, new TablePartitionId(tableId1, 0), new TablePartitionId(tableId2, 0));

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        setPrimaryReplica(node, zonePartitionId);

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        Transaction transaction = node.transactions().begin();
        kvView1.put(transaction, 42, 69);
        kvView2.put(transaction, 142, 169);
        if (commit) {
            transaction.commit();
        } else {
            transaction.rollback();
        }

        for (Node currentNode : cluster) {
            assertTrue(waitForCondition(
                    () -> !txStatesInPartitionStorage(currentNode.txStatePartitionStorage(zoneId, 0)).isEmpty(),
                    SECONDS.toMillis(10)
            ));
        }

        List<Executable> assertions = new ArrayList<>();
        for (int i = 0; i < cluster.size(); i++) {
            int finalI = i;
            Node currentNode = cluster.get(finalI);

            assertions.add(() -> assertTxStateStorageAsExpected(
                    "Node " + finalI + " zone",
                    currentNode.txStatePartitionStorage(zoneId, 0),
                    1,
                    commit
            ));
            assertions.add(() -> assertTxStateStorageAsExpected(
                    "Node " + finalI + " table1",
                    tableTxStatePartitionStorage(currentNode, tableId1, 0),
                    0,
                    commit
            ));
            assertions.add(() -> assertTxStateStorageAsExpected(
                    "Node " + finalI + " table2",
                    tableTxStatePartitionStorage(currentNode, tableId2, 0),
                    0,
                    commit
            ));
        }

        assertAll(assertions);
    }

    private static void assertTxStateStorageAsExpected(
            String storageName,
            TxStatePartitionStorage txStatePartitionStorage,
            int expectedCount,
            boolean commit
    ) {
        List<TxState> txStates = txStatesInPartitionStorage(txStatePartitionStorage);

        assertThat("For " + storageName, txStates, hasSize(expectedCount));
        assertThat(txStates, everyItem(is(commit ? TxState.COMMITTED : TxState.ABORTED)));
    }

    private static List<TxState> txStatesInPartitionStorage(TxStatePartitionStorage txStatePartitionStorage) {
        return IgniteTestUtils.bypassingThreadAssertions(() -> {
            try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = txStatePartitionStorage.scan()) {
                return cursor.stream()
                        .map(pair -> pair.get2().txState())
                        .collect(toList());
            }
        });
    }

    private static TxStatePartitionStorage tableTxStatePartitionStorage(Node node, int tableId1, int partitionId)
            throws NodeStoppingException {
        InternalTable internalTable1 = node.tableManager.table(tableId1).internalTable();
        TxStatePartitionStorage txStatePartitionStorage = internalTable1.txStateStorage().getPartitionStorage(partitionId);

        assertThat(txStatePartitionStorage, is(notNullValue()));

        return txStatePartitionStorage;
    }

    private void setupTableIdToZoneIdConverter(ZonePartitionId zonePartitionId, TablePartitionId... tablePartitionIds) {
        Function<ReplicaRequest, ReplicationGroupId> requestConverter = requestConverter(zonePartitionId, tablePartitionIds);

        cluster.forEach(node -> node.setRequestConverter(requestConverter));
    }

    private static Function<ReplicaRequest, ReplicationGroupId> requestConverter(
            ZonePartitionId zonePartitionId, TablePartitionId... tablePartitionIds
    ) {
        Set<ReplicationGroupId> tablePartitionIdsSet = Set.of(tablePartitionIds);

        return request ->  {
            ReplicationGroupId replicationGroupId = request.groupId().asReplicationGroupId();

            if (tablePartitionIdsSet.contains(replicationGroupId) && !(request instanceof WriteIntentSwitchReplicaRequest)) {
                return zonePartitionId;
            } else {
                return replicationGroupId;
            }
        };
    }

    private void setPrimaryReplica(Node node, @Nullable ZonePartitionId zonePartitionId) {
        ClusterNode newPrimaryReplicaNode = node.clusterService.topologyService().localMember();

        HybridTimestamp leaseStartTime = node.hybridClock.now();

        placementDriver.setPrimary(newPrimaryReplicaNode, leaseStartTime);

        if (zonePartitionId != null) {
            PrimaryReplicaChangeCommand cmd = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                    .primaryReplicaNodeId(newPrimaryReplicaNode.id())
                    .primaryReplicaNodeName(newPrimaryReplicaNode.name())
                    .leaseStartTime(leaseStartTime.longValue())
                    .build();

            CompletableFuture<Void> primaryReplicaChangeFuture = node.replicaManager
                    .replica(zonePartitionId)
                    .thenCompose(replica -> replica.raftClient().run(cmd));

            assertThat(primaryReplicaChangeFuture, willCompleteSuccessfully());
        }
    }

    private void truncateLogOnEveryNode(ReplicationGroupId groupId) {
        CompletableFuture<?>[] truncateFutures = cluster.stream()
                .map(node -> truncateLog(node, groupId))
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(truncateFutures), willCompleteSuccessfully());
    }

    private static CompletableFuture<Void> truncateLog(Node node, ReplicationGroupId groupId) {
        Member member = Member.votingMember(node.name);

        // Using the infamous trick of triggering snapshot twice to cause Raft log truncation.
        return node.replicaManager.replica(groupId)
                .thenCompose(replica -> replica.createSnapshotOn(member).thenCompose(v -> replica.createSnapshotOn(member)));
    }
}
