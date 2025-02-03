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
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZoneWithStorageProfile;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getZoneId;
import static org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager.FEATURE_FLAG_NAME;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.getTableId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.KeyValueView;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
@WithSystemProperty(key = FEATURE_FLAG_NAME, value = "true")
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

    @AfterEach
    void tearDown() throws Exception {
        closeAll(cluster.parallelStream().map(node -> node::stop));
    }

    private void startCluster(TestInfo testInfo, int size) throws Exception {
        List<NetworkAddress> addresses = IntStream.range(0, size)
                .mapToObj(i -> new NetworkAddress("localhost", BASE_PORT + i))
                .collect(toList());

        nodeFinder = new StaticNodeFinder(addresses);

        IntStream.range(0, size)
                .mapToObj(i -> newNode(testInfo, addresses.get(i), nodeFinder))
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

    private Node addNodeToCluster(TestInfo testInfo, Function<ReplicaRequest, ReplicationGroupId> requestConverter) {
        Node node = newNode(testInfo, new NetworkAddress("localhost", BASE_PORT + cluster.size()), nodeFinder);

        node.setRequestConverter(requestConverter);

        cluster.add(node);

        node.start();

        node.waitWatches();

        assertThat(node.cmgManager.onJoinReady(), willCompleteSuccessfully());

        return node;
    }

    private Node newNode(TestInfo testInfo, NetworkAddress address, NodeFinder nodeFinder) {
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
    @Test
    void testReplicationOnAllNodes(TestInfo testInfo) throws Exception {
        startCluster(testInfo, 3);

        // Create a zone with a single partition on every node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size());

        int tableId1 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        int tableId2 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        setupTableIdToZoneIdConverter(zonePartitionId, new TablePartitionId(tableId1, 0), new TablePartitionId(tableId2, 0));

        Node node = cluster.get(0);

        setPrimaryReplica(node, zonePartitionId);

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        // Test single insert.
        kvView1.put(null, 42, 69);
        kvView2.put(null, 142, 169);

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            assertThat(kvView1.get(null, 42), is(69));
            assertThat(kvView1.get(null, 142), is(nullValue()));

            assertThat(kvView2.get(null, 42), is(nullValue()));
            assertThat(kvView2.get(null, 142), is(169));
        }

        // Test batch insert.
        Map<Integer, Integer> data1 = IntStream.range(0, 10).boxed().collect(toMap(Function.identity(), Function.identity()));
        Map<Integer, Integer> data2 = IntStream.range(10, 20).boxed().collect(toMap(Function.identity(), Function.identity()));

        kvView1.putAll(null, data1);
        kvView2.putAll(null, data2);

        for (Node n : cluster) {
            setPrimaryReplica(n, zonePartitionId);

            assertThat(kvView1.getAll(null, data1.keySet()), is(data1));
            assertThat(kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

            assertThat(kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
            assertThat(kvView2.getAll(null, data2.keySet()), is(data2));
        }
    }

    /**
     * Tests that inserted data is replicated to a newly joined replica node.
     */
    @Test
    void testDataRebalance(TestInfo testInfo) throws Exception {
        testDataRebalanceImpl(testInfo, false);
    }

    /**
     * Same as {@link #testDataRebalance} but replication is performed using a Raft snapshot instead of Raft log replay.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22416")
    @Test
    void testDataRebalanceUsingRaftSnapshot(TestInfo testInfo) throws Exception {
        testDataRebalanceImpl(testInfo, true);
    }

    private void testDataRebalanceImpl(TestInfo testInfo, boolean truncateRaftLog) throws Exception {
        startCluster(testInfo, 2);

        // Create a zone with a single partition on every node + one extra replica for the upcoming node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size() + 1);

        int tableId1 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        int tableId2 = createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        Function<ReplicaRequest, ReplicationGroupId> requestConverter =
                requestConverter(zonePartitionId, new TablePartitionId(tableId1, 0), new TablePartitionId(tableId2, 0));

        cluster.forEach(node -> node.setRequestConverter(requestConverter));

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

        Node newNode = addNodeToCluster(testInfo, requestConverter);

        // Wait for the rebalance to kick in.
        assertTrue(waitForCondition(() -> newNode.replicaManager.isReplicaStarted(zonePartitionId), 10_000L));

        setPrimaryReplica(newNode, zonePartitionId);

        assertThat(kvView1.getAll(null, data1.keySet()), is(data1));
        assertThat(kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

        assertThat(kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
        assertThat(kvView2.getAll(null, data2.keySet()), is(data2));
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
