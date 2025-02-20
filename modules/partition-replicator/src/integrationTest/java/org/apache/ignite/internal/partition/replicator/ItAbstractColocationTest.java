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
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.partition.replicator.fixtures.TestPlacementDriver;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication

/**
 * Base class for tests that require a cluster with zone replication.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
abstract class ItAbstractColocationTest extends IgniteAbstractTest {
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final int BASE_PORT = 20_000;

    static final int AWAIT_TIMEOUT_MILLIS = 10_000;

    @InjectConfiguration
    private SystemLocalConfiguration systemConfiguration;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private NodeAttributesConfiguration defaultNodeAttributesConfiguration;

    @InjectConfiguration("mock.profiles = {" + DEFAULT_STORAGE_PROFILE + ".engine = aipersist, test.engine=test}")
    private StorageConfiguration storageConfiguration;

    @InjectConfiguration
    private MetaStorageConfiguration metaStorageConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutorService;

    @InjectConfiguration
    GcConfiguration gcConfiguration;

    @InjectConfiguration
    TransactionConfiguration txConfiguration;

    final List<Node> cluster = new ArrayList<>();

    // TODO Remove https://issues.apache.org/jira/browse/IGNITE-24375
    final TestPlacementDriver placementDriver = new TestPlacementDriver();

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

    void startCluster(int size) throws Exception {
        startCluster(size, null, null);
    }

    void startCluster(int size, List<NodeAttributesConfiguration> customAttributes) throws Exception {
        assertThat(customAttributes.size(), equalTo(size));

        startCluster(size, null, customAttributes);
    }

    void startCluster(
            int size,
            @Nullable Node.InvokeInterceptor invokeInterceptor,
            @Nullable List<NodeAttributesConfiguration> customAttributes
    ) throws Exception {
        List<NetworkAddress> addresses = IntStream.range(0, size)
                .mapToObj(i -> new NetworkAddress("localhost", BASE_PORT + i))
                .collect(toList());

        nodeFinder = new StaticNodeFinder(addresses);

        boolean hasCustomAttributes = customAttributes != null;

        IntStream.range(0, size)
                .mapToObj(i -> newNode(
                        addresses.get(i),
                        nodeFinder,
                        invokeInterceptor,
                        hasCustomAttributes ? customAttributes.get(i) : defaultNodeAttributesConfiguration))
                .forEach(cluster::add);

        cluster.parallelStream().forEach(Node::start);

        Node node0 = cluster.get(0);

        node0.cmgManager.initCluster(List.of(node0.name), List.of(node0.name), "cluster");

        placementDriver.setPrimary(node0.clusterService.topologyService().localMember());

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
                AWAIT_TIMEOUT_MILLIS
        ));
    }

    protected Node addNodeToCluster() {
        return addNodeToCluster(cluster.size());
    }

    Node addNodeToCluster(int idx) {
        Node node = newNode(new NetworkAddress("localhost", BASE_PORT + idx), nodeFinder);

        cluster.add(node);

        node.start();

        node.waitWatches();

        assertThat(node.cmgManager.onJoinReady(), willCompleteSuccessfully());

        return node;
    }

    void stopNode(int idx) {
        Node node = getNode(idx);

        node.stop();

        cluster.remove(idx);
    }

    private Node newNode(NetworkAddress address, NodeFinder nodeFinder) {
        return newNode(address, nodeFinder, null, defaultNodeAttributesConfiguration);
    }

    private Node newNode(
            NetworkAddress address,
            NodeFinder nodeFinder,
            @Nullable Node.InvokeInterceptor invokeInterceptor,
            NodeAttributesConfiguration nodeAttributesConfiguration
    ) {
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
                invokeInterceptor,
                gcConfiguration
        );
    }

    static void createZone(Node node, String zoneName, int partitions, int replicas) {
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

    static void createTable(Node node, String zoneName, String tableName) {
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

    Node getNode(int nodeIndex) {
        return cluster.get(nodeIndex);
    }

    Node getNode(String nodeName) {
        return cluster.stream().filter(n -> n.name.equals(nodeName)).findFirst().orElseThrow();
    }

    void setPrimaryReplica(Node node, @Nullable ZonePartitionId zonePartitionId) {
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

    long idleSafeTimePropagationDuration() {
        return replicationConfiguration.idleSafeTimePropagationDuration().value();
    }
}
