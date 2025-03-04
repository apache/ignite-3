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
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
abstract class AbstractZoneReplicationTest extends IgniteAbstractTest {
    private static final int BASE_PORT = 20_000;

    protected static final String TEST_ZONE_NAME = "TEST_ZONE";

    protected static final String TEST_TABLE_NAME1 = "TEST_TABLE_1";

    protected static final String TEST_TABLE_NAME2 = "TEST_TABLE_2";

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

    @InjectConfiguration
    private static GcConfiguration gcConfiguration;

    @InjectConfiguration
    private SqlLocalConfiguration sqlLocalConfiguration;

    @InjectConfiguration
    private SqlDistributedConfiguration sqlDistributedConfiguration;

    @InjectExecutorService
    private static ScheduledExecutorService scheduledExecutorService;

    final List<Node> cluster = new ArrayList<>();

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

    protected void startCluster(int size) throws Exception {
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

    protected Node addNodeToCluster() {
        Node node = newNode(new NetworkAddress("localhost", BASE_PORT + cluster.size()), nodeFinder);

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
                systemConfiguration,
                raftConfiguration,
                nodeAttributesConfiguration,
                storageConfiguration,
                metaStorageConfiguration,
                replicationConfiguration,
                txConfiguration,
                scheduledExecutorService,
                null,
                gcConfiguration,
                sqlLocalConfiguration,
                sqlDistributedConfiguration
        );
    }

    protected int createZone(String zoneName, int partitions, int replicas) {
        return createZoneWithProfile(zoneName, partitions, replicas, DEFAULT_STORAGE_PROFILE);
    }

    protected int createZoneWithProfile(String zoneName, int partitions, int replicas, String profile) {
        Node node = cluster.get(0);

        createZoneWithStorageProfile(
                node.catalogManager,
                zoneName,
                partitions,
                replicas,
                profile
        );

        return getZoneId(node.catalogManager, zoneName, node.hybridClock.nowLong());
    }

    protected int createTable(String zoneName, String tableName) {
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
}
