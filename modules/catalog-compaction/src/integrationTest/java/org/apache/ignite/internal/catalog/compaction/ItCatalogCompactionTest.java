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

package org.apache.ignite.internal.catalog.compaction;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner.TimeHolder;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests to verify catalog compaction.
 */
class ItCatalogCompactionTest extends ClusterPerClassIntegrationTest {
    private static final int CLUSTER_SIZE = 3;

    /** How often we update the low water mark. */
    private static final long LW_UPDATE_TIME_MS = TimeUnit.MILLISECONDS.toMillis(500);

    /**
     * Checkpoint interval determines how often we update a snapshot of minActiveTxBeginTime,
     * it should be less than {@link #LW_UPDATE_TIME_MS} for the test to work.
     */
    private static final long CHECK_POINT_INTERVAL_MS = LW_UPDATE_TIME_MS / 2;

    /** Show be greater than 2 x {@link #LW_UPDATE_TIME_MS}. */
    private static final long COMPACTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

    @Override
    protected int initialNodes() {
        return CLUSTER_SIZE;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  storage.profiles: {"
                + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
                + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
                + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
                + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
                + "  },\n"
                + "  storage.engines: { "
                + "    aipersist: { checkpoint: { "
                + "      interval: " + CHECK_POINT_INTERVAL_MS
                + "    } } "
                + "  },\n"
                + "  clientConnector.port: {},\n"
                + "  rest.port: {},\n"
                + "  compute.threadPoolSize: 1\n"
                + "}";
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String clusterConfiguration = format(
                "ignite { gc: {lowWatermark: { dataAvailabilityTime: {}, updateInterval: {} } } }",
                // dataAvailabilityTime is 2 x updateFrequency by default
                LW_UPDATE_TIME_MS * 2, LW_UPDATE_TIME_MS
        );

        builder.clusterConfiguration(clusterConfiguration);
    }

    @BeforeAll
    void enableCompaction() {
        List<Ignite> nodes = CLUSTER.runningNodes().collect(Collectors.toList());
        assertEquals(initialNodes(), nodes.size());

        for (var node : nodes) {
            IgniteImpl ignite = unwrapIgniteImpl(node);
            ignite.catalogCompactionRunner().enable(true);
        }
    }

    @BeforeEach
    public void beforeEach() {
        dropAllTables();
    }

    @Test
    void testGlobalMinimumTxBeginTime() {
        IgniteImpl node0 = unwrapIgniteImpl(CLUSTER.node(0));
        IgniteImpl node1 = unwrapIgniteImpl(CLUSTER.node(1));
        IgniteImpl node2 = unwrapIgniteImpl(CLUSTER.node(2));

        List<CatalogCompactionRunner> compactors = List.of(
                node0.catalogCompactionRunner(),
                node1.catalogCompactionRunner(),
                node2.catalogCompactionRunner()
        );

        Collection<ClusterNode> topologyNodes = node0.clusterNodes();

        InternalTransaction tx1 = (InternalTransaction) node0.transactions().begin();
        InternalTransaction tx2 = (InternalTransaction) node1.transactions().begin();
        InternalTransaction readonlyTx = (InternalTransaction) node1.transactions().begin(new TransactionOptions().readOnly(true));
        InternalTransaction tx3 = (InternalTransaction) node2.transactions().begin();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx1.startTimestamp().longValue()));
        });

        tx1.rollback();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx2.startTimestamp().longValue()));
        });

        tx2.commit();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.minActiveTxBeginTime, is(tx3.startTimestamp().longValue()));
        });

        tx3.rollback();

        // Since there are no active RW transactions in the cluster, the minimum time will be min(now()) across all nodes.
        compactors.forEach(compactor -> {
            long minTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            long maxTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            // Read-only transactions are not counted,
            assertThat(timeHolder.minActiveTxBeginTime, greaterThan(readonlyTx.startTimestamp().longValue()));

            assertThat(timeHolder.minActiveTxBeginTime, greaterThanOrEqualTo(minTime));
            assertThat(timeHolder.minActiveTxBeginTime, lessThanOrEqualTo(maxTime));
        });

        readonlyTx.rollback();
    }

    @Test
    public void testCompactionRun() throws InterruptedException {
        sql(format("create zone if not exists test with partitions={}, replicas={}, storage_profiles='default'",
                CLUSTER_SIZE, CLUSTER_SIZE)
        );

        sql("alter zone test set default");

        sql("create table a(a int primary key)");
        sql("alter table a add column b int");

        IgniteImpl ignite = unwrapIgniteImpl(CLUSTER.aliveNode());
        CatalogManagerImpl catalogManager = ((CatalogManagerImpl) ignite.catalogManager());

        log.info("Awaiting for the first compaction to run...");

        Catalog catalog1 = catalogManager.catalog(catalogManager.activeCatalogVersion(ignite.clock().nowLong()));
        assertNotNull(catalog1);

        expectEarliestCatalogVersion(catalog1.version() - 1);

        log.info("Awaiting for the second compaction to run...");

        sql("alter table a add column c int");

        Catalog catalog2 = catalogManager.catalog(catalogManager.activeCatalogVersion(ignite.clock().nowLong()));
        assertNotNull(catalog2);

        assertTrue(catalog1.version() < catalog2.version(), "Catalog version should have changed");

        expectEarliestCatalogVersion(catalog2.version() - 1);
    }

    private static void waitPrimaryReplicas(List<TablePartitionId> groups) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.aliveNode());
        List<CompletableFuture<?>> waitFutures = new ArrayList<>(groups.size());

        for (TablePartitionId groupId : groups) {
            CompletableFuture<ReplicaMeta> waitFut = node.placementDriver()
                    .awaitPrimaryReplica(groupId, node.clock().now(), 10, TimeUnit.SECONDS);

            waitFutures.add(waitFut);
        }

        await(CompletableFutures.allOf(waitFutures));
    }

    private static List<TablePartitionId> prepareExpectedGroups(CatalogManagerImpl catalogManager, int partsCount) {
        IgniteImpl ignite = unwrapIgniteImpl(CLUSTER.aliveNode());

        Catalog lastCatalog = catalogManager.catalog(
                catalogManager.activeCatalogVersion(ignite.clock().nowLong()));
        assertNotNull(lastCatalog);

        Collection<CatalogTableDescriptor> tables = lastCatalog.tables();
        assertThat(tables, hasSize(2));

        List<TablePartitionId> expected = new ArrayList<>(partsCount * tables.size());

        tables.forEach(tab -> {
            for (int p = 0; p < partsCount; p++) {
                expected.add(new TablePartitionId(tab.id(), p));
            }
        });

        return expected;
    }

    private static void expectEarliestCatalogVersion(int expectedVersion) throws InterruptedException {
        long waitTime = COMPACTION_INTERVAL_MS;

        boolean compacted = IgniteTestUtils.waitForCondition(() -> {
            for (var node : CLUSTER.runningNodes().collect(Collectors.toList())) {
                IgniteImpl ignite = unwrapIgniteImpl(node);
                CatalogManagerImpl catalogManager = ((CatalogManagerImpl) ignite.catalogManager());

                if (catalogManager.earliestCatalogVersion() != expectedVersion) {
                    return false;
                }
            }

            return true;
        }, 1000, waitTime);

        assertTrue(compacted, "The earliest catalog version does not match. Wait time ms=" + waitTime);
    }
}
