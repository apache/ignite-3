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

import static java.util.stream.Collectors.toUnmodifiableList;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.InternalClusterNodeImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.awaitility.Awaitility;
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

    /** Should be greater than 2 x {@link #LW_UPDATE_TIME_MS}. */
    private static final long COMPACTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

    /** Transactions that are started in the test. */
    private final List<InternalTransaction> transactions = new ArrayList<>();

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
                + "      intervalMillis: " + CHECK_POINT_INTERVAL_MS
                + "    } } "
                + "  },\n"
                + "  clientConnector.port: {},\n"
                + "  rest.port: {},\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String clusterConfiguration = format(
                "ignite { gc: {lowWatermark: { dataAvailabilityTimeMillis: {}, updateIntervalMillis: {} } } }",
                // dataAvailabilityTime is 2 x updateFrequency by default
                LW_UPDATE_TIME_MS * 2, LW_UPDATE_TIME_MS
        );

        builder.clusterConfiguration(clusterConfiguration);
    }

    @BeforeAll
    void setup() {
        List<Ignite> nodes = CLUSTER.runningNodes().collect(Collectors.toList());
        assertEquals(initialNodes(), nodes.size());
    }

    @BeforeEach
    public void beforeEach() {
        transactions.forEach(Transaction::rollback);
        transactions.clear();

        dropAllTables();
    }

    @Test
    void testGlobalMinimumTxRequiredTime() {
        IgniteImpl node0 = unwrapIgniteImpl(CLUSTER.node(0));
        IgniteImpl node1 = unwrapIgniteImpl(CLUSTER.node(1));
        IgniteImpl node2 = unwrapIgniteImpl(CLUSTER.node(2));

        List<CatalogCompactionRunner> compactors = List.of(
                node0.catalogCompactionRunner(),
                node1.catalogCompactionRunner(),
                node2.catalogCompactionRunner()
        );

        Catalog catalog1 = getLatestCatalog(node2);

        Transaction tx1 = beginTx(node0, false);

        // Changing the catalog and starting transaction.
        sql("create table a(a int primary key)");
        Catalog catalog2 = getLatestCatalog(node0);
        assertThat(catalog2.version(), is(catalog1.version() + 1));
        List<Transaction> txs2 = Stream.of(node1, node2).map(node -> beginTx(node, false)).collect(Collectors.toList());
        List<InternalTransaction> ignoredReadonlyTxs = Stream.of(node0, node1, node2)
                .map(node -> beginTx(node, true))
                .collect(Collectors.toList());

        // Changing the catalog again and starting transaction.
        sql("alter table a add column (b int)");

        Awaitility.await().untilAsserted(() -> assertThat(getLatestCatalogVersion(node1), is(catalog2.version() + 1)));
        Catalog catalog3 = getLatestCatalog(node1);

        List<Transaction> txs3 = Stream.of(node0, node2).map(node -> beginTx(node, false)).collect(Collectors.toList());

        Collection<InternalClusterNode> topologyNodes = node0.cluster().nodes().stream()
                .map(InternalClusterNodeImpl::fromPublicClusterNode)
                .collect(toUnmodifiableList());

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.txMinRequiredTime, is(catalog1.time()));
        });

        tx1.rollback();

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.txMinRequiredTime, is(catalog2.time()));
        });

        txs2.forEach(Transaction::commit);

        compactors.forEach(compactor -> {
            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));
            assertThat(timeHolder.txMinRequiredTime, is(catalog3.time()));
        });

        txs3.forEach(Transaction::rollback);

        // Since there are no active RW transactions in the cluster, the minimum time will be min(now()) across all nodes.
        compactors.forEach(compactor -> {
            long minTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            TimeHolder timeHolder = await(compactor.determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            long maxTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            // Read-only transactions are not counted,
            ignoredReadonlyTxs.forEach(tx -> {
                assertThat(timeHolder.txMinRequiredTime, greaterThan(tx.schemaTimestamp().longValue()));
            });

            assertThat(timeHolder.txMinRequiredTime, greaterThanOrEqualTo(minTime));
            assertThat(timeHolder.txMinRequiredTime, lessThanOrEqualTo(maxTime));
        });

        ignoredReadonlyTxs.forEach(Transaction::rollback);
    }

    @Test
    public void testCompactionRun() {
        sql(format("create zone if not exists test (partitions {}, replicas {}) storage profiles ['default']",
                CLUSTER_SIZE, CLUSTER_SIZE)
        );

        sql("alter zone test set default");

        sql("create table a(a int primary key)");
        sql("alter table a add column b int");

        Ignite ignite = CLUSTER.aliveNode();

        log.info("Awaiting for the first compaction to run...");

        int catalogVersion1 = getLatestCatalogVersion(ignite);
        expectEarliestCatalogVersion(catalogVersion1 - 1);

        log.info("Awaiting for the second compaction to run...");

        sql("alter table a add column c int");

        int catalogVersion2 = getLatestCatalogVersion(ignite);
        assertTrue(catalogVersion1 < catalogVersion2, "Catalog version should have changed");

        expectEarliestCatalogVersion(catalogVersion2 - 1);

        sql("drop table a");

        log.info("Awaiting for the third compaction to run...");

        int catalogVersion3 = getLatestCatalogVersion(ignite);
        assertTrue(catalogVersion2 < catalogVersion3, "Catalog version should have changed");

        expectEarliestCatalogVersion(catalogVersion3 - 1);
    }

    @Test
    void droppedZoneCompacted() {
        String zoneName = "zone1";
        String tableName = "table1";
        sql(format(
                "create zone {} (partitions {}) storage profiles ['{}','{}']",
                zoneName,
                10,
                DEFAULT_AIMEM_PROFILE_NAME,
                DEFAULT_ROCKSDB_PROFILE_NAME
        ));
        sql(format("create table {}(id int primary key) zone {}", tableName, zoneName));
        sql(format("drop table {}", tableName));
        sql(format("drop zone {}", zoneName));

        sql(format("alter zone {} set auto scale up {}", "\"Default\"", 10));

        int catalogVersion = getLatestCatalogVersion(node(0));
        expectEarliestCatalogVersion(catalogVersion - 1);
    }

    private InternalTransaction beginTx(Ignite node, boolean readOnly) {
        TransactionOptions txOptions = new TransactionOptions().readOnly(readOnly);
        InternalTransaction tx = (InternalTransaction) node.transactions().begin(txOptions);

        transactions.add(tx);

        return tx;
    }

    private static int getLatestCatalogVersion(Ignite ignite) {
        Catalog catalog = getLatestCatalog(ignite);

        return catalog.version();
    }

    private static Catalog getLatestCatalog(Ignite ignite) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        CatalogManagerImpl catalogManager = ((CatalogManagerImpl) igniteImpl.catalogManager());

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(igniteImpl.clock().nowLong()));

        Objects.requireNonNull(catalog);

        return catalog;
    }

    private static void expectEarliestCatalogVersion(int expectedVersion) {
        Awaitility.await().pollInSameThread().timeout(COMPACTION_INTERVAL_MS, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            for (var node : CLUSTER.runningNodes().collect(Collectors.toList())) {
                IgniteImpl ignite = unwrapIgniteImpl(node);
                CatalogManagerImpl catalogManager = ((CatalogManagerImpl) ignite.catalogManager());

                assertThat("The earliest catalog version does not match. ",
                        catalogManager.earliestCatalogVersion(), is(expectedVersion));
            }
        });
    }
}
