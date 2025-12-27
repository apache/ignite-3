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
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner.TimeHolder;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.tx.ActiveLocalTxMinimumRequiredTimeProvider;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
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

        DebugInfoCollector debug = new DebugInfoCollector(List.of(node0, node1, node2));

        List<CatalogCompactionRunner> compactors = List.of(
                node0.catalogCompactionRunner(),
                node1.catalogCompactionRunner(),
                node2.catalogCompactionRunner()
        );

        debug.recordGlobalTxState("init");
        debug.recordCatalogState("init");
        debug.recordMinTxTimesState("init");

        Catalog catalog1 = getLatestCatalog(node2);
        InternalTransaction tx1 = beginTx(node0, false);
        debug.recordTx(tx1);

        // Changing the catalog and starting transaction.
        sql("create table a(a int primary key)");
        Catalog catalog2 = getLatestCatalog(node0);
        assertThat(catalog2.version(), is(catalog1.version() + 1));
        List<InternalTransaction> txs2 = Stream.of(node1, node2).map(node -> beginTx(node, false)).collect(Collectors.toList());
        List<InternalTransaction> ignoredReadonlyTxs = Stream.of(node0, node1, node2)
                .map(node -> beginTx(node, true))
                .collect(Collectors.toList());

        debug.recordTx(txs2);
        debug.recordTx(ignoredReadonlyTxs);

        // Changing the catalog again and starting transaction.
        sql("alter table a add column (b int)");

        Awaitility.await().until(() -> getLatestCatalogVersion(node1), is(catalog2.version() + 1));
        Catalog catalog3 = getLatestCatalog(node1);

        List<InternalTransaction> txs3 = Stream.of(node0, node2).map(node -> beginTx(node, false)).collect(Collectors.toList());

        debug.recordTx(txs3);

        Collection<InternalClusterNode> topologyNodes = node0.cluster().nodes().stream()
                .map(ClusterNodeImpl::fromPublicClusterNode)
                .collect(toUnmodifiableList());

        for (int i = 0; i < compactors.size(); i++) {
            TimeHolder timeHolder = await(compactors.get(i).determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            String failureMessage = "Initial condition failed on node #" + i;

            assertEquals(catalog1.time(), timeHolder.txMinRequiredTime, () -> debug.dumpDebugInfo(failureMessage));
        }

        tx1.rollback();

        for (int i = 0; i < compactors.size(); i++) {
            TimeHolder timeHolder = await(compactors.get(i).determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            String failureMessage = "Condition failed after first tx rollback on node #" + i;

            assertEquals(catalog2.time(), timeHolder.txMinRequiredTime, () -> debug.dumpDebugInfo(failureMessage));
        }

        txs2.forEach(Transaction::commit);

        for (int i = 0; i < compactors.size(); i++) {
            TimeHolder timeHolder = await(compactors.get(i).determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            String failureMessage = "Condition failed after transactions commit on node #" + i;

            assertEquals(catalog3.time(), timeHolder.txMinRequiredTime, () -> debug.dumpDebugInfo(failureMessage));
        }

        txs3.forEach(Transaction::rollback);

        // Since there are no active RW transactions in the cluster, the minimum time will be min(now()) across all nodes.
        for (int i = 0; i < compactors.size(); i++) {
            long minTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            TimeHolder timeHolder = await(compactors.get(i).determineGlobalMinimumRequiredTime(topologyNodes, 0L));

            long maxTime = Stream.of(node0, node1, node2).map(node -> node.clockService().nowLong()).min(Long::compareTo).orElseThrow();

            // Read-only transactions are not counted,
            ignoredReadonlyTxs.forEach(tx -> {
                assertThat(timeHolder.txMinRequiredTime, greaterThan(tx.schemaTimestamp().longValue()));
            });

            long actual = timeHolder.txMinRequiredTime;

            String failureMessage = "node #" + i + ": " + minTime + " <= " + actual + " <= " + maxTime;

            assertTrue(minTime <= actual && actual <= maxTime, () -> debug.dumpDebugInfo(failureMessage));
        }

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

    private class DebugInfoCollector {
        private final List<IgniteImpl> nodes;
        private final Map<UUID, String> nodesById;
        private final IgniteStringBuilder buffer = new IgniteStringBuilder("Test debug info").nl();
        private final List<InternalTransaction> transactions = new ArrayList<>();

        DebugInfoCollector(List<IgniteImpl> nodes) {
            this.nodes = nodes;
            Map<UUID, String> nodesById = new HashMap<>();

            for (IgniteImpl node : nodes) {
                nodesById.put(node.id(), node.name());
            }

            this.nodesById = nodesById;
        }

        void recordCatalogState(String contextMessage) {
            buffer.nl();

            for (IgniteImpl node : nodes) {
                buffer.app("Catalog state(").app(contextMessage)
                        .app(") on node ").app(node.name()).nl();

                CatalogManager mgr = node.catalogManager();

                for (int ver = mgr.earliestCatalogVersion(); ver <= mgr.latestCatalogVersion(); ver++) {
                    Catalog catalog = mgr.catalog(ver);

                    buffer.app("  ").app(ver).app(" | ").app(catalog == null ? -1 : catalog.time()).nl();
                }
            }
        }

        void recordMinTxTimesState(String contextMessage) {
            buffer.nl();

            buffer.app("Minimum RW tx times (").app(contextMessage).app(')').nl();

            for (IgniteImpl node : nodes) {
                ActiveLocalTxMinimumRequiredTimeProvider timeProvider = node.catalogCompactionRunner()
                        .activeLocalTxMinimumRequiredTimeProvider();

                buffer.app("  ").app(node.name()).app(": ").app(timeProvider.minimumRequiredTime()).nl();
            }
        }

        void recordGlobalTxState(String contextMessage) {
            buffer.nl();

            buffer.app("System transactions state (").app(contextMessage).app(')').nl();

            for (IgniteImpl node : nodes) {
                TxManager txManager = node.txManager();

                buffer.app("  ").app(node.name())
                        .app(": pending=").app(txManager.pending())
                        .app(", finished=").app(txManager.finished())
                        .nl();
            }
        }

        void recordTransactionsState() {
            // Sort by start time.
            transactions.sort(Comparator.comparing(t -> beginTimestamp(t.id())));

            List<InternalTransaction> roTransactions = new ArrayList<>();
            List<InternalTransaction> rwTransactions = new ArrayList<>();

            for (InternalTransaction tx : transactions) {
                if (tx.isReadOnly()) {
                    roTransactions.add(tx);
                } else {
                    rwTransactions.add(tx);
                }
            }

            buffer.nl();
            buffer.app("RW transactions state").nl();

            for (InternalTransaction tx : rwTransactions) {
                buffer.app("  ")
                        .app(tx.isFinishingOrFinished() ? "finished" : "active  ").app(" | ")
                        .app(nodesById.get(tx.coordinatorId())).app(" | ")
                        .app(beginTimestamp(tx.id()))
                        .nl();
            }

            buffer.nl();
            buffer.app("RO transactions state").nl();

            for (InternalTransaction tx : roTransactions) {
                buffer.app("  ")
                        .app(tx.isFinishingOrFinished() ? "finished" : "active  ").app(" | ")
                        .app(beginTimestamp(tx.id()))
                        .nl();
            }
        }

        void recordTx(InternalTransaction tx) {
            transactions.add(tx);
        }

        void recordTx(List<InternalTransaction> txs) {
            transactions.addAll(txs);
        }

        String dumpDebugInfo(String messageHeader) {
            recordGlobalTxState("onFailure");
            recordCatalogState("onFailure");
            recordMinTxTimesState("onFailure");
            recordTransactionsState();

            String debugInfo = messageHeader + System.lineSeparator() + System.lineSeparator() + buffer.toString();

            log.info(debugInfo);

            return debugInfo;
        }
    }
}
