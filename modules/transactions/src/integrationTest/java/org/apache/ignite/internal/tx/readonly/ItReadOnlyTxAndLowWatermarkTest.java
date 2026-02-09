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

package org.apache.ignite.internal.tx.readonly;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapInternalTransaction;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class ItReadOnlyTxAndLowWatermarkTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    // 100 keys to make sure that at least one key ends up on every of 2 nodes.
    private static final int KEY_COUNT = 100;

    private static final long SHORT_DATA_AVAILABILITY_TIME_MS = 1000;

    @Override
    protected int initialNodes() {
        // 2 nodes to have a non-coordinator node in cluster.
        return 2;
    }

    @Override
    protected int[] cmgMetastoreNodes() {
        return new int[] {0};
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration("ignite.gc.lowWatermark: {\n"
                // Update frequently.
                + "  updateIntervalMillis: 100\n"
                + "}");
    }

    @BeforeEach
    void createTable() {
        node(0).sql().executeScript("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR)");
    }

    @ParameterizedTest
    @EnumSource(TransactionalReader.class)
    void roTransactionNoticesTupleVersionsMissingDueToGcOnDataNodes(TransactionalReader reader) throws Exception {
        updateDataAvailabilityTimeToShortPeriod();

        Ignite coordinator = node(0);
        KeyValueView<Integer, String> kvView = kvView(coordinator);

        insertOriginalValuesToBothNodes(KEY_COUNT, kvView);

        Transaction roTx = coordinator.transactions().begin(new TransactionOptions().readOnly(true));

        updateToNewValues(KEY_COUNT, kvView);

        waitTillLwmTriesToRaiseAndEraseOverrittenVersions();

        IgniteException ex = assertThrows(IgniteException.class, () -> reader.read(coordinator, roTx));
        assertThat(ex, isA(reader.sql() ? SqlException.class : TransactionException.class));
        assertThat(ex, hasToString(
                either(containsString("Read timestamp is not available anymore."))
                        .or(containsString("Transaction is already finished"))
        ));
        assertThat("Wrong error code: " + ex.codeAsString(), ex.code(),
                either(is(Transactions.TX_STALE_READ_ONLY_OPERATION_ERR))
                        .or(is(Transactions.TX_ALREADY_FINISHED_ERR))
        );
    }

    private void updateDataAvailabilityTimeToShortPeriod() {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node(0));

        LowWatermarkConfiguration lwmConfig = igniteImpl.clusterConfiguration()
                .getConfiguration(GcExtensionConfiguration.KEY)
                .gc()
                .lowWatermark();

        assertThat(lwmConfig.dataAvailabilityTimeMillis().update(SHORT_DATA_AVAILABILITY_TIME_MS), willCompleteSuccessfully());
    }

    private static KeyValueView<Integer, String> kvView(Ignite coordinator) {
        return coordinator.tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);
    }

    private void insertOriginalValuesToBothNodes(int keyCount, KeyValueView<Integer, String> kvView) throws Exception {
        PartitionDistribution partDistribution = node(0).tables().table(TABLE_NAME).partitionDistribution();
        Set<String> primaryNames = new HashSet<>();

        for (int i = 0; i < keyCount; i++) {
            kvView.put(null, i, "original-" + i);

            if (primaryNames.size() < 2) {
                ClusterNode primaryReplica = primaryReplicaFor(i, partDistribution);
                primaryNames.add(primaryReplica.name());
            }
        }

        assertThat("Expecting both nodes to host inserted keys", primaryNames, hasSize(2));
    }

    private static ClusterNode primaryReplicaFor(int key, PartitionDistribution partitionDistribution) throws Exception {
        CompletableFuture<ClusterNode> primaryReplicaFuture = partitionDistribution.partitionAsync(key, Mapper.of(Integer.class))
                .thenCompose(partitionDistribution::primaryReplicaAsync);

        return primaryReplicaFuture.get(10, SECONDS);
    }

    private static void updateToNewValues(int keyCount, KeyValueView<Integer, String> kvView) {
        for (int i = 0; i < keyCount; i++) {
            kvView.put(null, i, "updated-" + i);
        }
    }

    private static void waitTillLwmTriesToRaiseAndEraseOverrittenVersions() throws InterruptedException {
        Thread.sleep(2 * SHORT_DATA_AVAILABILITY_TIME_MS);
    }

    @CartesianTest
    @WithSystemProperty(key = ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "100")
    void lwmIsAllowedToBeRaisedOnDataNodesAfterRoTransactionFinish(
            @Enum(TransactionalReader.class) TransactionalReader reader,
            @Values(booleans = {true, false}) boolean commit
    ) throws Exception {
        Ignite coordinator = node(0);
        KeyValueView<Integer, String> kvView = kvView(coordinator);

        insertOriginalValuesToBothNodes(KEY_COUNT, kvView);

        Transaction roTx = coordinator.transactions().begin(new TransactionOptions().readOnly(true));

        reader.read(coordinator, roTx);

        if (commit) {
            roTx.commit();
        } else {
            roTx.rollback();
        }

        updateDataAvailabilityTimeToShortPeriod();

        HybridTimestamp readTimestamp = unwrapInternalTransaction(roTx).readTimestamp();

        assertLwmGrowsAbove(readTimestamp, node(0));
        assertLwmGrowsAbove(readTimestamp, node(1));
    }

    private static void assertLwmGrowsAbove(HybridTimestamp ts, Ignite node) throws InterruptedException {
        LowWatermarkImpl lowWatermark = unwrapIgniteImpl(node).lowWatermark();

        assertTrue(
                waitForCondition(
                        () -> {
                            HybridTimestamp lwm = lowWatermark.getLowWatermark();
                            return lwm != null && lwm.compareTo(ts) > 0;
                        },
                        SECONDS.toMillis(10)
                ),
                "Did not see low watermark going up in time"
        );
    }

    @ParameterizedTest
    @EnumSource(TransactionalReader.class)
    @WithSystemProperty(key = ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "100")
    void nonFinishedRoTransactionsOfCoordinatorsThatLeftDontHoldLwm(TransactionalReader reader) throws Exception {
        Ignite coordinator = node(1);
        KeyValueView<Integer, String> kvView = kvView(coordinator);

        insertOriginalValuesToBothNodes(KEY_COUNT, kvView);

        Transaction roTx = coordinator.transactions().begin(new TransactionOptions().readOnly(true));

        // Do actual read(s) in the transaction.
        reader.read(coordinator, roTx);

        // Stop the coordinator.
        stopNode(1);

        updateDataAvailabilityTimeToShortPeriod();

        HybridTimestamp readTimestamp = unwrapInternalTransaction(roTx).readTimestamp();

        assertLwmGrowsAbove(readTimestamp, node(0));
    }

    private enum TransactionalReader {
        SINGLE_GETS {
            @Override
            void read(Ignite ignite, Transaction transaction) {
                KeyValueView<Integer, String> kvView = kvView(ignite);
                for (int i = 0; i < KEY_COUNT; i++) {
                    kvView.get(transaction, i);
                }
            }

            @Override
            boolean sql() {
                return false;
            }
        },
        MULTI_GET {
            @Override
            void read(Ignite ignite, Transaction transaction) {
                List<Integer> keys = IntStream.range(0, KEY_COUNT).boxed().collect(toList());
                kvView(ignite).getAll(transaction, keys);
            }

            @Override
            boolean sql() {
                return false;
            }
        },
        SELECT_ALL {
            @Override
            void read(Ignite ignite, Transaction transaction) {
                try (ResultSet<SqlRow> resultSet = ignite.sql().execute(transaction, "SELECT * FROM " + TABLE_NAME)) {
                    resultSet.forEachRemaining(item -> {});
                }
            }

            @Override
            boolean sql() {
                return true;
            }
        },
        SELECT_COUNT {
            @Override
            void read(Ignite ignite, Transaction transaction) {
                try (ResultSet<SqlRow> resultSet = ignite.sql().execute(transaction, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    resultSet.forEachRemaining(item -> {});
                }
            }

            @Override
            boolean sql() {
                return true;
            }
        };

        abstract void read(Ignite ignite, Transaction transaction);

        abstract boolean sql();
    }
}
