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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.message.IndexMessagesFactory;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeResponse;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.schema.TestProfileConfigurationSchema;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For testing {@link IndexNodeFinishedRwTransactionsChecker}. */
public class ItIndexNodeFinishedRwTransactionsCheckerTest extends ClusterPerClassIntegrationTest {
    private static final IndexMessagesFactory FACTORY = new IndexMessagesFactory();

    private static final String TABLE_NAME = "TEST_TABLE";

    private static String zoneNameForUpdateCatalogVersionOnly = "FAKE_TEST_ZONE";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void setUp() {
        if (node() != null) {
            createZoneOnlyIfNotExists(zoneName(TABLE_NAME), 1, 2, TestProfileConfigurationSchema.TEST_PROFILE_NAME);
            createZoneOnlyIfNotExists(zoneNameForUpdateCatalogVersionOnly, 1, 1, TestProfileConfigurationSchema.TEST_PROFILE_NAME);
            createTableOnly(TABLE_NAME, zoneName(TABLE_NAME));
        }
    }

    @AfterEach
    void tearDown() {
        if (node() != null) {
            sql("DROP TABLE IF EXISTS " + TABLE_NAME);
            sql("DROP ZONE IF EXISTS " + zoneName(TABLE_NAME));
            sql("DROP ZONE IF EXISTS " + zoneNameForUpdateCatalogVersionOnly);
        }
    }

    @Test
    void testNoTransactions() {
        int latestCatalogVersion = latestCatalogVersion();

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion - 1));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion));

        assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion + 1));

        fakeUpdateCatalog();
        int newLatestCatalogVersion = latestCatalogVersion();

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(newLatestCatalogVersion));
    }

    @ParameterizedTest(name = "commit = {0}")
    @ValueSource(booleans = {true, false})
    void testEmptyTransaction(boolean commit) {
        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(commit, tx -> {
            assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
            assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion + 1));

            fakeUpdateCatalog();

            assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
            assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
        });

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
    }

    @ParameterizedTest(name = "commit = {0}")
    @ValueSource(booleans = {true, false})
    void testNotEmptyTransaction(boolean commit) {
        insertPeople(TABLE_NAME, new Person(0, "0", 0.0));

        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(commit, tx -> {
            insertPeople(tx, TABLE_NAME, new Person(1, "1", 1.0));

            assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
            assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion + 1));

            fakeUpdateCatalog();

            assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
            assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
        });

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
    }

    @ParameterizedTest(name = "commit = {0}")
    @ValueSource(booleans = {true, false})
    void testTransactionInsertIntoMultiplePartitions(boolean commit) {
        assertThat(tableImpl().internalTable().partitions(), greaterThan(1));

        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(commit, tx -> {
            long[] beforeInserts = partitionSizes();

            int id = 0;

            do {
                insertPeople(tx, TABLE_NAME, new Person(id, String.valueOf(id), id + 0.0));

                id++;
            } while (differences(beforeInserts, partitionSizes()) < 2);

            fakeUpdateCatalog();

            assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
            assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
        });

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
    }

    @Test
    void testOnePhaseCommitViaKeyValue() {
        int oldLatestCatalogVersion = latestCatalogVersion();

        TableImpl tableImpl = tableImpl();

        var continueUpdateMvPartitionStorageFuture = new CompletableFuture<Void>();

        CompletableFuture<Void> awaitStartUpdateAnyMvPartitionStorageFuture = awaitStartUpdateAnyMvPartitionStorage(
                tableImpl.internalTable().storage(),
                continueUpdateMvPartitionStorageFuture
        );

        CompletableFuture<Void> putAsync = tableImpl.keyValueView().putAsync(
                null,
                Tuple.create().set("ID", 0), Tuple.create().set("NAME", "0").set("SALARY", 0.0)
        );

        assertThat(awaitStartUpdateAnyMvPartitionStorageFuture, willCompleteSuccessfully());

        fakeUpdateCatalog();

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
        assertFalse(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));

        continueUpdateMvPartitionStorageFuture.complete(null);

        assertThat(putAsync, willCompleteSuccessfully());

        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(oldLatestCatalogVersion));
        assertTrue(isNodeFinishedRwTransactionsStartedBeforeFromNetwork(latestCatalogVersion()));
    }

    private static void runInTx(boolean commit, Consumer<Transaction> consumer) {
        Transaction tx = node().transactions().begin(new TransactionOptions().readOnly(false));

        try {
            consumer.accept(tx);
        } finally {
            assertThat(commit ? tx.commitAsync() : tx.rollbackAsync(), willCompleteSuccessfully());
        }
    }

    private static TableImpl tableImpl() {
        CompletableFuture<Table> tableFuture = node().tables().tableAsync(TABLE_NAME);

        assertThat(tableFuture, willBe(notNullValue()));

        return unwrapTableImpl(tableFuture.join());
    }

    private static long[] partitionSizes() {
        return IgniteTestUtils.bypassingThreadAssertions(() -> {
            InternalTable table = tableImpl().internalTable();

            return IntStream.range(0, table.partitions())
                    .mapToObj(table.storage()::getMvPartition)
                    .mapToLong(ItIndexNodeFinishedRwTransactionsCheckerTest::partitionSize)
                    .toArray();
        });
    }

    private static long partitionSize(MvPartitionStorage partitionStorage) {
        try (PartitionTimestampCursor cursor = partitionStorage.scan(HybridTimestamp.MAX_VALUE)) {
            return cursor.stream().count();
        }
    }

    private static long differences(long[] partitionSizes0, long[] partitionsSizes1) {
        assertEquals(partitionSizes0.length, partitionsSizes1.length);

        return IntStream.range(0, partitionSizes0.length)
                .filter(i -> partitionSizes0[i] != partitionsSizes1[i])
                .count();
    }

    private static void fakeUpdateCatalog() {
        int oldLatestCatalogVersion = latestCatalogVersion();

        String oldZoneName = zoneNameForUpdateCatalogVersionOnly;
        String newZoneName = zoneNameForUpdateCatalogVersionOnly + 0;

        sql(String.format("ALTER ZONE %s RENAME TO %s", oldZoneName, newZoneName));

        zoneNameForUpdateCatalogVersionOnly = newZoneName;

        assertThat(latestCatalogVersion(), greaterThan(oldLatestCatalogVersion));
    }

    private static int latestCatalogVersion() {
        return node().catalogManager().latestCatalogVersion();
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static boolean isNodeFinishedRwTransactionsStartedBeforeFromNetwork(int catalogVersion) {
        CompletableFuture<NetworkMessage> invokeFuture = node().clusterService().messagingService().invoke(
                node().node(),
                FACTORY.isNodeFinishedRwTransactionsStartedBeforeRequest().targetCatalogVersion(catalogVersion).build(),
                1_000
        );

        assertThat(invokeFuture, willCompleteSuccessfully());

        return ((IsNodeFinishedRwTransactionsStartedBeforeResponse) invokeFuture.join()).finished();
    }

    private static CompletableFuture<Void> awaitStartUpdateAnyMvPartitionStorage(
            MvTableStorage mvTableStorage,
            CompletableFuture<Void> continueUpdateFuture
    ) {
        var awaitStartUpdateAnyMvPartitionStorageFuture = new CompletableFuture<Void>();

        for (int partitionId = 0; partitionId < mvTableStorage.getTableDescriptor().getPartitions(); partitionId++) {
            MvPartitionStorage mvPartitionStorage = Wrappers.unwrapNullable(
                    mvTableStorage.getMvPartition(partitionId),
                    TestMvPartitionStorage.class
            );

            doAnswer(invocation -> {
                awaitStartUpdateAnyMvPartitionStorageFuture.complete(null);

                assertThat(continueUpdateFuture, willCompleteSuccessfully());

                return invocation.callRealMethod();
            }).when(mvPartitionStorage).runConsistently(any());
        }

        return awaitStartUpdateAnyMvPartitionStorageFuture;
    }
}
