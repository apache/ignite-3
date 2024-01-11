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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.message.RwTransactionsFinishedResponse;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** For testing {@link TxManagerImpl#isRwTransactionsFinished(int)}. */
public class ItRwTransactionsFinishedTest extends ClusterPerClassIntegrationTest {
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private static final String TABLE_NAME = "TEST_TABLE";

    private static String zoneNameForUpdateCatalogVersionOnly = "FAKE_TEST_ZONE";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void setUp() {
        if (node() != null) {
            createTableOnly(TABLE_NAME, DEFAULT_ZONE_NAME);
            createZoneOnly(zoneNameForUpdateCatalogVersionOnly, 1, 1);
        }
    }

    @AfterEach
    void tearDown() {
        if (node() != null) {
            sql("DROP TABLE IF EXISTS " + TABLE_NAME);
            sql("DROP ZONE IF EXISTS " + zoneNameForUpdateCatalogVersionOnly);
        }
    }

    private static Stream<Arguments> txTypeAndWayCompleteTx() {
        return Stream.of(
                Arguments.of(true, true),  // Read-only, commit.
                Arguments.of(true, false), // Read-only, rollback.
                Arguments.of(false, true), // Read-write, commit.
                Arguments.of(false, false) // Read-write, rollback.
        );
    }

    @Test
    void testNoTransactions() {
        TxManagerImpl txManager = txManagerImpl();
        int latestCatalogVersion = latestCatalogVersion();

        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion - 1));
        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion));

        assertFalse(txManager.isRwTransactionsFinished(latestCatalogVersion + 1));

        fakeUpdateCatalog();
        int newLatestCatalogVersion = latestCatalogVersion();

        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion));
        assertTrue(txManager.isRwTransactionsFinished(newLatestCatalogVersion));
    }

    @ParameterizedTest(name = "readOnly = {0}, commit = {1}")
    @MethodSource("txTypeAndWayCompleteTx")
    void testEmptyTransaction(boolean readOnly, boolean commit) {
        TxManagerImpl txManager = txManagerImpl();
        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(readOnly, commit, tx -> {
            assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
            assertFalse(txManager.isRwTransactionsFinished(oldLatestCatalogVersion + 1));

            fakeUpdateCatalog();

            assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
            assertEquals(readOnly, txManager.isRwTransactionsFinished(latestCatalogVersion()));
        });

        assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion()));
    }

    @ParameterizedTest(name = "readOnly = {0}, commit = {1}")
    @MethodSource("txTypeAndWayCompleteTx")
    void testNotEmptyTransaction(boolean readOnly, boolean commit) {
        insertPeople(TABLE_NAME, new Person(0, "0", 0.0));

        TxManagerImpl txManager = txManagerImpl();
        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(readOnly, commit, tx -> {
            if (readOnly) {
                sql(tx, "SELECT * FROM " + TABLE_NAME);
            } else {
                insertPeople(tx, TABLE_NAME, new Person(1, "1", 1.0));
            }

            assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
            assertFalse(txManager.isRwTransactionsFinished(oldLatestCatalogVersion + 1));

            fakeUpdateCatalog();

            assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
            assertEquals(readOnly, txManager.isRwTransactionsFinished(latestCatalogVersion()));
        });

        assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion()));
    }

    @ParameterizedTest(name = "commit = {0}")
    @ValueSource(booleans = {true, false})
    void testReadWriteTransactionInsertIntoMultiplePartitions(boolean commit) {
        assertThat(tableImpl().internalTable().partitions(), greaterThan(1));

        TxManagerImpl txManager = txManagerImpl();
        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(false, commit, rwTx -> {
            long[] beforeInserts = partitionSizes();

            int id = 0;

            do {
                insertPeople(rwTx, TABLE_NAME, new Person(id, String.valueOf(id), id + 0.0));

                id++;
            } while (differences(beforeInserts, partitionSizes()) < 2);

            fakeUpdateCatalog();

            assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
            assertFalse(txManager.isRwTransactionsFinished(latestCatalogVersion()));
        });

        assertTrue(txManager.isRwTransactionsFinished(oldLatestCatalogVersion));
        assertTrue(txManager.isRwTransactionsFinished(latestCatalogVersion()));
    }

    @Test
    void testRwTransactionsFinishedMessages() {
        int oldLatestCatalogVersion = latestCatalogVersion();

        runInTx(false, true, tx -> {
            fakeUpdateCatalog();

            assertTrue(isRwTransactionsFinishedFromNetwork(oldLatestCatalogVersion));
            assertFalse(isRwTransactionsFinishedFromNetwork(latestCatalogVersion()));
        });

        assertTrue(isRwTransactionsFinishedFromNetwork(oldLatestCatalogVersion));
        assertTrue(isRwTransactionsFinishedFromNetwork(latestCatalogVersion()));
    }

    private static void runInTx(boolean readOnly, boolean commit, Consumer<Transaction> consumer) {
        Transaction tx = node().transactions().begin(new TransactionOptions().readOnly(readOnly));

        try {
            consumer.accept(tx);
        } finally {
            assertThat(commit ? tx.commitAsync() : tx.rollbackAsync(), willCompleteSuccessfully());
        }
    }

    private static TableImpl tableImpl() {
        CompletableFuture<Table> tableFuture = node().tables().tableAsync(TABLE_NAME);

        assertThat(tableFuture, willBe(notNullValue()));

        return (TableImpl) tableFuture.join();
    }

    private static long[] partitionSizes() {
        InternalTable table = tableImpl().internalTable();

        return IntStream.range(0, table.partitions())
                .mapToLong(partitionId -> table.storage().getMvPartition(partitionId).rowsCount())
                .toArray();
    }

    private static int differences(long[] partitionSizes0, long[] partitionsSizes1) {
        assertEquals(partitionSizes0.length, partitionsSizes1.length);

        return (int) IntStream.range(0, partitionSizes0.length)
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

    private static TxManagerImpl txManagerImpl() {
        return ((TxManagerImpl) node().txManager());
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static boolean isRwTransactionsFinishedFromNetwork(int catalogVersion) {
        CompletableFuture<NetworkMessage> invokeFuture = node().clusterService().messagingService().invoke(
                node().node(),
                FACTORY.rwTransactionsFinishedRequest().targetSchemaVersion(catalogVersion).build(),
                1_000
        );

        assertThat(invokeFuture, willCompleteSuccessfully());

        return ((RwTransactionsFinishedResponse) invokeFuture.join()).finished();
    }
}
