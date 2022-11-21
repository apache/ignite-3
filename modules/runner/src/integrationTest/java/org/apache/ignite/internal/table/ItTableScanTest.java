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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests to check a scan internal command.
 */
public class ItTableScanTest extends AbstractBasicIntegrationTest {

    /** Table name. */
    private static final String TABLE_NAME = "test";

    /** Sorted index name. */
    public static final String SORTED_IDX = "sorted_idx";

    /** Ids to insert. */
    private static final List<Integer> ROW_IDS = List.of(1, 2, 5, 6, 7, 10, 53);

    @BeforeEach
    public void beforeTest() {
        TableImpl table = getOrCreateTable();

        loadData(table);
    }

    @AfterEach
    public void afterTest() {
        clearData(getOrCreateTable());
    }

    @Test
    public void testInsertWaitScanComplete() throws Exception {
        TableImpl table = getOrCreateTable();

        InternalTable internalTable = table.internalTable();

        UUID soredIndexId = getSortedIndexId();

        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, soredIndexId, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(2);

        IgniteTestUtils.waitForCondition(() -> scannedRows.size() == 2, 10_000);

        assertEquals(2, scannedRows.size());
        assertFalse(scanned.isDone());

        CompletableFuture<Void> insertFut = table.keyValueView()
                .putAsync(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));

        assertFalse(insertFut.isDone());

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.waitForCondition(() -> scannedRows.size() == ROW_IDS.size(), 10_000);

        assertEquals(ROW_IDS.size(), scannedRows.size());
        assertTrue(scanned.isDone());
        assertTrue(insertFut.isDone());
    }

    @Test
    public void testInsertDuringScan() throws Exception {
        TableImpl table = getOrCreateTable();

        InternalTable internalTable = table.internalTable();

        UUID soredIndexId = getSortedIndexId();

        ArrayList<ByteBuffer> scannedRows = new ArrayList<>();

        Publisher<BinaryRow> publisher = internalTable.scan(0, null, soredIndexId, null, null, 0, null);

        CompletableFuture<Void> scanned = new CompletableFuture<>();

        Subscription subscription = subscribeToPublisher(scannedRows, publisher, scanned);

        subscription.request(1);

        IgniteTestUtils.waitForCondition(() -> !scannedRows.isEmpty(), 10_000);

        assertEquals(1, scannedRows.size());
        assertFalse(scanned.isDone());

        table.keyValueView().put(null, Tuple.create().set("key", 3), Tuple.create().set("valInt", 3).set("valStr", "New_3"));

        subscription.request(1_000); // Request so much entries here to close the publisher.

        IgniteTestUtils.waitForCondition(() -> scannedRows.size() == ROW_IDS.size() + 1, 10_000);

        assertEquals(ROW_IDS.size(), scannedRows.size());
        assertTrue(scanned.isDone());
    }

    /**
     * Loads data.
     *
     * @param table Ignite table.
     */
    @NotNull
    private static void loadData(TableImpl table) {
        ROW_IDS.forEach(id -> insertRow(id));

        for (Integer rowId : ROW_IDS) {
            Tuple row = table.keyValueView().get(null, Tuple.create().set("key", rowId));

            assertNotNull(row);
            assertEquals("Str_" + rowId, row.value("valStr"));
            assertEquals(rowId, row.value("valInt"));
        }
    }

    /**
     * Clears data with primary keys for 0 to 100.
     *
     * @param table Ignite table.
     */
    private static void clearData(TableImpl table) {
        ArrayList<Tuple> keysToRemove = new ArrayList<>(100);

        IntStream.range(0, 100).forEach(rowId -> keysToRemove.add(Tuple.create().set("key", rowId)));

        table.keyValueView().removeAll(null, keysToRemove);
    }

    /**
     * Gets an index id.
     *
     * @return Index id.
     */
    private static UUID getSortedIndexId() {
        return ((IgniteImpl) CLUSTER_NODES.get(0)).clusterConfiguration().getConfiguration(TablesConfiguration.KEY).indexes()
                .get(SORTED_IDX.toUpperCase()).id().value();
    }

    /**
     * Subscribes to a cursor publisher.
     *
     * @param scannedRows List of rows, that were scanned.
     * @param publisher Publisher.
     * @param scanned A future that will be completed when the scan is finished.
     * @return Subscription, that can request rows from cluster.
     */
    private static Subscription subscribeToPublisher(
            List<ByteBuffer> scannedRows,
            Publisher<BinaryRow> publisher,
            CompletableFuture<Void> scanned
    ) {
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();

        publisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
            }

            @Override
            public void onNext(BinaryRow item) {
                scannedRows.add(item.valueSlice());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
                scanned.complete(null);
            }
        });

        return subscriptionRef.get();
    }

    /**
     * Creates or gets, if the table already exists, a table with the sorted index.
     *
     * @return Ignite table.
     */
    private static TableImpl getOrCreateTable() {
        sql("CREATE TABLE IF NOT EXISTS " + TABLE_NAME
                + " (key INTEGER PRIMARY KEY, valInt INTEGER, valStr VARCHAR) WITH REPLICAS=1, PARTITIONS=1;");

        sql("CREATE INDEX IF NOT EXISTS " + SORTED_IDX + " ON " + TABLE_NAME + " USING TREE (valInt)");

        return (TableImpl) CLUSTER_NODES.get(0).tables().table(TABLE_NAME);
    }

    /**
     * Adds a new row to the table.
     *
     * @param rowId Primary key of the new row.
     */
    private static void insertRow(int rowId) {
        sql(IgniteStringFormatter.format("INSERT INTO {} (key, valInt, valStr) VALUES ({}, {}, '{}');",
                TABLE_NAME, rowId, rowId, "Str_" + rowId));

    }
}
