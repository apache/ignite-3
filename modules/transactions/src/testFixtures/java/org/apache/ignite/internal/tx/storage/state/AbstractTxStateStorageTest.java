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

package org.apache.ignite.internal.tx.storage.state;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.storage.state.TxStateStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Abstract tx storage test.
 */
public abstract class AbstractTxStateStorageTest extends BaseIgniteAbstractTest {
    protected static final int TABLE_ID = 1;

    protected TxStateTableStorage tableStorage;

    /**
     * Creates {@link TxStateStorage} to test.
     */
    protected abstract TxStateTableStorage createTableStorage();

    @BeforeEach
    protected void beforeTest() {
        tableStorage = createTableStorage();

        tableStorage.start();
    }

    @AfterEach
    protected void afterTest() throws Exception {
        tableStorage.close();
    }

    @Test
    public void testPutGetRemove() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            UUID txId = UUID.randomUUID();

            txIds.add(txId);

            storage.put(txId, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txId)));
        }

        for (int i = 0; i < 100; i++) {
            TxMeta txMeta = storage.get(txIds.get(i));
            TxMeta txMetaExpected = new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
            assertEquals(txMetaExpected, txMeta);
        }

        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                storage.remove(txIds.get(i), i, 1);
            }
        }

        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                TxMeta txMeta = storage.get(txIds.get(i));
                assertNull(txMeta);
            } else {
                TxMeta txMeta = storage.get(txIds.get(i));
                TxMeta txMetaExpected = new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
                assertEquals(txMetaExpected, txMeta);
            }
        }
    }

    private List<TablePartitionId> generateEnlistedPartitions(int c) {
        return IntStream.range(0, c)
                .mapToObj(partitionNumber -> new TablePartitionId(TABLE_ID, partitionNumber))
                .collect(toList());
    }

    private HybridTimestamp generateTimestamp(UUID uuid) {
        long time = Math.abs(uuid.getMostSignificantBits());

        if (time <= 0) {
            time = 1;
        }

        return hybridTimestamp(time);
    }

    @Test
    public void testCas() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        UUID txId = UUID.randomUUID();

        TxMeta txMeta1 = new TxMeta(TxState.COMMITTED, new ArrayList<>(), generateTimestamp(txId));
        TxMeta txMeta2 = new TxMeta(TxState.COMMITTED, new ArrayList<>(),  generateTimestamp(UUID.randomUUID()));

        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1, 1));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1, 1));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta1, 1, 1));

        TxMeta txMetaWrongTimestamp0 =
                new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), generateTimestamp(UUID.randomUUID()));
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaWrongTimestamp0, 1, 1));

        TxMeta txMetaNullTimestamp0 = new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp0, 3, 2));

        assertEquals(storage.get(txId), txMeta1);

        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3, 2));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3, 2));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta2, 3, 2));

        TxMeta txMetaNullTimestamp2 = new TxMeta(txMeta2.txState(), txMeta2.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp2, 3, 2));

        assertEquals(storage.get(txId), txMeta2);
    }

    @Test
    public void testScan() {
        TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
        TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);
        TxStateStorage storage2 = tableStorage.getOrCreateTxStateStorage(2);

        Map<UUID, TxMeta> txs = new HashMap<>();

        putRandomTxMetaWithCommandIndex(storage0, 1, 0);
        putRandomTxMetaWithCommandIndex(storage2, 1, 0);

        for (int i = 0; i < 100; i++) {
            IgniteBiTuple<UUID, TxMeta> txData = putRandomTxMetaWithCommandIndex(storage1, i, i);
            txs.put(txData.get1(), txData.get2());
        }

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scanCursor = storage1.scan()) {
            assertTrue(scanCursor.hasNext());

            while (scanCursor.hasNext()) {
                IgniteBiTuple<UUID, TxMeta> txData = scanCursor.next();
                TxMeta txMeta = txs.remove(txData.getKey());

                assertNotNull(txMeta);
                assertNotNull(txData);
                assertNotNull(txData.getValue());
                assertEquals(txMeta, txData.getValue());
            }

            assertTrue(txs.isEmpty());
        }
    }

    @Test
    public void testDestroy() {
        TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
        TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);

        UUID txId0 = UUID.randomUUID();
        storage0.put(txId0, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(1), generateTimestamp(txId0)));

        UUID txId1 = UUID.randomUUID();
        storage1.put(txId1, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(1), generateTimestamp(txId1)));

        storage0.destroy();

        assertNotNull(storage1.get(txId1));
    }

    @Test
    public void scansInOrderDefinedByTxIds() {
        TxStateStorage partitionStorage = tableStorage.getOrCreateTxStateStorage(0);

        for (int i = 0; i < 100; i++) {
            putRandomTxMetaWithCommandIndex(partitionStorage, i, i);
        }

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scanCursor = partitionStorage.scan()) {
            List<UUID> txIds = new ArrayList<>();

            while (scanCursor.hasNext()) {
                IgniteBiTuple<UUID, TxMeta> txData = scanCursor.next();

                txIds.add(txData.getKey());
            }

            assertThat(txIds, equalTo(txIds.stream().sorted(new UnsignedUuidComparator()).collect(toList())));
        }
    }

    @Test
    public void scanOnlySeesDataExistingAtTheMomentOfCreation() {
        TxStateStorage partitionStorage = tableStorage.getOrCreateTxStateStorage(0);

        UUID existingBeforeScan = new UUID(2, 0);
        partitionStorage.put(existingBeforeScan, randomTxMeta(1, existingBeforeScan));

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = partitionStorage.scan()) {
            UUID prependedDuringScan = new UUID(1, 0);
            partitionStorage.put(prependedDuringScan, randomTxMeta(1, prependedDuringScan));
            UUID appendedDuringScan = new UUID(3, 0);
            partitionStorage.put(appendedDuringScan, randomTxMeta(1, appendedDuringScan));

            List<UUID> txIdsReturnedByScan = cursor.stream()
                    .map(IgniteBiTuple::getKey)
                    .collect(toList());

            assertThat(txIdsReturnedByScan, is(List.of(existingBeforeScan)));
        }
    }

    @Test
    void lastAppliedIndexGetterIsConsistentWithSetter() {
        TxStateStorage partitionStorage = tableStorage.getOrCreateTxStateStorage(0);

        partitionStorage.lastApplied(10, 2);

        assertThat(partitionStorage.lastAppliedIndex(), is(10L));
    }

    @Test
    void lastAppliedTermGetterIsConsistentWithSetter() {
        TxStateStorage partitionStorage = tableStorage.getOrCreateTxStateStorage(0);

        partitionStorage.lastApplied(10, 2);

        assertThat(partitionStorage.lastAppliedTerm(), is(2L));
    }

    @Test
    void compareAndSetMakesLastAppliedChangeVisible() {
        TxStateStorage partitionStorage = tableStorage.getOrCreateTxStateStorage(0);

        UUID txId = UUID.randomUUID();
        partitionStorage.compareAndSet(txId, null, randomTxMeta(1, txId), 10, 2);

        assertThat(partitionStorage.lastAppliedIndex(), is(10L));
        assertThat(partitionStorage.lastAppliedTerm(), is(2L));
    }

    @Test
    public void testSuccessRebalance() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        // We can't finish rebalance that we haven't started.
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.finishRebalance(100, 500));

        List<IgniteBiTuple<UUID, TxMeta>> rowsBeforeStartRebalance = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        startRebalanceWithChecks(storage, rowsBeforeStartRebalance);

        List<IgniteBiTuple<UUID, TxMeta>> rowsOnRebalance = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        // Let's fill it with new data.
        fillStorage(storage, rowsOnRebalance);

        checkLastApplied(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        // Let's complete rebalancing.
        assertThat(storage.finishRebalance(30, 50), willCompleteSuccessfully());

        // Let's check the storage.
        checkLastApplied(storage, 30, 30, 50);

        checkStorageContainsRows(storage, rowsOnRebalance);
    }

    @Test
    public void testFailRebalance() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        // Nothing will happen because rebalance has not started.
        assertThat(storage.abortRebalance(), willCompleteSuccessfully());

        List<IgniteBiTuple<UUID, TxMeta>> rowsBeforeStartRebalance = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        startRebalanceWithChecks(storage, rowsBeforeStartRebalance);

        List<IgniteBiTuple<UUID, TxMeta>> rowsOnRebalance = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        // Let's fill it with new data.
        fillStorage(storage, rowsOnRebalance);

        checkLastApplied(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        // Let's abort rebalancing.
        assertThat(storage.abortRebalance(), willCompleteSuccessfully());

        // Let's check the storage.
        checkLastApplied(storage, 0, 0, 0);

        checkStorageIsEmpty(storage);
    }

    @Test
    public void testStartRebalanceForClosedOrDestroyedPartition() {
        TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
        TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);

        storage0.close();
        storage1.destroy();

        assertThrowsIgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, storage0::startRebalance);
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, storage1::startRebalance);
    }

    @Test
    void testClear() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        // Cleaning up on empty storage should not generate errors.
        assertThat(storage.clear(), willCompleteSuccessfully());

        checkLastApplied(storage, 0, 0, 0);
        checkStorageIsEmpty(storage);

        List<IgniteBiTuple<UUID, TxMeta>> rows = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        fillStorage(storage, rows);

        // Cleanup the non-empty storage.
        assertThat(storage.clear(), willCompleteSuccessfully());

        checkLastApplied(storage, 0, 0, 0);
        checkStorageIsEmpty(storage);
    }

    @Test
    void testCleanOnClosedDestroyedAndRebalancedStorages() {
        TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
        TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);
        TxStateStorage storage2 = tableStorage.getOrCreateTxStateStorage(2);

        storage0.close();
        storage1.destroy();
        assertThat(storage2.startRebalance(), willCompleteSuccessfully());

        try {
            assertThrowsIgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, storage0::clear);
            assertThrowsIgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, storage1::clear);
            assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, storage2::clear);
        } finally {
            assertThat(storage2.abortRebalance(), willCompleteSuccessfully());
        }
    }

    private static void checkStorageIsEmpty(TxStateStorage storage) {
        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(scan.stream().collect(toList()), is(empty()));
        }
    }

    protected static void checkStorageContainsRows(TxStateStorage storage, List<IgniteBiTuple<UUID, TxMeta>> expRows) {
        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(
                    scan.stream().collect(toList()),
                    containsInAnyOrder(expRows.toArray(new IgniteBiTuple[0]))
            );
        }
    }

    private void startRebalanceWithChecks(TxStateStorage storage, List<IgniteBiTuple<UUID, TxMeta>> rows) {
        fillStorage(storage, rows);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanCursorBeforeStartRebalance = storage.scan();

        assertThat(storage.startRebalance(), willCompleteSuccessfully());

        checkTxStateStorageMethodsWhenRebalanceInProgress(storage);

        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, scanCursorBeforeStartRebalance::hasNext);
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, scanCursorBeforeStartRebalance::next);

        // We cannot start a new rebalance until the current one has ended.
        assertThrows(IgniteInternalException.class, storage::startRebalance);
    }

    private static void checkTxStateStorageMethodsWhenRebalanceInProgress(TxStateStorage storage) {
        checkLastApplied(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.lastApplied(100, 500));
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.get(UUID.randomUUID()));
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.remove(UUID.randomUUID(), 1, 1));
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, storage::scan);
    }

    private IgniteBiTuple<UUID, TxMeta> putRandomTxMetaWithCommandIndex(TxStateStorage storage, int enlistedPartsCount, long commandIndex) {
        UUID txId = UUID.randomUUID();
        TxMeta txMeta = randomTxMeta(enlistedPartsCount, txId);
        storage.compareAndSet(txId, null, txMeta, commandIndex, 1);

        return new IgniteBiTuple<>(txId, txMeta);
    }

    private TxMeta randomTxMeta(int enlistedPartsCount, UUID txId) {
        return new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(enlistedPartsCount), generateTimestamp(txId));
    }

    /**
     * Creates random tx meta.
     *
     * @param enlistedPartsCount Count of enlisted partitions.
     * @param txId Tx ID.
     */
    protected IgniteBiTuple<UUID, TxMeta> randomTxMetaTuple(int enlistedPartsCount, UUID txId) {
        return new IgniteBiTuple<>(
                txId,
                new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(enlistedPartsCount), generateTimestamp(txId))
        );
    }

    private static void assertThrowsIgniteInternalException(int expFullErrorCode, Executable executable) {
        IgniteInternalException exception = assertThrows(IgniteInternalException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    /**
     * Fills storage.
     *
     * @param storage Storage.
     * @param rows Rows.
     */
    protected static void fillStorage(TxStateStorage storage, List<IgniteBiTuple<UUID, TxMeta>> rows) {
        assertThat(rows, hasSize(greaterThanOrEqualTo(2)));

        for (int i = 0; i < rows.size(); i++) {
            IgniteBiTuple<UUID, TxMeta> row = rows.get(i);

            if ((i % 2) == 0) {
                assertTrue(storage.compareAndSet(row.get1(), null, row.get2(), i * 10L, i * 10L));
            } else {
                storage.put(row.get1(), row.get2());
            }
        }
    }

    /**
     * Checks last applied index and term.
     *
     * @param storage Storage.
     * @param expLastAppliedIndex Expected last applied index.
     * @param expLastAppliedTerm Expected last applied term.
     * @param expPersistentIndex Expected persistent last applied index.
     */
    protected static void checkLastApplied(
            TxStateStorage storage,
            long expLastAppliedIndex,
            long expPersistentIndex,
            long expLastAppliedTerm
    ) {
        assertEquals(expLastAppliedIndex, storage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, storage.lastAppliedTerm());
    }
}
