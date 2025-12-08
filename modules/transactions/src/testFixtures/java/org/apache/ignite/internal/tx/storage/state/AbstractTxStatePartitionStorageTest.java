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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage.REBALANCE_IN_PROGRESS;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Abstract tx state partition storage test.
 */
public abstract class AbstractTxStatePartitionStorageTest extends BaseIgniteAbstractTest {
    protected static final int TABLE_ID = 1;

    protected static final int ZONE_ID = 2;

    protected static final byte[] GROUP_CONFIGURATION = {1, 2, 3};

    protected static final byte[] SNAPSHOT_INFO = {4, 5, 6};

    protected static final LeaseInfo LEASE_INFO = new LeaseInfo(1, UUID.randomUUID(), "node");

    protected TxStateStorage txStateStorage;

    /**
     * Creates {@link TxStatePartitionStorage} to test.
     */
    protected abstract TxStateStorage createZoneStorage();

    @BeforeEach
    protected void beforeTest() {
        txStateStorage = createZoneStorage();

        txStateStorage.start();
    }

    @AfterEach
    protected void afterTest() throws Exception {
        txStateStorage.close();
    }

    @Test
    public void testPutGetRemove() {
        testPutGetRemove0((storage, txIds) -> {
            int index = 0;

            for (UUID txId : txIds) {
                storage.remove(txId, index++, 1);
            }
        });
    }

    @Test
    public void testPutGetRemoveAll() {
        testPutGetRemove0((storage, txIds) -> storage.removeAll(txIds, 1, 1));
    }

    private void testPutGetRemove0(BiConsumer<TxStatePartitionStorage, Set<UUID>> removeOp) {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            UUID txId = UUID.randomUUID();

            txIds.add(txId);

            storage.putForRebalance(txId, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txId)));
        }

        for (int i = 0; i < 100; i++) {
            TxMeta txMeta = storage.get(txIds.get(i));
            TxMeta txMetaExpected = new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
            assertEquals(txMetaExpected, txMeta);
        }

        Set<UUID> toRemove = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                toRemove.add(txIds.get(i));
            }
        }

        long indexBeforeRemove = storage.lastAppliedIndex();
        long termBeforeRemove = storage.lastAppliedTerm();

        removeOp.accept(storage, toRemove);

        assertTrue(storage.lastAppliedIndex() > indexBeforeRemove);
        assertTrue(storage.lastAppliedTerm() > termBeforeRemove);

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

    private static List<EnlistedPartitionGroup> generateEnlistedPartitions(int c) {
        return IntStream.range(0, c)
                .mapToObj(partitionNumber -> new EnlistedPartitionGroup(new ZonePartitionId(ZONE_ID, partitionNumber), Set.of(TABLE_ID)))
                .collect(toList());
    }

    private static HybridTimestamp generateTimestamp(UUID uuid) {
        long time = Math.abs(uuid.getMostSignificantBits());

        if (time <= 0) {
            time = 1;
        }

        return hybridTimestamp(time);
    }

    @Test
    public void testRemoveAll() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            UUID txId = UUID.randomUUID();

            txIds.add(txId);

            storage.putForRebalance(txId, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txId)));
        }

        assertThrows(NullPointerException.class, () -> storage.removeAll(null, 0, 0),
                "Collection of the transaction IDs intended for removal cannot be null.");

        storage.removeAll(emptyList(), 1, 1);

        UUID uuid0 = txIds.get(0);
        UUID uuid1 = txIds.get(1);

        storage.removeAll(List.of(uuid0, uuid0, uuid1, uuid1, UUID.randomUUID()), 2, 1);

        assertNull(storage.get(uuid0));
        assertNull(storage.get(uuid1));

        for (int i = 2; i < 100; i++) {
            TxMeta txMetaExpected = new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
            assertEquals(txMetaExpected, storage.get(txIds.get(i)));
        }

        long newIndex = 3;
        long newTerm = 2;

        assertTrue(storage.lastAppliedIndex() < newIndex);
        assertTrue(storage.lastAppliedTerm() < newTerm);

        storage.removeAll(txIds, newIndex, newTerm);

        assertEquals(newIndex, storage.lastAppliedIndex());
        assertEquals(newTerm, storage.lastAppliedTerm());

        for (int i = 0; i < 100; i++) {
            assertNull(storage.get(txIds.get(i)));
        }
    }

    @Test
    public void testCas() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

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

        long newIndex = 4;
        long newTerm = 3;

        assertTrue(storage.lastAppliedIndex() < newIndex);
        assertTrue(storage.lastAppliedTerm() < newTerm);

        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, newIndex, newTerm));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, newIndex, newTerm));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta2, newIndex, newTerm));

        assertEquals(newIndex, storage.lastAppliedIndex());
        assertEquals(newTerm, storage.lastAppliedTerm());

        TxMeta txMetaNullTimestamp2 = new TxMeta(txMeta2.txState(), txMeta2.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp2, 3, 2));

        assertEquals(storage.get(txId), txMeta2);
    }

    @Test
    public void testScan() {
        TxStatePartitionStorage storage0 = txStateStorage.getOrCreatePartitionStorage(0);
        TxStatePartitionStorage storage1 = txStateStorage.getOrCreatePartitionStorage(1);
        TxStatePartitionStorage storage2 = txStateStorage.getOrCreatePartitionStorage(2);

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
        TxStatePartitionStorage storage0 = txStateStorage.getOrCreatePartitionStorage(0);
        TxStatePartitionStorage storage1 = txStateStorage.getOrCreatePartitionStorage(1);

        UUID txId0 = UUID.randomUUID();
        storage0.putForRebalance(txId0, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(1), generateTimestamp(txId0)));

        UUID txId1 = UUID.randomUUID();
        storage1.putForRebalance(txId1, new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(1), generateTimestamp(txId1)));

        storage0.destroy();

        assertNotNull(storage1.get(txId1));
    }

    @Test
    public void scansInOrderDefinedByTxIds() {
        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(0);

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
        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(0);

        UUID existingBeforeScan = new UUID(2, 0);
        partitionStorage.putForRebalance(existingBeforeScan, randomTxMeta(1, existingBeforeScan));

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = partitionStorage.scan()) {
            UUID prependedDuringScan = new UUID(1, 0);
            partitionStorage.putForRebalance(prependedDuringScan, randomTxMeta(1, prependedDuringScan));
            UUID appendedDuringScan = new UUID(3, 0);
            partitionStorage.putForRebalance(appendedDuringScan, randomTxMeta(1, appendedDuringScan));

            List<UUID> txIdsReturnedByScan = cursor.stream()
                    .map(IgniteBiTuple::getKey)
                    .collect(toList());

            assertThat(txIdsReturnedByScan, is(List.of(existingBeforeScan)));
        }
    }

    @Test
    void lastAppliedIndexGetterIsConsistentWithSetter() {
        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(0);

        partitionStorage.lastApplied(10, 2);

        assertThat(partitionStorage.lastAppliedIndex(), is(10L));
    }

    @Test
    void compareAndSetMakesLastAppliedChangeVisible() {
        TxStatePartitionStorage partitionStorage = txStateStorage.getOrCreatePartitionStorage(0);

        UUID txId = UUID.randomUUID();
        partitionStorage.compareAndSet(txId, null, randomTxMeta(1, txId), 10, 2);

        assertThat(partitionStorage.lastAppliedIndex(), is(10L));
        assertThat(partitionStorage.lastAppliedTerm(), is(2L));
    }

    @Test
    public void testSuccessRebalance() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        MvPartitionMeta partitionMeta = saneMvPartitionMeta(30, 50);

        // We can't finish rebalance that we haven't started.
        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.finishRebalance(partitionMeta));

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
        fillStorageDuringRebalance(storage, rowsOnRebalance);

        checkMeta(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, null, null, null);

        // Let's complete rebalancing.
        assertThat(storage.finishRebalance(partitionMeta), willCompleteSuccessfully());

        // Let's check the storage.
        checkMeta(storage, 30, 50, partitionMeta.groupConfig(), partitionMeta.leaseInfo(), partitionMeta.snapshotInfo());

        checkStorageContainsRows(storage, rowsOnRebalance);
    }

    @Test
    public void testFailRebalance() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

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
        fillStorageDuringRebalance(storage, rowsOnRebalance);

        checkMeta(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, null, null, null);

        // Let's abort rebalancing.
        assertThat(storage.abortRebalance(), willCompleteSuccessfully());

        // Let's check the storage.
        checkMeta(storage, 0, 0, null, null, null);

        checkStorageIsEmpty(storage);
    }

    @Test
    public void testStartRebalanceForClosedOrDestroyedPartition() {
        TxStatePartitionStorage storage0 = txStateStorage.getOrCreatePartitionStorage(0);
        TxStatePartitionStorage storage1 = txStateStorage.getOrCreatePartitionStorage(1);

        storage0.close();
        storage1.destroy();

        assertThrowsStorageClosedException(TX_STATE_STORAGE_STOPPED_ERR, storage0::startRebalance);
        assertThrowsStorageDestroyedException(TX_STATE_STORAGE_ERR, storage1::startRebalance);
    }

    @Test
    void testClear() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        // Cleaning up on empty storage should not generate errors.
        assertThat(storage.clear(), willCompleteSuccessfully());

        checkMeta(storage, 0, 0, null, null, null);
        checkStorageIsEmpty(storage);

        List<IgniteBiTuple<UUID, TxMeta>> rows = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        fillStorage(storage, rows);

        // Cleanup the non-empty storage.
        assertThat(storage.clear(), willCompleteSuccessfully());

        checkMeta(storage, 0, 0, null, null, null);
        checkStorageIsEmpty(storage);
    }

    @Test
    void testCleanOnClosedDestroyedAndRebalancedStorages() {
        TxStatePartitionStorage storage0 = txStateStorage.getOrCreatePartitionStorage(0);
        TxStatePartitionStorage storage1 = txStateStorage.getOrCreatePartitionStorage(1);
        TxStatePartitionStorage storage2 = txStateStorage.getOrCreatePartitionStorage(2);

        storage0.close();
        storage1.destroy();
        assertThat(storage2.startRebalance(), willCompleteSuccessfully());

        try {
            assertThrowsStorageClosedException(TX_STATE_STORAGE_STOPPED_ERR, storage0::clear);
            assertThrowsStorageException(TX_STATE_STORAGE_ERR, storage1::clear);
            assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, storage2::clear);
        } finally {
            assertThat(storage2.abortRebalance(), willCompleteSuccessfully());
        }
    }

    @Test
    void testSetAndReadMeta() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        storage.lastApplied(1, 15);

        assertThat(storage.lastAppliedIndex(), is(1L));
        assertThat(storage.lastAppliedTerm(), is(15L));

        storage.committedGroupConfiguration(GROUP_CONFIGURATION, 2, 30);

        assertThat(storage.committedGroupConfiguration(), is(GROUP_CONFIGURATION));
        assertThat(storage.lastAppliedIndex(), is(2L));
        assertThat(storage.lastAppliedTerm(), is(30L));

        storage.leaseInfo(LEASE_INFO, 3, 50);

        assertThat(storage.leaseInfo(), is(LEASE_INFO));
        assertThat(storage.lastAppliedIndex(), is(3L));
        assertThat(storage.lastAppliedTerm(), is(50L));

        storage.snapshotInfo(SNAPSHOT_INFO);

        assertThat(storage.snapshotInfo(), is(SNAPSHOT_INFO));
    }

    @Test
    void testCloseStartedRebalance() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        assertThat(storage.startRebalance(), willCompleteSuccessfully());

        assertThrows(IgniteInternalException.class, storage::close);

        assertThat(storage.finishRebalance(saneMvPartitionMeta(0, 0)), willCompleteSuccessfully());
    }

    @Test
    void testDestroyStartedRebalance() {
        TxStatePartitionStorage storage = txStateStorage.getOrCreatePartitionStorage(0);

        assertThat(storage.startRebalance(), willCompleteSuccessfully());

        assertDoesNotThrow(storage::destroy);
    }

    private static void checkStorageIsEmpty(TxStatePartitionStorage storage) {
        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(scan.stream().collect(toList()), is(empty()));
        }
    }

    protected static void checkStorageContainsRows(TxStatePartitionStorage storage, List<IgniteBiTuple<UUID, TxMeta>> expRows) {
        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(
                    scan.stream().collect(toList()),
                    containsInAnyOrder(expRows.toArray(new IgniteBiTuple[0]))
            );
        }
    }

    private static void startRebalanceWithChecks(TxStatePartitionStorage storage, List<IgniteBiTuple<UUID, TxMeta>> rows) {
        fillStorage(storage, rows);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanCursorBeforeStartRebalance = storage.scan();

        assertThat(storage.startRebalance(), willCompleteSuccessfully());

        checkTxStateStorageMethodsWhenRebalanceInProgress(storage);

        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, scanCursorBeforeStartRebalance::hasNext);
        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, scanCursorBeforeStartRebalance::next);

        // We cannot start a new rebalance until the current one has ended.
        assertThrows(TxStateStorageException.class, storage::startRebalance);
    }

    private static void checkTxStateStorageMethodsWhenRebalanceInProgress(TxStatePartitionStorage storage) {
        checkMeta(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, null, null, null);

        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.lastApplied(100, 500));
        assertThrowsStorageRebalanceException(
                TX_STATE_STORAGE_REBALANCE_ERR,
                () -> storage.committedGroupConfiguration(BYTE_EMPTY_ARRAY, 1, 2)
        );
        assertThrowsStorageRebalanceException(
                TX_STATE_STORAGE_REBALANCE_ERR,
                () -> storage.leaseInfo(LEASE_INFO, 1, 2)
        );
        assertThrowsStorageRebalanceException(
                TX_STATE_STORAGE_REBALANCE_ERR,
                () -> storage.snapshotInfo(SNAPSHOT_INFO)
        );
        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.get(UUID.randomUUID()));
        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, () -> storage.remove(UUID.randomUUID(), 100, 500));
        assertThrowsStorageRebalanceException(TX_STATE_STORAGE_REBALANCE_ERR, storage::scan);
    }

    private static IgniteBiTuple<UUID, TxMeta> putRandomTxMetaWithCommandIndex(
            TxStatePartitionStorage storage,
            int enlistedPartsCount,
            long commandIndex
    ) {
        UUID txId = UUID.randomUUID();
        TxMeta txMeta = randomTxMeta(enlistedPartsCount, txId);
        storage.compareAndSet(txId, null, txMeta, commandIndex, 1);

        return new IgniteBiTuple<>(txId, txMeta);
    }

    private static TxMeta randomTxMeta(int enlistedPartsCount, UUID txId) {
        return new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(enlistedPartsCount), generateTimestamp(txId));
    }

    /**
     * Creates random tx meta.
     *
     * @param enlistedPartsCount Count of enlisted partitions.
     * @param txId Tx ID.
     */
    protected static IgniteBiTuple<UUID, TxMeta> randomTxMetaTuple(int enlistedPartsCount, UUID txId) {
        return new IgniteBiTuple<>(
                txId,
                new TxMeta(TxState.COMMITTED, generateEnlistedPartitions(enlistedPartsCount), generateTimestamp(txId))
        );
    }

    private static void assertThrowsStorageException(int expFullErrorCode, Executable executable) {
        TxStateStorageException exception = assertThrows(TxStateStorageException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    private static void assertThrowsStorageClosedException(int expFullErrorCode, Executable executable) {
        TxStateStorageClosedException exception = assertThrows(TxStateStorageClosedException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    private static void assertThrowsStorageDestroyedException(int expFullErrorCode, Executable executable) {
        TxStateStorageDestroyedException exception = assertThrows(TxStateStorageDestroyedException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    private static void assertThrowsStorageRebalanceException(int expFullErrorCode, Executable executable) {
        TxStateStorageRebalanceException exception = assertThrows(TxStateStorageRebalanceException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    /**
     * Fills storage.
     *
     * @param storage Storage.
     * @param rows Rows.
     */
    protected static void fillStorage(TxStatePartitionStorage storage, List<IgniteBiTuple<UUID, TxMeta>> rows) {
        assertThat(rows, hasSize(greaterThanOrEqualTo(2)));

        for (int i = 0; i < rows.size(); i++) {
            IgniteBiTuple<UUID, TxMeta> row = rows.get(i);

            if ((i % 2) == 0) {
                assertTrue(storage.compareAndSet(row.get1(), null, row.get2(), i * 10L, i * 10L));
            } else {
                storage.putForRebalance(row.get1(), row.get2());
            }
        }

        long lastAppliedIndex = storage.lastAppliedIndex();
        long lastAppliedTerm = storage.lastAppliedTerm();

        storage.committedGroupConfiguration(GROUP_CONFIGURATION, lastAppliedIndex + 1, lastAppliedTerm);
        storage.leaseInfo(LEASE_INFO, lastAppliedIndex + 2, lastAppliedTerm);
        storage.snapshotInfo(SNAPSHOT_INFO);
    }

    protected static void fillStorageDuringRebalance(TxStatePartitionStorage storage, List<IgniteBiTuple<UUID, TxMeta>> rows) {
        rows.forEach(row -> storage.putForRebalance(row.get1(), row.get2()));
    }

    /**
     * Checks last applied index and term.
     *
     * @param storage Storage.
     * @param expLastAppliedIndex Expected last applied index.
     * @param expLastAppliedTerm Expected last applied term.
     */
    protected static void checkMeta(
            TxStatePartitionStorage storage,
            long expLastAppliedIndex,
            long expLastAppliedTerm,
            byte @Nullable [] expConfiguration,
            @Nullable LeaseInfo expLeaseInfo,
            byte @Nullable [] expSnapshotInfo
    ) {
        assertThat(storage.lastAppliedIndex(), is(expLastAppliedIndex));
        assertThat(storage.lastAppliedTerm(), is(expLastAppliedTerm));
        assertThat(storage.committedGroupConfiguration(), is(expConfiguration));
        assertThat(storage.leaseInfo(), is(expLeaseInfo));
        assertThat(storage.snapshotInfo(), is(expSnapshotInfo));
    }

    private static MvPartitionMeta saneMvPartitionMeta(long lastAppliedIndex, long lastAppliedTerm) {
        return new MvPartitionMeta(
                lastAppliedIndex,
                lastAppliedTerm,
                GROUP_CONFIGURATION,
                LEASE_INFO,
                SNAPSHOT_INFO
        );
    }
}
