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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.tx.storage.state.TxStateStorage.FULL_REBALANCE_IN_PROGRESS;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_FULL_REBALANCE_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Abstract tx storage test.
 */
public abstract class AbstractTxStateStorageTest {
    private TxStateTableStorage tableStorage;

    /**
     * Creates {@link TxStateStorage} to test.
     */
    protected abstract TxStateTableStorage createTableStorage();

    @BeforeEach
    void beforeTest() {
        tableStorage = createTableStorage();

        tableStorage.start();
    }

    @AfterEach
    void afterTest() {
        tableStorage.close();
    }

    @Test
    public void testPutGetRemove() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        List<UUID> txIds = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            UUID txId = UUID.randomUUID();

            txIds.add(txId);

            storage.put(txId, new TxMeta(TxState.COMMITED, generateEnlistedPartitions(i), generateTimestamp(txId)));
        }

        for (int i = 0; i < 100; i++) {
            TxMeta txMeta = storage.get(txIds.get(i));
            TxMeta txMetaExpected = new TxMeta(TxState.COMMITED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
            assertTxMetaEquals(txMetaExpected, txMeta);
        }

        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                storage.remove(txIds.get(i));
            }
        }

        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                TxMeta txMeta = storage.get(txIds.get(i));
                assertNull(txMeta);
            } else {
                TxMeta txMeta = storage.get(txIds.get(i));
                TxMeta txMetaExpected = new TxMeta(TxState.COMMITED, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
                assertTxMetaEquals(txMetaExpected, txMeta);
            }
        }
    }

    private List<ReplicationGroupId> generateEnlistedPartitions(int c) {
        return IntStream.range(0, c).mapToObj(TestReplicationGroupId::new).collect(toList());
    }

    private HybridTimestamp generateTimestamp(UUID uuid) {
        long physical = Math.abs(uuid.getMostSignificantBits());

        if (physical == 0) {
            physical++;
        }

        int logical = Math.abs(Long.valueOf(uuid.getLeastSignificantBits()).intValue());

        return new HybridTimestamp(physical, logical);
    }

    @Test
    public void testCas() {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        UUID txId = UUID.randomUUID();

        TxMeta txMeta1 = new TxMeta(TxState.COMMITED, new ArrayList<>(), generateTimestamp(txId));
        TxMeta txMeta2 = new TxMeta(TxState.COMMITED, new ArrayList<>(), generateTimestamp(UUID.randomUUID()));

        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1, 1));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1, 1));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta1, 1, 1));

        TxMeta txMetaWrongTimestamp0 =
                new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), generateTimestamp(UUID.randomUUID()));
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaWrongTimestamp0, 1, 1));

        TxMeta txMetaNullTimestamp0 = new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp0, 3, 2));

        assertTxMetaEquals(storage.get(txId), txMeta1);

        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3, 2));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3, 2));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta2, 3, 2));

        TxMeta txMetaNullTimestamp2 = new TxMeta(txMeta2.txState(), txMeta2.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp2, 3, 2));

        assertTxMetaEquals(storage.get(txId), txMeta2);
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
                assertTxMetaEquals(txMeta, txData.getValue());
            }

            assertTrue(txs.isEmpty());
        }
    }

    @Test
    public void testDestroy() {
        TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
        TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);

        UUID txId0 = UUID.randomUUID();
        storage0.put(txId0, new TxMeta(TxState.COMMITED, generateEnlistedPartitions(1), generateTimestamp(txId0)));

        UUID txId1 = UUID.randomUUID();
        storage1.put(txId1, new TxMeta(TxState.COMMITED, generateEnlistedPartitions(1), generateTimestamp(txId1)));

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
    public void testSuccessFullRebalance() throws Exception {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        // We can't finish a full rebalance that we haven't started.
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_FULL_REBALANCE_ERR, () -> storage.finishFullRebalance(100, 500));

        assertEquals(0, storage.lastAppliedIndex());
        assertEquals(0, storage.persistedIndex());
        assertEquals(0, storage.lastAppliedTerm());

        // Let's fill the storage with data before the start of a full rebalance.

        UUID txIdBeforeFullRebalance0 = UUID.randomUUID();
        UUID txIdBeforeFullRebalance1 = UUID.randomUUID();

        TxMeta txMetaBeforeRebalance0 = randomTxMeta(1, txIdBeforeFullRebalance0);
        TxMeta txMetaBeforeRebalance1 = randomTxMeta(1, txIdBeforeFullRebalance1);

        storage.put(txIdBeforeFullRebalance0, txMetaBeforeRebalance0);
        storage.put(txIdBeforeFullRebalance1, txMetaBeforeRebalance1);

        // Let's start a full rebalance.
        storage.startFullRebalance().get(1, SECONDS);

        checkTxStateStorageReadMethodsWhenFullRebalanceInProgress(storage);

        // We cannot start a new rebalance until the current one has ended.
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_FULL_REBALANCE_ERR, storage::startFullRebalance);

        // Let's fill it with new data.

        UUID txIdOnFullRebalance0 = new UUID(0, 0);
        UUID txIdOnFullRebalance1 = new UUID(1, 1);

        TxMeta txMetaOnFullRebalance0 = randomTxMeta(1, txIdOnFullRebalance0);
        TxMeta txMetaOnFullRebalance1 = randomTxMeta(1, txIdOnFullRebalance1);

        storage.put(txIdOnFullRebalance0, txMetaOnFullRebalance0);
        assertTrue(storage.compareAndSet(txIdOnFullRebalance1, null, txMetaOnFullRebalance1, 10, 20));

        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.persistedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedTerm());

        // Let's complete a full rebalancing.

        storage.finishFullRebalance(30, 50).get(1, SECONDS);

        // Let's check the storage.

        assertEquals(30, storage.lastAppliedIndex());
        assertEquals(30, storage.persistedIndex());
        assertEquals(50, storage.lastAppliedTerm());

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(
                    scan.stream().collect(toList()),
                    contains(
                            new IgniteBiTuple<>(txIdOnFullRebalance0, txMetaOnFullRebalance0),
                            new IgniteBiTuple<>(txIdOnFullRebalance1, txMetaOnFullRebalance1)
                    )
            );
        }
    }

    @Test
    public void testFailFullRebalance() throws Exception {
        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

        // Nothing will happen because the full rebalance has not started.
        storage.abortFullRebalance().get(1, SECONDS);

        assertEquals(0, storage.lastAppliedIndex());
        assertEquals(0, storage.persistedIndex());
        assertEquals(0, storage.lastAppliedTerm());

        // Let's fill the storage with data before the start of a full rebalance.

        UUID txIdBeforeFullRebalance0 = UUID.randomUUID();
        UUID txIdBeforeFullRebalance1 = UUID.randomUUID();

        TxMeta txMetaBeforeRebalance0 = randomTxMeta(1, txIdBeforeFullRebalance0);
        TxMeta txMetaBeforeRebalance1 = randomTxMeta(1, txIdBeforeFullRebalance1);

        storage.put(txIdBeforeFullRebalance0, txMetaBeforeRebalance0);
        storage.put(txIdBeforeFullRebalance1, txMetaBeforeRebalance1);

        // Let's start a full rebalance.

        storage.startFullRebalance().get(1, SECONDS);

        checkTxStateStorageReadMethodsWhenFullRebalanceInProgress(storage);

        // We cannot start a new rebalance until the current one has ended.
        assertThrows(IgniteInternalException.class, storage::startFullRebalance);

        // Let's fill it with new data.

        UUID txIdOnFullRebalance0 = new UUID(0, 0);
        UUID txIdOnFullRebalance1 = new UUID(1, 1);

        TxMeta txMetaOnFullRebalance0 = randomTxMeta(1, txIdOnFullRebalance0);
        TxMeta txMetaOnFullRebalance1 = randomTxMeta(1, txIdOnFullRebalance1);

        storage.put(txIdOnFullRebalance0, txMetaOnFullRebalance0);
        assertTrue(storage.compareAndSet(txIdOnFullRebalance1, null, txMetaOnFullRebalance1, 10, 20));

        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.persistedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedTerm());

        // Let's abort a full rebalancing.

        storage.abortFullRebalance().get(1, SECONDS);

        // Let's check the storage.

        assertEquals(0, storage.lastAppliedIndex());
        assertEquals(0, storage.persistedIndex());
        assertEquals(0, storage.lastAppliedTerm());

        try (Cursor<IgniteBiTuple<UUID, TxMeta>> scan = storage.scan()) {
            assertThat(scan.stream().collect(toList()), is(empty()));
        }
    }

    private static void checkTxStateStorageReadMethodsWhenFullRebalanceInProgress(TxStateStorage storage) {
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.persistedIndex());
        assertEquals(FULL_REBALANCE_IN_PROGRESS, storage.lastAppliedTerm());

        assertThrowsIgniteInternalException(TX_STATE_STORAGE_FULL_REBALANCE_ERR, () -> storage.lastApplied(100, 500));
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_FULL_REBALANCE_ERR, () -> storage.get(UUID.randomUUID()));
        assertThrowsIgniteInternalException(TX_STATE_STORAGE_FULL_REBALANCE_ERR, storage::scan);
    }

    private IgniteBiTuple<UUID, TxMeta> putRandomTxMetaWithCommandIndex(TxStateStorage storage, int enlistedPartsCount, long commandIndex) {
        UUID txId = UUID.randomUUID();
        TxMeta txMeta = randomTxMeta(enlistedPartsCount, txId);
        storage.compareAndSet(txId, null, txMeta, commandIndex, 1);

        return new IgniteBiTuple<>(txId, txMeta);
    }

    private TxMeta randomTxMeta(int enlistedPartsCount, UUID txId) {
        return new TxMeta(TxState.COMMITED, generateEnlistedPartitions(enlistedPartsCount), generateTimestamp(txId));
    }

    private static void assertTxMetaEquals(TxMeta txMeta0, TxMeta txMeta1) {
        assertEquals(txMeta0.txState(), txMeta1.txState());
        assertEquals(txMeta0.commitTimestamp(), txMeta1.commitTimestamp());
        assertEquals(txMeta0.enlistedPartitions(), txMeta1.enlistedPartitions());
    }

    private static void assertThrowsIgniteInternalException(int expFullErrorCode, Executable executable) {
        IgniteInternalException exception = assertThrows(IgniteInternalException.class, executable);

        assertEquals(expFullErrorCode, exception.code());
    }

    /**
     * Test implementation of replication group id.
     */
    private static class TestReplicationGroupId implements ReplicationGroupId {
        /** Partition id. */
        private final int prtId;

        /**
         * The constructor.
         *
         * @param prtId Partition id.
         */
        private TestReplicationGroupId(int prtId) {
            this.prtId = prtId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestReplicationGroupId that = (TestReplicationGroupId) o;
            return prtId == that.prtId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(prtId);
        }

        @Override
        public String toString() {
            return "part_" + prtId;
        }
    }
}
