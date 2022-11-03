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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tx storage test.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class TxStateStorageAbstractTest {
    @WorkDirectory
    protected Path workDir;

    private TxStateTableStorage tableStorage;

    @BeforeEach
    void initStorage() {
        tableStorage = createStorage();
    }

    @AfterEach
    void closeStorage() {
        if (tableStorage != null) {
            tableStorage.close();
        }
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

        storage.close();
    }

    private List<ReplicationGroupId> generateEnlistedPartitions(int c) {
        return IntStream.range(0, c).mapToObj(i -> new TestReplicationGroupId(i)).collect(toList());
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

        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, null, txMeta1, 1));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta1, 1));

        TxMeta txMetaWrongTimestamp0 =
                new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), generateTimestamp(UUID.randomUUID()));
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaWrongTimestamp0, 1));

        TxMeta txMetaNullTimestamp0 = new TxMeta(txMeta1.txState(), txMeta1.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp0, 3));

        assertTxMetaEquals(storage.get(txId), txMeta1);

        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3));
        // Checking idempotency.
        assertTrue(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 3));
        assertTrue(storage.compareAndSet(txId, TxState.ABORTED, txMeta2, 3));

        TxMeta txMetaWrongTimestamp2 =
                new TxMeta(txMeta2.txState(), txMeta2.enlistedPartitions(), generateTimestamp(UUID.randomUUID()));

        TxMeta txMetaNullTimestamp2 = new TxMeta(txMeta2.txState(), txMeta2.enlistedPartitions(), null);
        assertFalse(storage.compareAndSet(txId, TxState.ABORTED, txMetaNullTimestamp2, 3));

        assertTxMetaEquals(storage.get(txId), txMeta2);

        storage.close();
    }

    @Test
    public void testScan() throws Exception {
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

        List.of(storage0, storage1, storage2).forEach(TxStateStorage::close);
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

        storage0.close();
        storage1.close();
    }

    @Test
    public void scansInOrderDefinedByTxIds() throws Exception {
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

        partitionStorage.close();
    }

    @Test
    public void scanOnlySeesDataExistingAtTheMomentOfCreation() throws Exception {
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

        partitionStorage.close();
    }

    private IgniteBiTuple<UUID, TxMeta> putRandomTxMetaWithCommandIndex(TxStateStorage storage, int enlistedPartsCount, long commandIndex) {
        UUID txId = UUID.randomUUID();
        TxMeta txMeta = randomTxMeta(enlistedPartsCount, txId);
        storage.compareAndSet(txId, null, txMeta, commandIndex);

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

    /**
     * Creates {@link TxStateStorage} to test.
     *
     * @return Tx state storage.
     */
    protected abstract TxStateTableStorage createStorage();

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
        public TestReplicationGroupId(int prtId) {
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
