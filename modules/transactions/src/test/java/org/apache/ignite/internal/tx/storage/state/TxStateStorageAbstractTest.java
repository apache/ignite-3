/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tx storage test.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class TxStateStorageAbstractTest {
    @WorkDirectory
    protected Path workDir;

    @AfterEach
    public void afterTest() {
        destroyStorage();
    }

    @Test
    public void testPutGetRemove() throws Exception {
        try (TxStateTableStorage tableStorage = createStorage()) {
            TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

            List<UUID> txIds = new ArrayList<>();

            for (int i = 0; i < 100; i++) {
                UUID txId = UUID.randomUUID();

                txIds.add(txId);

                storage.put(txId, new TxMeta(TxState.PENDING, generateEnlistedPartitions(i), generateTimestamp(txId)));
            }

            for (int i = 0; i < 100; i++) {
                TxMeta txMeta = storage.get(txIds.get(i));
                TxMeta txMetaExpected = new TxMeta(TxState.PENDING, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
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
                    TxMeta txMetaExpected = new TxMeta(TxState.PENDING, generateEnlistedPartitions(i), generateTimestamp(txIds.get(i)));
                    assertTxMetaEquals(txMetaExpected, txMeta);
                }
            }
        }
    }

    private List<String> generateEnlistedPartitions(int c) {
        return IntStream.range(0, c).mapToObj(String::valueOf).collect(toList());
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
    public void testCas() throws Exception {
        try (TxStateTableStorage tableStorage = createStorage()) {
            TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(0);

            UUID txId = UUID.randomUUID();

            TxMeta txMeta0 = new TxMeta(TxState.PENDING, new ArrayList<>(), generateTimestamp(txId));
            TxMeta txMeta1 = new TxMeta(TxState.COMMITED, new ArrayList<>(), generateTimestamp(txId));
            TxMeta txMeta2 = new TxMeta(TxState.COMMITED, new ArrayList<>(), generateTimestamp(UUID.randomUUID()));

            storage.put(txId, txMeta0);

            assertTxMetaEquals(storage.get(txId), txMeta0);

            assertFalse(storage.compareAndSet(txId, txMeta1.txState(), txMeta2, 1));
            assertTrue(storage.compareAndSet(txId, txMeta0.txState(), txMeta2, 2));

            assertTxMetaEquals(storage.get(txId), txMeta2);
        }
    }

    @Test
    public void testScan() throws Exception {
        try (TxStateTableStorage tableStorage = createStorage()) {
            TxStateStorage storage0 = tableStorage.getOrCreateTxStateStorage(0);
            TxStateStorage storage1 = tableStorage.getOrCreateTxStateStorage(1);

            Map<UUID, TxMeta> txs = new HashMap<>();

            for (int i = 0; i < 100; i++) {
                UUID txId = UUID.randomUUID();
                TxMeta txMeta = new TxMeta(TxState.PENDING, generateEnlistedPartitions(i), generateTimestamp(txId));
                txs.put(txId, txMeta);
                storage0.put(txId, txMeta);
                storage0.compareAndSet(txId, TxState.PENDING, txMeta, i);
            }

            UUID txId1 = UUID.randomUUID();
            TxMeta txMeta1 = new TxMeta(TxState.PENDING, generateEnlistedPartitions(0), generateTimestamp(txId1));
            storage1.put(txId1, txMeta1);
            storage1.compareAndSet(txId1, TxState.PENDING, txMeta1, 0);

            try (Cursor<IgniteBiTuple<UUID, TxMeta>> scanCursor = storage0.scan()) {
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
     * Destroy storage, for proper clean-up.
     */
    protected abstract void destroyStorage();
}
