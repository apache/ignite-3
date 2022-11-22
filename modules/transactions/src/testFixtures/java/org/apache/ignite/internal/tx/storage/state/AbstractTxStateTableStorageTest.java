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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

/**
 * Abstract class for {@link TxStateTableStorage} testing.
 */
public abstract class AbstractTxStateTableStorageTest {
    private static final int PARTITION_ID = 0;

    private static final int PARTITION_ID_1 = 1;

    private TxStateTableStorage tableStorage;

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    protected final void initialize(TxStateTableStorage tableStorage) {
        this.tableStorage = tableStorage;
    }

    @Test
    public void testStartRebalance() throws Exception {
        // Let's check that for a non-existent partition we cannot start a rebalance.
        assertThrows(StorageException.class, () -> tableStorage.startRebalance(PARTITION_ID_1));

        UUID txId0 = UUID.randomUUID();
        UUID txId1 = UUID.randomUUID();

        TxMeta txMeta0 = mock(TxMeta.class);
        TxMeta txMeta1 = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        storage.put(txId0, txMeta0);
        storage.put(txId1, txMeta1);

        storage.lastApplied(100, 500);

        storage.flush().get(1, TimeUnit.SECONDS);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanBeforeStartRebalance = storage.scan();

        // Let's skip first element in cursor to check that after starting rebalance, we will read data until rebalance starts.
        scanBeforeStartRebalance.next();

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(storage, tableStorage.getTxStateStorage(PARTITION_ID));

        // Let's skip first element in cursor to check that after starting rebalance, we will read data until rebalance starts.
        Cursor<IgniteBiTuple<UUID, TxMeta>> scanAfterStartRebalance = storage.scan();

        scanAfterStartRebalance.next();

        UUID txId2 = UUID.randomUUID();
        UUID txId3 = UUID.randomUUID();

        TxMeta txMeta2 = mock(TxMeta.class);
        TxMeta txMeta3 = mock(TxMeta.class);

        storage.put(txId2, txMeta2);
        storage.compareAndSet(txId3, null, txMeta3, 500, 1_000);

        storage.lastApplied(5_000, 10_000);

        assertEquals(100L, storage.lastAppliedIndex());
        assertEquals(500L, storage.lastAppliedTerm());
        assertEquals(100L, storage.persistedIndex());

        assertEquals(txMeta0, storage.get(txId0));
        assertEquals(txMeta1, storage.get(txId1));

        assertNull(storage.get(txId2));
        assertNull(storage.get(txId3));

        assertThat(getAll(storage.scan()), containsInAnyOrder(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1)));

        assertThat(
                getAllRemaining(scanBeforeStartRebalance),
                containsInAnyOrder(is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))))
        );

        assertThat(
                getAllRemaining(scanAfterStartRebalance),
                containsInAnyOrder(is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))))
        );

        // Let's check that until the current rebalancing is completed, when trying to start a new one, there will be an error.
        assertThrows(StorageException.class, () -> tableStorage.startRebalance(PARTITION_ID));
    }

    @Test
    public void testAbortRebalance() throws Exception {
        // Let's check that there will be no error if we try to abort a rebalance that has not been started.
        assertDoesNotThrow(() -> tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        UUID txId0 = UUID.randomUUID();
        UUID txId1 = UUID.randomUUID();

        TxMeta txMeta0 = mock(TxMeta.class);
        TxMeta txMeta1 = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        storage.put(txId0, txMeta0);
        storage.put(txId1, txMeta1);

        storage.lastApplied(100, 500);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanBeforeStartRebalance = storage.scan();

        // Let's skip first element in cursor to check that after aborting rebalance, we will read data until rebalance starts.
        scanBeforeStartRebalance.next();

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanAfterStartRebalance = storage.scan();

        // Let's skip first element in cursor to check that after aborting started rebalance, we will read data before start of rebalance.
        scanAfterStartRebalance.next();

        UUID txId2 = UUID.randomUUID();
        UUID txId3 = UUID.randomUUID();

        TxMeta txMeta2 = mock(TxMeta.class);
        TxMeta txMeta3 = mock(TxMeta.class);

        storage.put(txId2, txMeta2);
        storage.put(txId3, txMeta3);

        storage.lastApplied(500, 1_000);

        tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(storage, tableStorage.getTxStateStorage(PARTITION_ID));

        // Let's check that there will be no error if we try to abort the rebalance once again.
        assertDoesNotThrow(() -> tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        assertEquals(100L, storage.lastAppliedIndex());
        assertEquals(500L, storage.lastAppliedTerm());
        assertEquals(100L, storage.persistedIndex());

        assertEquals(txMeta0, storage.get(txId0));
        assertEquals(txMeta1, storage.get(txId1));

        assertNull(storage.get(txId3));
        assertNull(storage.get(txId3));

        assertThat(getAll(storage.scan()), containsInAnyOrder(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1)));

        assertThat(
                getAllRemaining(scanBeforeStartRebalance),
                containsInAnyOrder(is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))))
        );

        assertThat(
                getAllRemaining(scanAfterStartRebalance),
                containsInAnyOrder(is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))))
        );
    }

    @Test
    public void testFinishRebalance() throws Exception {
        // Let's check that there will be no error if we try to finish a rebalance that has not been started.
        assertDoesNotThrow(() -> tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        UUID txId0 = UUID.randomUUID();
        UUID txId1 = UUID.randomUUID();

        TxMeta txMeta0 = mock(TxMeta.class);
        TxMeta txMeta1 = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        storage.put(txId0, txMeta0);
        storage.put(txId1, txMeta1);

        storage.lastApplied(100, 500);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanBeforeStartRebalance = storage.scan();

        // Let's skip first element in cursor to check that after finishing rebalance, we will read data until rebalance starts.
        scanBeforeStartRebalance.next();

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        Cursor<IgniteBiTuple<UUID, TxMeta>> scanAfterStartRebalance = storage.scan();

        // Let's skip first element in cursor to check that after completion of started rebalance, we will read data before start of
        // rebalance and then new data.
        scanAfterStartRebalance.next();

        UUID txId2 = UUID.randomUUID();
        UUID txId3 = UUID.randomUUID();

        TxMeta txMeta2 = mock(TxMeta.class);
        TxMeta txMeta3 = mock(TxMeta.class);

        storage.put(txId0, txMeta0);
        storage.put(txId1, txMeta1);
        storage.put(txId2, txMeta2);
        storage.put(txId3, txMeta3);

        storage.lastApplied(500, 1_000);

        tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(storage, tableStorage.getTxStateStorage(PARTITION_ID));

        // Let's check that there will be no error if we try to finish the rebalance once again.
        assertDoesNotThrow(() -> tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        assertEquals(500L, storage.lastAppliedIndex());
        assertEquals(1_000L, storage.lastAppliedTerm());
        assertEquals(500L, storage.persistedIndex());

        assertEquals(txMeta0, storage.get(txId0));
        assertEquals(txMeta1, storage.get(txId1));
        assertEquals(txMeta2, storage.get(txId2));
        assertEquals(txMeta3, storage.get(txId3));

        assertThat(
                getAll(storage.scan()),
                containsInAnyOrder(
                        new IgniteBiTuple<>(txId0, txMeta0),
                        new IgniteBiTuple<>(txId1, txMeta1),
                        new IgniteBiTuple<>(txId2, txMeta2),
                        new IgniteBiTuple<>(txId3, txMeta3)
                )
        );

        assertThat(
                getAllRemaining(scanBeforeStartRebalance),
                containsInAnyOrder(is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))))
        );

        assertThat(
                getAllRemaining(scanAfterStartRebalance),
                containsInAnyOrder(
                        is(oneOf(new IgniteBiTuple<>(txId0, txMeta0), new IgniteBiTuple<>(txId1, txMeta1))),
                        eq(new IgniteBiTuple<>(txId2, txMeta2)),
                        eq(new IgniteBiTuple<>(txId3, txMeta3))
                )
        );
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(Collectors.toList());
        }
    }

    private static <T> List<T> getAllRemaining(Cursor<T> cursor) {
        try (cursor) {
            List<T> res = new ArrayList<>();

            while (cursor.hasNext()) {
                res.add(cursor.next());
            }

            return res;
        }
    }
}
