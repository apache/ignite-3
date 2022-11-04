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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

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
        assertThrows(StorageException.class, () -> tableStorage.startRebalance(PARTITION_ID_1));

        UUID txId = UUID.randomUUID();
        TxMeta txMeta = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        assertThat(storage, instanceOf(TxStateStorageDecorator.class));

        storage.put(txId, txMeta);

        storage.lastAppliedIndex(100);

        storage.flush().get(1, TimeUnit.SECONDS);

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        TxStateStorage newStorage0 = tableStorage.getTxStateStorage(PARTITION_ID);

        assertNotNull(newStorage0);

        assertSame(storage, newStorage0);

        assertEquals(0L, newStorage0.lastAppliedIndex());
        assertEquals(0L, newStorage0.persistedIndex());

        assertThat(getAll(newStorage0.scan()), empty());

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        TxStateStorage newStorage1 = tableStorage.getTxStateStorage(PARTITION_ID);

        assertSame(newStorage0, newStorage1);
    }

    @Test
    public void testAbortRebalance() throws Exception {
        assertDoesNotThrow(() -> tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        UUID oldTxId = UUID.randomUUID();
        TxMeta oldTxMeta = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        storage.put(oldTxId, oldTxMeta);

        storage.lastAppliedIndex(100);

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        UUID newTxId = UUID.randomUUID();
        TxMeta newTxMeta = mock(TxMeta.class);

        storage.put(newTxId, newTxMeta);

        storage.lastAppliedIndex(500);

        tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(storage, tableStorage.getTxStateStorage(PARTITION_ID));

        assertDoesNotThrow(() -> tableStorage.abortRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        assertEquals(100L, storage.lastAppliedIndex());

        assertThat(getAll(storage.scan()), containsInAnyOrder(new IgniteBiTuple<>(oldTxId, oldTxMeta)));
    }

    @Test
    public void testFinishRebalance() throws Exception {
        assertDoesNotThrow(() -> tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        UUID oldTxId = UUID.randomUUID();
        TxMeta oldTxMeta = mock(TxMeta.class);

        TxStateStorage storage = tableStorage.getOrCreateTxStateStorage(PARTITION_ID);

        storage.put(oldTxId, oldTxMeta);

        storage.lastAppliedIndex(100);

        tableStorage.startRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        UUID newTxId = UUID.randomUUID();
        TxMeta newTxMeta = mock(TxMeta.class);

        storage.put(newTxId, newTxMeta);

        storage.lastAppliedIndex(500);

        tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS);

        assertSame(storage, tableStorage.getTxStateStorage(PARTITION_ID));

        assertDoesNotThrow(() -> tableStorage.finishRebalance(PARTITION_ID).get(1, TimeUnit.SECONDS));

        assertEquals(500L, storage.lastAppliedIndex());

        assertThat(getAll(storage.scan()), containsInAnyOrder(new IgniteBiTuple<>(newTxId, newTxMeta)));
    }

    private static <T> List<T> getAll(Cursor<T> cursor) {
        try (cursor) {
            return cursor.stream().collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
