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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link TxStateStorageOnRebalance} testing.
 */
@ExtendWith(MockitoExtension.class)
public class TxStateStorageOnRebalanceTest {
    @Mock
    private TxStateStorage txStateStorage;

    private TxStateStorageOnRebalance txStateStorageOnRebalance;

    @BeforeEach
    void setUp() {
        txStateStorageOnRebalance = new TxStateStorageOnRebalance(txStateStorage);
    }

    @Test
    void testGet() {
        assertThrows(IllegalStateException.class, () -> txStateStorageOnRebalance.get(UUID.randomUUID()));
    }

    @Test
    void testPut() {
        UUID txId = UUID.randomUUID();
        TxMeta txMeta = mock(TxMeta.class);

        txStateStorageOnRebalance.put(txId, txMeta);

        verify(txStateStorage, times(1)).put(eq(txId), eq(txMeta));
    }

    @Test
    void testCompareAndSet() {
        UUID txId = UUID.randomUUID();
        TxState txStateExpected = TxState.ABORTED;
        TxMeta txMeta = mock(TxMeta.class);
        long commandIndex = 100;
        long commandTerm = 500;

        txStateStorageOnRebalance.compareAndSet(txId, txStateExpected, txMeta, commandIndex, commandTerm);

        verify(txStateStorage, times(1)).compareAndSet(eq(txId), eq(txStateExpected), eq(txMeta), eq(commandIndex), eq(commandTerm));
    }

    @Test
    void testRemove() {
        UUID txId = UUID.randomUUID();

        txStateStorageOnRebalance.remove(txId);

        verify(txStateStorage, times(1)).remove(eq(txId));
    }

    @Test
    void testScan() {
        assertThrows(IllegalStateException.class, () -> txStateStorageOnRebalance.scan());
    }

    @Test
    void testFlush() {
        txStateStorageOnRebalance.flush();

        verify(txStateStorage, times(1)).flush();
    }

    @Test
    void testAppliedIndex() {
        txStateStorageOnRebalance.lastAppliedIndex();

        verify(txStateStorage, times(1)).lastAppliedIndex();

        txStateStorageOnRebalance.lastApplied(100, 500);

        verify(txStateStorage, times(1)).lastApplied(eq(100L), eq(500L));

        txStateStorageOnRebalance.persistedIndex();

        verify(txStateStorage, times(1)).persistedIndex();
    }

    @Test
    void testDestroy() {
        txStateStorageOnRebalance.destroy();

        verify(txStateStorage, times(1)).destroy();
    }

    @Test
    void testClose() throws Exception {
        txStateStorageOnRebalance.close();

        verify(txStateStorage, times(1)).close();
    }
}
