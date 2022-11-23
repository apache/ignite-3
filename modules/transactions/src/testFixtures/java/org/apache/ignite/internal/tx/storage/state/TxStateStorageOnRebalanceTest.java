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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private TxStateStorage oldStorage;

    @Mock
    private TxStateStorage newStorage;

    private TxStateStorageOnRebalance txStateStorageOnRebalance;

    @BeforeEach
    void setUp() {
        txStateStorageOnRebalance = new TxStateStorageOnRebalance(oldStorage, newStorage);
    }

    @Test
    void testGet() {
        UUID txId0 = UUID.randomUUID();

        txStateStorageOnRebalance.get(txId0);

        verify(oldStorage, times(1)).get(eq(txId0));
        verify(newStorage, never()).get(eq(txId0));

        txStateStorageOnRebalance.finishRebalance();

        UUID txId1 = UUID.randomUUID();

        txStateStorageOnRebalance.get(txId1);

        verify(oldStorage, never()).get(eq(txId1));
        verify(newStorage, times(1)).get(eq(txId1));
    }

    @Test
    void testPut() {
        UUID txId0 = UUID.randomUUID();
        TxMeta txMeta0 = mock(TxMeta.class);

        txStateStorageOnRebalance.put(txId0, txMeta0);

        verify(oldStorage, never()).put(eq(txId0), eq(txMeta0));
        verify(newStorage, times(1)).put(eq(txId0), eq(txMeta0));

        txStateStorageOnRebalance.finishRebalance();

        UUID txId1 = UUID.randomUUID();
        TxMeta txMeta1 = mock(TxMeta.class);

        txStateStorageOnRebalance.put(txId1, txMeta1);

        verify(oldStorage, never()).put(eq(txId1), eq(txMeta1));
        verify(newStorage, times(1)).put(eq(txId1), eq(txMeta1));
    }

    @Test
    void testCompareAndSet() {
        UUID txId0 = UUID.randomUUID();
        TxState txStateExpected0 = TxState.ABORTED;
        TxMeta txMeta0 = mock(TxMeta.class);
        long commandIndex0 = 100;
        long commandTerm0 = 500;

        txStateStorageOnRebalance.compareAndSet(txId0, txStateExpected0, txMeta0, commandIndex0, commandTerm0);

        verify(oldStorage, never()).compareAndSet(eq(txId0), eq(txStateExpected0), eq(txMeta0), eq(commandIndex0), eq(commandTerm0));
        verify(newStorage, times(1)).compareAndSet(eq(txId0), eq(txStateExpected0), eq(txMeta0), eq(commandIndex0), eq(commandTerm0));

        txStateStorageOnRebalance.finishRebalance();

        UUID txId1 = UUID.randomUUID();
        TxState txStateExpected1 = TxState.ABORTED;
        TxMeta txMeta1 = mock(TxMeta.class);
        long commandIndex1 = 100;
        long commandTerm1 = 500;

        txStateStorageOnRebalance.compareAndSet(txId1, txStateExpected1, txMeta1, commandIndex1, commandTerm1);

        verify(oldStorage, never()).compareAndSet(eq(txId1), eq(txStateExpected1), eq(txMeta1), eq(commandIndex1), eq(commandTerm1));
        verify(newStorage, times(1)).compareAndSet(eq(txId1), eq(txStateExpected1), eq(txMeta1), eq(commandIndex1), eq(commandTerm1));
    }

    @Test
    void testRemove() {
        UUID txId0 = UUID.randomUUID();

        txStateStorageOnRebalance.remove(txId0);

        verify(oldStorage, never()).remove(eq(txId0));
        verify(newStorage, times(1)).remove(eq(txId0));

        txStateStorageOnRebalance.finishRebalance();

        UUID txId1 = UUID.randomUUID();

        txStateStorageOnRebalance.remove(txId1);

        verify(oldStorage, never()).remove(eq(txId1));
        verify(newStorage, times(1)).remove(eq(txId1));
    }

    @Test
    void testScan() {
        txStateStorageOnRebalance.scan();

        verify(oldStorage, times(1)).scan();
        verify(newStorage, never()).scan();

        txStateStorageOnRebalance.finishRebalance();

        txStateStorageOnRebalance.scan();

        verify(oldStorage, times(1)).scan();
        verify(newStorage, times(1)).scan();
    }

    @Test
    void testFlush() {
        txStateStorageOnRebalance.flush();

        verify(oldStorage, never()).flush();
        verify(newStorage, times(1)).flush();

        txStateStorageOnRebalance.finishRebalance();

        txStateStorageOnRebalance.flush();

        verify(oldStorage, never()).flush();
        verify(newStorage, times(2)).flush();
    }

    @Test
    void testAppliedIndex() {
        txStateStorageOnRebalance.lastAppliedIndex();

        verify(oldStorage, times(1)).lastAppliedIndex();
        verify(newStorage, never()).lastAppliedIndex();

        txStateStorageOnRebalance.lastApplied(100, 500);

        verify(oldStorage, never()).lastApplied(eq(100L), eq(500L));
        verify(newStorage, times(1)).lastApplied(eq(100L), eq(500L));

        txStateStorageOnRebalance.persistedIndex();

        verify(oldStorage, times(1)).persistedIndex();
        verify(newStorage, never()).persistedIndex();

        txStateStorageOnRebalance.finishRebalance();

        txStateStorageOnRebalance.lastAppliedIndex();

        verify(oldStorage, times(1)).lastAppliedIndex();
        verify(newStorage, times(1)).lastAppliedIndex();

        txStateStorageOnRebalance.lastApplied(200, 700);

        verify(oldStorage, never()).lastApplied(eq(200L), eq(700L));
        verify(newStorage, times(1)).lastApplied(eq(200L), eq(700L));

        txStateStorageOnRebalance.persistedIndex();

        verify(oldStorage, times(1)).persistedIndex();
        verify(newStorage, times(1)).persistedIndex();
    }

    @Test
    void testDestroy() {
        txStateStorageOnRebalance.destroy();

        verify(oldStorage, times(1)).destroy();
        verify(newStorage, times(1)).destroy();

        txStateStorageOnRebalance.finishRebalance();

        txStateStorageOnRebalance.destroy();

        verify(oldStorage, times(2)).destroy();
        verify(newStorage, times(2)).destroy();
    }

    @Test
    void testClose() {
        txStateStorageOnRebalance.close();

        verify(oldStorage, times(1)).close();
        verify(newStorage, times(1)).close();

        txStateStorageOnRebalance.finishRebalance();

        txStateStorageOnRebalance.close();

        verify(oldStorage, times(2)).close();
        verify(newStorage, times(2)).close();
    }
}
