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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionExpirationRegistryTest extends BaseIgniteAbstractTest {
    private TransactionExpirationRegistry registry;
    private VolatileTxStateMetaStorage txStateMetaStorage;

    @Mock
    private InternalTransaction tx1;

    @Mock
    private InternalTransaction tx2;

    @BeforeEach
    void configureMocks() {
        txStateMetaStorage = VolatileTxStateMetaStorage.createStarted();
        registry = new TransactionExpirationRegistry(txStateMetaStorage);

        HybridClock clock = new TestHybridClock(() -> 0);

        UUID txId1 = transactionId(clock.now(), 0);
        UUID txId2 = transactionId(clock.now(), 0);

        lenient().when(tx1.id()).thenReturn(txId1);
        lenient().when(tx2.id()).thenReturn(txId2);
        lenient().when(tx1.rollbackTimeoutExceededAsync()).thenReturn(nullCompletedFuture());
        lenient().when(tx2.rollbackTimeoutExceededAsync()).thenReturn(nullCompletedFuture());
    }

    @Test
    void abortsTransactionsBeforeExpirationTime() {
        registry.register(tx1, 1000);
        registry.register(tx2, 2000);

        registry.expireUpTo(3000);

        verify(tx1).rollbackTimeoutExceededAsync();
        verify(tx2).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsTransactionsExactlyOnExpirationTime() {
        registry.register(tx1, 1000);

        registry.expireUpTo(1000);

        verify(tx1).rollbackTimeoutExceededAsync();
    }

    @Test
    void doesNotAbortTransactionsAfterExpirationTime() {
        registry.register(tx1, 1001);

        registry.expireUpTo(1000);

        verify(tx1, never()).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsTransactionsExpiredAfterFewExpirations() {
        registry.register(tx1, 1000);

        registry.expireUpTo(1000);
        registry.expireUpTo(2000);

        verify(tx1).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsTransactionsWithSameExpirationTime() {
        registry.register(tx1, 1000);
        registry.register(tx2, 1000);

        registry.expireUpTo(2000);

        verify(tx1).rollbackTimeoutExceededAsync();
        verify(tx2).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsAlreadyExpiredTransactionOnRegistration() {
        registry.expireUpTo(2000);

        registry.register(tx1, 1000);
        registry.register(tx2, 2000);

        verify(tx1).rollbackTimeoutExceededAsync();
        verify(tx2).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsAlreadyExpiredTransactionJustOnce() {
        registry.expireUpTo(2000);

        registry.register(tx1, 1000);
        registry.register(tx2, 2000);

        registry.expireUpTo(2000);

        verify(tx1, times(1)).rollbackTimeoutExceededAsync();
        verify(tx2, times(1)).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsAllRegistered() {
        registry.register(tx1, 1000);
        registry.register(tx2, Long.MAX_VALUE);

        registry.abortAllRegistered();

        verify(tx1).rollbackTimeoutExceededAsync();
        verify(tx2).rollbackTimeoutExceededAsync();
    }

    @Test
    void abortsOnRegistrationAfterAbortingAllRegistered() {
        registry.abortAllRegistered();

        registry.register(tx1, 1000);
        registry.register(tx2, Long.MAX_VALUE);

        verify(tx1).rollbackTimeoutExceededAsync();
        verify(tx2).rollbackTimeoutExceededAsync();
    }

    @Test
    void removesTransactionOnUnregister() {
        registry.register(tx1, 1000);

        lenient().when(tx1.getTimeout()).thenReturn(1000L);

        registry.unregister(tx1);

        registry.expireUpTo(2000);

        // Should not be aborted due to expiration as we removed the transaction.
        verify(tx1, never()).rollbackTimeoutExceededAsync();
    }

    @Test
    void unregisterIsIdempotent() {
        registry.register(tx1, 1000);

        lenient().when(tx1.getTimeout()).thenReturn(1000L);

        registry.unregister(tx1);

        assertDoesNotThrow(() -> registry.unregister(tx1));
    }
}
