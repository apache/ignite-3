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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionExpirationRegistryTest extends BaseIgniteAbstractTest {
    private final TransactionExpirationRegistry registry = new TransactionExpirationRegistry();

    @Mock
    private InternalTransaction tx1;

    @Mock
    private InternalTransaction tx2;

    @BeforeEach
    void configureMocks() {
        lenient().when(tx1.rollbackAsync()).thenReturn(nullCompletedFuture());
        lenient().when(tx2.rollbackAsync()).thenReturn(nullCompletedFuture());
    }

    @Test
    void abortsTransactionsBeforeExpirationTime() {
        registry.register(tx1, new HybridTimestamp(1000, 0));
        registry.register(tx2, new HybridTimestamp(2000, 0));

        registry.expireUpTo(new HybridTimestamp(3000, 0));

        verify(tx1).rollbackAsync();
        verify(tx2).rollbackAsync();
    }

    @Test
    void abortsTransactionsExactlyOnExpirationTime() {
        registry.register(tx1, new HybridTimestamp(1000, 0));

        registry.expireUpTo(new HybridTimestamp(1000, 0));

        verify(tx1).rollbackAsync();
    }

    @Test
    void doesNotAbortTransactionsAfterExpirationTime() {
        registry.register(tx1, new HybridTimestamp(1000, 1));

        registry.expireUpTo(new HybridTimestamp(1000, 0));

        verify(tx1, never()).rollbackAsync();
    }

    @Test
    void abortsTransactionsExpiredAfterFewExpirations() {
        registry.register(tx1, new HybridTimestamp(1000, 1));

        registry.expireUpTo(new HybridTimestamp(1000, 0));
        registry.expireUpTo(new HybridTimestamp(2000, 0));

        verify(tx1).rollbackAsync();
    }

    @Test
    void abortsAlreadyExpiredTransactionOnRegistration() {
        registry.expireUpTo(new HybridTimestamp(2000, 0));

        registry.register(tx1, new HybridTimestamp(1000, 0));
        registry.register(tx2, new HybridTimestamp(2000, 0));

        verify(tx1).rollbackAsync();
        verify(tx2).rollbackAsync();
    }

    @Test
    void abortsAlreadyExpiredTransactionJustOnce() {
        registry.expireUpTo(new HybridTimestamp(2000, 0));

        registry.register(tx1, new HybridTimestamp(1000, 0));
        registry.register(tx2, new HybridTimestamp(2000, 0));

        registry.expireUpTo(new HybridTimestamp(2000, 0));

        verify(tx1, times(1)).rollbackAsync();
        verify(tx2, times(1)).rollbackAsync();
    }

    @Test
    void abortsAllRegistered() {
        registry.register(tx1, new HybridTimestamp(1000, 0));
        registry.register(tx2, HybridTimestamp.MAX_VALUE);

        registry.abortAllRegistered();

        verify(tx1).rollbackAsync();
        verify(tx2).rollbackAsync();
    }

    @Test
    void abortsOnRegistrationAfterAbortingAllRegistered() {
        registry.abortAllRegistered();

        registry.register(tx1, new HybridTimestamp(1000, 0));
        registry.register(tx2, HybridTimestamp.MAX_VALUE);

        verify(tx1).rollbackAsync();
        verify(tx2).rollbackAsync();
    }

    @Test
    void removesTransactionOnUnregister() {
        registry.register(tx1, new HybridTimestamp(1000, 0));

        registry.unregister(tx1);

        registry.expireUpTo(new HybridTimestamp(2000, 0));

        // Should not be aborted due to expiration as we removed the transaction.
        verify(tx1, never()).rollbackAsync();
    }

    @Test
    void unregisterIsIdempotent() {
        registry.register(tx1, new HybridTimestamp(1000, 0));

        registry.unregister(tx1);
        registry.unregister(tx1);
    }
}
