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

package org.apache.ignite.client;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientTransactionInflights;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests inflight tracker.
 */
public class ClientTransactionInflightTest {
    private final UUID txId = UUID.randomUUID();

    private final ClientTransactionInflights inflights = new ClientTransactionInflights();

    private final ClientTransaction transaction;

    {
        transaction = mock(ClientTransaction.class);
        Mockito.when(transaction.txId()).thenReturn(txId);
    }

    @Test
    public void testState1() {
        inflights.addInflight(transaction);

        assertEquals(1, inflights.map().get(txId).inflights);
    }

    @Test
    public void testState2() {
        assertThrows(AssertionError.class, () -> inflights.removeInflight(txId, null));
    }

    @Test
    public void testState3() {
        inflights.addInflight(transaction);
        inflights.removeInflight(txId, null);

        CompletableFuture<Void> fut = inflights.finishFuture(txId);
        assertTrue(fut.isDone());
    }

    @Test
    public void testState4() {
        inflights.addInflight(transaction);

        CompletableFuture<Void> fut = inflights.finishFuture(txId);
        assertFalse(fut.isDone());

        inflights.removeInflight(txId, null);

        assertThat(fut, willCompleteSuccessfully());
    }

    @Test
    public void testState5() {
        inflights.addInflight(transaction);

        CompletableFuture<Void> fut = inflights.finishFuture(txId);
        assertFalse(fut.isDone());

        inflights.removeInflight(txId, new TestException());

        assertThat(fut, CompletableFutureExceptionMatcher.willThrow(TestException.class));
    }

    @Test
    public void testState6() {
        inflights.addInflight(transaction);
        inflights.removeInflight(txId, new TestException());

        CompletableFuture<Void> fut = inflights.finishFuture(txId);

        assertThat(fut, CompletableFutureExceptionMatcher.willThrow(TestException.class));
    }

    private static class TestException extends Exception {}
}
