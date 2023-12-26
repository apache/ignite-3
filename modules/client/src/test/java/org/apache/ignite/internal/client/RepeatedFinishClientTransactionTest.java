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

package org.apache.ignite.internal.client;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Tests repeated commit/rollback operations.
 */
public class RepeatedFinishClientTransactionTest extends BaseIgniteAbstractTest {
    @Test
    public void testRepeatedCommitRollbackAfterCommit() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txFinishStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1, false);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertNotSame(firstCommitFut, secondCommitFut);
        assertSame(secondCommitFut, rollbackFut);
        assertSame(secondCommitFut, tx.commitAsync());
        assertSame(rollbackFut, tx.rollbackAsync());

        assertFalse(firstCommitFut.isDone());
        assertFalse(secondCommitFut.isDone());
        assertFalse(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstCommitFut.get(3, TimeUnit.SECONDS);
        assertTrue(firstCommitFut.isDone());
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollback() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txFinishStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1, false);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstRollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> commitFut = tx.commitAsync();

        CompletableFuture<Void> secondRollbackFut = tx.rollbackAsync();

        assertNotSame(firstRollbackFut, secondRollbackFut);
        assertSame(secondRollbackFut, commitFut);
        assertSame(commitFut, tx.commitAsync());
        assertSame(secondRollbackFut, tx.rollbackAsync());

        assertFalse(firstRollbackFut.isDone());
        assertFalse(secondRollbackFut.isDone());
        assertFalse(commitFut.isDone());

        secondFinishLatch.countDown();

        firstRollbackFut.get(3, TimeUnit.SECONDS);
        assertTrue(firstRollbackFut.isDone());
        assertTrue(secondRollbackFut.isDone());
        assertTrue(commitFut.isDone());
    }

    @Test
    public void testRepeatedCommitRollbackAfterCommitWithException() throws Exception {
        TestClientChannel clientChannel = mock(TestClientChannel.class);

        when(clientChannel.serviceAsync(anyInt(), any(), any())).thenReturn(failedFuture(new Exception("Expected exception.")));

        ClientTransaction tx = new ClientTransaction(clientChannel, 1, false);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> commitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        try {
            commitFut.get(3, TimeUnit.SECONDS);

            fail();
        } catch (Exception ignored) {
            // No op.
        }

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollbackWithException() throws Exception {
        TestClientChannel clientChannel = mock(TestClientChannel.class);

        when(clientChannel.serviceAsync(anyInt(), any(), any())).thenReturn(failedFuture(new Exception("Expected exception.")));

        ClientTransaction tx = new ClientTransaction(clientChannel, 1, false);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> rollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        try {
            rollbackFut.get(3, TimeUnit.SECONDS);

            fail();
        } catch (Exception ignored) {
            // No op.
        }

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    private static class TestClientChannel implements ClientChannel {
        private final CountDownLatch txFinishStartedLatch;

        private final CountDownLatch secondFinishLatch;

        TestClientChannel(CountDownLatch txFinishStartedLatch, CountDownLatch secondFinishLatch) {
            this.txFinishStartedLatch = txFinishStartedLatch;
            this.secondFinishLatch = secondFinishLatch;
        }

        @Override
        public <T> CompletableFuture<T> serviceAsync(
                int opCode, PayloadWriter payloadWriter, PayloadReader<T> payloadReader, boolean expectNotifications) {
            txFinishStartedLatch.countDown();

            try {
                secondFinishLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return nullCompletedFuture();
        }

        @Override
        public boolean closed() {
            return false;
        }

        @Override
        public ProtocolContext protocolContext() {
            return null;
        }

        @Override
        public void addPartitionAssignmentChangeListener(Consumer<Long> listener) {
            // No-op.
        }

        @Override
        public void addObservableTimestampListener(Consumer<Long> listener) {
            // No-op.
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
