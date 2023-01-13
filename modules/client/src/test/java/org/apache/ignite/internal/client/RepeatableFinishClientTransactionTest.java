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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.junit.jupiter.api.Test;

/**
 * Tests repeatable commit/rollback operations.
 */
public class RepeatableFinishClientTransactionTest {
    @Test
    public void testRepeatableCommitRollbackAfterCommit() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txFinishStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertNotSame(firstCommitFut, secondCommitFut);
        assertNotSame(secondCommitFut, rollbackFut);
        assertNotSame(firstCommitFut, rollbackFut);
        assertSame(secondCommitFut, tx.commitAsync());
        assertNotSame(rollbackFut, tx.rollbackAsync());

        assertFalse(firstCommitFut.isDone());
        assertFalse(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstCommitFut.get(3, TimeUnit.SECONDS);
        assertTrue(secondCommitFut.isDone());
    }

    @Test
    public void testRepeatableCommitRollbackAfterRollback() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txFinishStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstRollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> commitFut = tx.commitAsync();

        CompletableFuture<Void> secondRollbackFut = tx.rollbackAsync();

        assertNotSame(firstRollbackFut, commitFut);
        assertNotSame(commitFut, secondRollbackFut);
        assertNotSame(firstRollbackFut, secondRollbackFut);

        assertFalse(firstRollbackFut.isDone());
        assertTrue(commitFut.isDone());
        assertTrue(secondRollbackFut.isDone());

        secondFinishLatch.countDown();

        firstRollbackFut.get(3, TimeUnit.SECONDS);
        assertTrue(commitFut.isDone());
        assertTrue(secondRollbackFut.isDone());
    }

    private static class TestClientChannel implements ClientChannel {
        private final CountDownLatch txFinishStartedLatch;

        private final CountDownLatch secondFinishLatch;

        TestClientChannel(CountDownLatch txFinishStartedLatch, CountDownLatch secondFinishLatch) {
            this.txFinishStartedLatch = txFinishStartedLatch;
            this.secondFinishLatch = secondFinishLatch;
        }

        @Override
        public <T> CompletableFuture<T> serviceAsync(int opCode, PayloadWriter payloadWriter, PayloadReader<T> payloadReader) {
            txFinishStartedLatch.countDown();

            try {
                secondFinishLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return completedFuture(null);
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
        public void addTopologyAssignmentChangeListener(Consumer<ClientChannel> listener) {

        }

        @Override
        public void close() throws Exception {

        }
    }
}
