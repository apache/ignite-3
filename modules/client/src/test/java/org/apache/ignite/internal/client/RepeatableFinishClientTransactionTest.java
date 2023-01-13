package org.apache.ignite.internal.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.junit.jupiter.api.Test;

public class RepeatableFinishClientTransactionTest {
    @Test
    public void testRepeatableCommitRollbackAfterCommit() throws Exception {
        CountDownLatch txStartedLatch = new CountDownLatch(1);

        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        txStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertFalse(firstCommitFut == secondCommitFut);
        assertFalse(secondCommitFut == rollbackFut);
        assertTrue(secondCommitFut == tx.commitAsync());
        assertFalse(rollbackFut == tx.rollbackAsync());

        assertFalse(firstCommitFut.isDone());
        assertFalse(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstCommitFut.get(3, TimeUnit.SECONDS);
        assertTrue(secondCommitFut.isDone());
    }

    @Test
    public void testRepeatableCommitRollbackAfterRollback() throws Exception {
        CountDownLatch txStartedLatch = new CountDownLatch(1);

        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestClientChannel clientChannel = new TestClientChannel(txStartedLatch, secondFinishLatch);

        ClientTransaction tx = new ClientTransaction(clientChannel, 1);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstRollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        txStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertFalse(firstRollbackFut == secondCommitFut);
        assertFalse(secondCommitFut == rollbackFut);

        assertFalse(firstRollbackFut.isDone());
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstRollbackFut.get(3, TimeUnit.SECONDS);
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());
    }

    private static class TestClientChannel implements ClientChannel {
        private final CountDownLatch txStartedLatch;

        private final CountDownLatch secondFinishLatch;

        public TestClientChannel(CountDownLatch txStartedLatch, CountDownLatch secondFinishLatch) {
            this.txStartedLatch = txStartedLatch;
            this.secondFinishLatch = secondFinishLatch;
        }


        @Override
        public <T> CompletableFuture<T> serviceAsync(int opCode, PayloadWriter payloadWriter, PayloadReader<T> payloadReader) {
            txStartedLatch.countDown();

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
