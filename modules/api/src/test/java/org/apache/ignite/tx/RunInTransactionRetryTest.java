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

package org.apache.ignite.tx;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

/**
 * Tests {@link IgniteTransactions#runInTransaction} and {@link IgniteTransactions#runInTransactionAsync} retries.
 */
public class RunInTransactionRetryTest {
    private static final int SHORT_TIMEOUT_MILLIS = 100;

    private long testStartTime;

    @BeforeEach
    public void setUp() {
        testStartTime = System.currentTimeMillis();
    }

    /**
     * Tests the different scenarios of retries.
     */
    @CartesianTest
    @CartesianTest.MethodFactory("testRetriesArgFactory")
    public void testRetries(
            boolean async,
            int closureFailureCount,
            int commitFailureCount,
            int rollbackFailureCount,
            ClosureFailureType closureFailureType
    ) {
        var closureFailures = new AtomicInteger(closureFailureCount);
        var igniteTransactions = new MockIgniteTransactions(commitFailureCount, rollbackFailureCount);

        Supplier<CompletableFuture<Integer>> testClosure;

        if (async) {
            testClosure = () -> igniteTransactions.runInTransactionAsync(
                    tx -> closureCall(tx, closureFailures, closureFailureType),
                    withShortTimeout()
            );
        } else {
            // closureFailureType is always SYNC in sync mode (closure shouldn't return future).
            testClosure = () -> igniteTransactions.runInTransaction(
                    (Function<Transaction, CompletableFuture<Integer>>) tx ->
                            closureCall(tx, closureFailures, ClosureFailureType.SYNC_FAIL),
                    withShortTimeout()
            );
        }

        boolean requiresEventualSuccess = closureFailureCount < Integer.MAX_VALUE
                // Commit failure can't be retried.
                && commitFailureCount == 0
                // Rollbacks should be retried until success or timeout, so the rollback must succeed before closure retry.
                && (closureFailureCount == 0 || rollbackFailureCount < Integer.MAX_VALUE);

        boolean syncFail = false;
        Exception ex = null;

        CompletableFuture<Integer> future = null;

        try {
            future = testClosure.get();
        } catch (Exception e) {
            syncFail = true;
            ex = e;
        }

        if (!syncFail) {
            try {
                future.join();

                // Closure succeeded, check that it's expected.
                assertTrue(requiresEventualSuccess);
            } catch (Exception e) {
                ex = e;
            }
        }

        if (requiresEventualSuccess) {
            assertEquals(42, future.join());
        } else {
            if (closureFailureCount == Integer.MAX_VALUE) {
                // Had to retry until timed out.
                checkTimeout();
            }

            assertNotNull(ex);

            if (!async) {
                assertTrue(syncFail);

                if (commitFailureCount > 0) {
                    if (closureFailureCount == Integer.MAX_VALUE || closureFailureCount > 0 && rollbackFailureCount == Integer.MAX_VALUE) {
                        // Closure exception should be rethrown.
                        assertThat(ex, instanceOf(FailedClosureTestException.class));
                    } else {
                        assertThat(ex, instanceOf(FailedCommitTestException.class));
                    }
                } else {
                    assertThat(ex, instanceOf(FailedClosureTestException.class));
                }
            } else {
                assertFalse(syncFail);

                assertThat(ex, instanceOf(CompletionException.class));
                assertThat(ex.getCause(), instanceOf(Exception.class));
                Exception cause = (Exception) ex.getCause();

                assertTrue(
                        cause instanceof FailedClosureTestException || cause instanceof FailedCommitTestException
                );
            }
        }
    }

    @SuppressWarnings("unused")
    private static ArgumentSets testRetriesArgFactory() {
        return ArgumentSets.argumentsForFirstParameter(true, false)
                .argumentsForNextParameter(0, 5, 10, Integer.MAX_VALUE)
                .argumentsForNextParameter(0, 5, 10, Integer.MAX_VALUE)
                .argumentsForNextParameter(0, 5, 10, Integer.MAX_VALUE)
                .argumentsForNextParameter(ClosureFailureType.SYNC_FAIL, ClosureFailureType.FUTURE_FAIL);
    }

    @Test
    public void testNoRetryAfterTimeout() {
        var igniteTransactions = new MockIgniteTransactions(0, 0);

        AtomicBoolean runned = new AtomicBoolean();

        assertThrows(
                FailedClosureTestException.class,
                () -> igniteTransactions.runInTransaction(
                        (Consumer<Transaction>) tx -> {
                            assertFalse(runned.get());
                            runned.set(true);
                            sleep(100);
                            throw new FailedClosureTestException();
                        },
                        new TransactionOptions().timeoutMillis(1)
                )
        );
    }

    @Test
    public void testNoRetryAfterTimeoutAsync() {
        var igniteTransactions = new MockIgniteTransactions(0, 0);

        AtomicBoolean runned = new AtomicBoolean();

        CompletableFuture<Integer> future = igniteTransactions.runInTransactionAsync(
                tx -> {
                    assertFalse(runned.get());
                    runned.set(true);
                    sleep(100);
                    throw new FailedClosureTestException();
                },
                new TransactionOptions().timeoutMillis(1)
        );

        try {
            future.join();
            fail();
        } catch (Exception e) {
            assertThat(e, instanceOf(CompletionException.class));
            assertThat(e.getCause(), instanceOf(FailedClosureTestException.class));
        }
    }

    private static TransactionOptions withShortTimeout() {
        return new TransactionOptions().timeoutMillis(SHORT_TIMEOUT_MILLIS);
    }

    private void checkTimeout() {
        long now = System.currentTimeMillis();
        long duration = now - testStartTime;
        // Assuming that at least 80% of timeout has passed (assuming currentTimeMillis inaccuracy).
        assertThat("duration was: " + duration, duration, greaterThan((long) (SHORT_TIMEOUT_MILLIS * 0.8)));
    }

    private static CompletableFuture<Integer> closureCall(
            Transaction tx,
            AtomicInteger closureFailures,
            ClosureFailureType closureFailureType
    ) {
        assertNotNull(tx);
        assertFalse(isFinished(tx));

        if (closureFailures.get() > 0) {
            closureFailures.decrementAndGet();

            if (closureFailureType == ClosureFailureType.SYNC_FAIL) {
                throw new FailedClosureTestException();
            } else if (closureFailureType == ClosureFailureType.FUTURE_FAIL) {
                return failedFuture(new FailedClosureTestException());
            } else {
                throw new AssertionError("unknown failure type");
            }
        } else {
            return completedFuture(42);
        }
    }

    private static boolean isFinished(Transaction tx) {
        assertInstanceOf(MockTransaction.class, tx);
        return ((MockTransaction) tx).finished;
    }

    private static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * If closure should fail, this enum defines the type of failure.
     */
    private enum ClosureFailureType {
        /** Closure throws exception. */
        SYNC_FAIL,

        /** Closure returns failed future. */
        FUTURE_FAIL
    }

    private static class MockIgniteTransactions implements IgniteTransactions {
        final AtomicInteger commitsToFail;
        final AtomicInteger rollbacksToFail;

        MockIgniteTransactions(int commitsToFail, int rollbacksToFail) {
            this.commitsToFail = new AtomicInteger(commitsToFail);
            this.rollbacksToFail = new AtomicInteger(rollbacksToFail);
        }

        @Override
        public Transaction begin(@Nullable TransactionOptions options) {
            return new MockTransaction(commitsToFail, rollbacksToFail);
        }

        @Override
        public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
            return completedFuture(begin(options));
        }
    }

    private static class MockTransaction implements Transaction {
        final AtomicInteger commitsToFail;
        final AtomicInteger rollbacksToFail;
        boolean finished;

        MockTransaction(AtomicInteger commitsToFail, AtomicInteger rollbacksToFail) {
            this.commitsToFail = commitsToFail;
            this.rollbacksToFail = rollbacksToFail;
        }

        @Override
        public void commit() throws TransactionException {
            try {
                commitAsync().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else {
                    throw e;
                }
            }
        }

        @Override
        public CompletableFuture<Void> commitAsync() {
            sleep(1);

            if (commitsToFail.get() > 0) {
                commitsToFail.decrementAndGet();
                return failedFuture(new FailedCommitTestException());
            } else {
                finished = true;
                return completedFuture(null);
            }
        }

        @Override
        public void rollback() throws TransactionException {
            try {
                rollbackAsync().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                } else {
                    throw e;
                }
            }
        }

        @Override
        public CompletableFuture<Void> rollbackAsync() {
            sleep(1);

            if (rollbacksToFail.get() > 0) {
                rollbacksToFail.decrementAndGet();
                return failedFuture(new FailedRollbackTestException());
            } else {
                finished = true;
                return completedFuture(null);
            }
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

    private static class FailedClosureTestException extends RuntimeException implements RetriableTransactionException {
        // No-op.
    }

    private static class FailedCommitTestException extends RuntimeException implements RetriableTransactionException {
        // No-op.
    }

    private static class FailedRollbackTestException extends RuntimeException implements RetriableTransactionException {
        // No-op.
    }
}
