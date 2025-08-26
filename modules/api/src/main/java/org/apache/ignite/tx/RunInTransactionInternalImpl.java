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

import static java.util.Collections.synchronizedList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.function.Function.identity;
import static org.apache.ignite.tx.IgniteTransactionDefaults.DEFAULT_RW_TX_TIMEOUT_SECONDS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * This is, in fact, the default implementation of the {@link IgniteTransactions#runInTransaction} and
 * {@link IgniteTransactions#runInTransactionAsync}, moved from the separate class to avoid the interface overloading. This
 * implementation is common for both client and embedded {@link IgniteTransactions}.
 */
class RunInTransactionInternalImpl {
    private static final int MAX_SUPPRESSED = 100;

    static <T> T runInTransactionInternal(
            IgniteTransactions igniteTransactions,
            Function<Transaction, T> clo,
            @Nullable TransactionOptions options,
            long startTimestamp,
            long initialTimeout
    ) throws TransactionException {
        Objects.requireNonNull(clo);

        TransactionOptions txOptions = options == null
                ? new TransactionOptions().timeoutMillis(TimeUnit.SECONDS.toMillis(DEFAULT_RW_TX_TIMEOUT_SECONDS))
                : options;

        List<Throwable> suppressed = new ArrayList<>();

        Transaction tx;
        T ret;

        while (true) {
            tx = igniteTransactions.begin(txOptions);

            try {
                ret = clo.apply(tx);

                break;
            } catch (Exception ex) {
                addSuppressedToList(suppressed, ex);

                long remainingTime = calcRemainingTime(initialTimeout, startTimestamp);

                if (remainingTime > 0 && isRetriable(ex)) {
                    // Rollback on user exception, should be retried until success or timeout to ensure the lock release
                    // before the next attempt.
                    rollbackWithRetry(tx, ex, startTimestamp, initialTimeout, suppressed);

                    long remaining = calcRemainingTime(initialTimeout, startTimestamp);

                    if (remaining > 0) {
                        // Will go on retry iteration.
                        txOptions = txOptions.timeoutMillis(remainingTime);
                    } else {
                        throwExceptionWithSuppressed(ex, suppressed);
                    }
                } else {
                    try {
                        // No retries here, rely on the durable finish.
                        tx.rollback();
                    } catch (Exception e) {
                        addSuppressedToList(suppressed, e);
                    }

                    throwExceptionWithSuppressed(ex, suppressed);
                }
            }
        }

        try {
            tx.commit();
        } catch (Exception e) {
            try {
                // Try to rollback tx in case if it's not finished. Retry is not needed here due to the durable finish.
                tx.rollback();
            } catch (Exception re) {
                e.addSuppressed(re);
            }

            throw e;
        }

        return ret;
    }

    private static void rollbackWithRetry(
            Transaction tx,
            Exception closureException,
            long startTimestamp,
            long initialTimeout,
            List<Throwable> suppressed
    ) {
        while (true) {
            try {
                tx.rollback();

                break;
            } catch (Exception re) {
                addSuppressedToList(suppressed, re);

                if (calcRemainingTime(initialTimeout, startTimestamp) <= 0) {
                    throwExceptionWithSuppressed(closureException, suppressed);
                }
            }
        }
    }

    static <T> CompletableFuture<T> runInTransactionAsyncInternal(
            IgniteTransactions igniteTransactions,
            Function<Transaction, CompletableFuture<T>> clo,
            @Nullable TransactionOptions options,
            long startTimestamp,
            long initialTimeout,
            @Nullable List<Throwable> suppressed
    ) {
        Objects.requireNonNull(clo);

        TransactionOptions txOptions = options == null
                ? new TransactionOptions().timeoutMillis(TimeUnit.SECONDS.toMillis(DEFAULT_RW_TX_TIMEOUT_SECONDS))
                : options;

        List<Throwable> sup = suppressed == null ? synchronizedList(new ArrayList<>()) : suppressed;

        return igniteTransactions
                .beginAsync(txOptions)
                // User closure with retries.
                .thenCompose(tx -> {
                    try {
                        return clo.apply(tx)
                                .handle((res, e) -> {
                                    if (e != null) {
                                        return handleClosureException(
                                                igniteTransactions,
                                                tx,
                                                clo,
                                                txOptions,
                                                startTimestamp,
                                                initialTimeout,
                                                sup,
                                                e
                                        );
                                    } else {
                                        return completedFuture(res);
                                    }
                                })
                                .thenCompose(identity())
                                .thenApply(res -> new TxWithVal<>(tx, res));
                    } catch (Exception e) {
                        return handleClosureException(igniteTransactions, tx, clo, txOptions, startTimestamp, initialTimeout, sup, e)
                                .thenApply(res -> new TxWithVal<>(tx, res));
                    }
                })
                // Transaction commit with rollback on failure, without retries.
                // Transaction rollback on closure failure is implemented in closure retry logic.
                .thenCompose(txWithVal ->
                        txWithVal.tx.commitAsync()
                                .handle((ignored, e) -> {
                                    if (e == null) {
                                        return completedFuture(null);
                                    } else {
                                        return txWithVal.tx.rollbackAsync()
                                                // Rethrow commit exception.
                                                .handle((ign, re) -> sneakyThrow(e));
                                    }
                                })
                                .thenCompose(fut -> fut)
                                .thenApply(ignored -> txWithVal.val)
                );
    }

    private static <T> CompletableFuture<T> handleClosureException(
            IgniteTransactions igniteTransactions,
            Transaction currentTx,
            Function<Transaction, CompletableFuture<T>> clo,
            TransactionOptions txOptions,
            long startTimestamp,
            long initialTimeout,
            List<Throwable> suppressed,
            Throwable e
    ) {
        addSuppressedToList(suppressed, e);

        long remainingTime = calcRemainingTime(initialTimeout, startTimestamp);

        if (remainingTime > 0 && isRetriable(e)) {
            // Rollback on user exception, should be retried until success or timeout to ensure the lock release
            // before the next attempt.
            return rollbackWithRetryAsync(currentTx, startTimestamp, initialTimeout, suppressed, e)
                    .thenCompose(ignored -> {
                        long remaining = calcRemainingTime(initialTimeout, startTimestamp);

                        if (remaining > 0) {
                            TransactionOptions opt = txOptions.timeoutMillis(remaining);

                            return runInTransactionAsyncInternal(
                                    igniteTransactions,
                                    clo,
                                    opt,
                                    startTimestamp,
                                    initialTimeout,
                                    suppressed
                            );
                        } else {
                            return throwExceptionWithSuppressedAsync(e, suppressed)
                                    .thenApply(ign -> null);
                        }
                    });
        } else {
            // No retries here, rely on the durable finish.
            return currentTx.rollbackAsync()
                    .exceptionally(re -> {
                        addSuppressedToList(suppressed, re);

                        return null;
                    })
                    .thenCompose(ignored -> throwExceptionWithSuppressedAsync(e, suppressed))
                    // Never executed.
                    .thenApply(ignored -> null);
        }
    }

    private static CompletableFuture<Void> rollbackWithRetryAsync(
            Transaction tx,
            long startTimestamp,
            long initialTimeout,
            List<Throwable> suppressed,
            Throwable e
    ) {
        return tx.rollbackAsync()
                .handle((ignored, re) -> {
                    CompletableFuture<Void> fut;

                    if (re == null) {
                        fut = completedFuture(null);
                    } else {
                        addSuppressedToList(suppressed, re);

                        if (calcRemainingTime(initialTimeout, startTimestamp) <= 0) {
                            for (Throwable s : suppressed) {
                                addSuppressed(e, s);
                            }

                            fut = failedFuture(e);
                        } else {
                            fut = rollbackWithRetryAsync(tx, startTimestamp, initialTimeout, suppressed, e);
                        }
                    }

                    return fut;
                })
                .thenCompose(identity());
    }

    private static void addSuppressedToList(List<Throwable> to, Throwable a) {
        if (to.size() < MAX_SUPPRESSED) {
            to.add(a);
        }
    }

    private static void addSuppressed(Throwable to, Throwable a) {
        if (to != null && a != null && to != a && to.getSuppressed().length < MAX_SUPPRESSED) {
            to.addSuppressed(a);
        }
    }

    private static void throwExceptionWithSuppressed(Throwable e, List<Throwable> suppressed) {
        for (Throwable t : suppressed) {
            addSuppressed(e, t);
        }

        sneakyThrow(e);
    }

    private static CompletableFuture<Void> throwExceptionWithSuppressedAsync(Throwable e, List<Throwable> suppressed) {
        for (Throwable t : suppressed) {
            addSuppressed(e, t);
        }

        return failedFuture(e);
    }

    private static boolean isRetriable(Throwable e) {
        return hasCause(e,
                TimeoutException.class,
                RetriableTransactionException.class
        );
    }

    private static boolean hasCause(Throwable e, Class<?>... classes) {
        Set<Throwable> processed = new HashSet<>();

        Throwable cause = e;
        while (cause != null) {
            if (!processed.add(cause)) {
                break;
            }

            for (Class<?> cls : classes) {
                if (cls.isAssignableFrom(cause.getClass())) {
                    return true;
                }
            }

            cause = cause.getCause();
        }

        return false;
    }

    private static long calcRemainingTime(long initialTimeout, long startTimestamp) {
        long now = System.currentTimeMillis();
        long remainingTime = initialTimeout - (now - startTimestamp);
        return remainingTime;
    }

    private static <E extends Throwable> E sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    private static class TxWithVal<T> {
        private final Transaction tx;
        private final T val;

        private TxWithVal(Transaction tx, T val) {
            this.tx = tx;
            this.val = val;
        }
    }
}
