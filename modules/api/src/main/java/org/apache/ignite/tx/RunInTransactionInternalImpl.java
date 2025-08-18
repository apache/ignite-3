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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

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

        while (true) {
            Transaction tx = igniteTransactions.begin(txOptions);

            try {
                T ret = clo.apply(tx);

                tx.commit();

                return ret;
            } catch (Throwable t) {
                suppressed.add(t);

                try {
                    tx.rollback(); // Try rolling back on user exception.
                } catch (Exception e) {
                    suppressed.add(e);
                }

                long now = System.currentTimeMillis();
                long remainingTime = initialTimeout - (now - startTimestamp);

                if (remainingTime > 0) {
                    // Will go on retry iteration.
                    txOptions = txOptions.timeoutMillis(remainingTime);
                } else {
                    for (Throwable e : suppressed) {
                        addSuppressed(t, e);
                    }

                    throw t;
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

        return igniteTransactions.beginAsync(txOptions).thenCompose(tx -> {
            try {
                return clo.apply(tx)
                        .thenCompose(
                                val -> tx.commitAsync()
                                        .thenApply(ignored -> val)
                        )
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
                        .thenCompose(identity());
            } catch (Exception e) {
                return handleClosureException(igniteTransactions, tx, clo, txOptions, startTimestamp, initialTimeout, sup, e);
            }
        });
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
        return currentTx.rollbackAsync()
                .exceptionally(e0 -> {
                    suppressed.add(e0);
                    return null;
                })
                .thenCompose(ignored -> {
                    long remainingTime = initialTimeout - (System.currentTimeMillis() - startTimestamp);

                    if (remainingTime > 0) {
                        TransactionOptions opt = txOptions.timeoutMillis(remainingTime);

                        return runInTransactionAsyncInternal(
                                igniteTransactions,
                                clo,
                                opt,
                                startTimestamp,
                                initialTimeout,
                                suppressed
                        );
                    } else {
                        for (Throwable t : suppressed) {
                            addSuppressed(e, t);
                        }

                        return failedFuture(e);
                    }
                });
    }

    private static void addSuppressed(Throwable to, Throwable a) {
        if (to != null && a != null && to != a && to.getSuppressed().length < MAX_SUPPRESSED) {
            to.addSuppressed(a);
        }
    }
}
