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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_CLIENT_GETALL_SUPPORTS_PRIORITY;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Transactions.ACQUIRE_LOCK_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.table.ClientTable.Batch;
import org.apache.ignite.internal.client.table.ClientTable.Reducer;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides batch map utility methods.
 */
class ClientTableMapUtils {
    private static final long DEFAULT_IMPLICIT_GET_ALL_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(5000);

    static <R, E> void mapAndRetry(
            MapFunction<E, R> mapFun,
            @Nullable R initialValue, Reducer<R> reducer,
            List<Transaction> txns,
            Map<Integer, List<E>> mapped,
            long[] startTs,
            CompletableFuture<R> resFut,
            IgniteLogger log
    ) {
        if (startTs[0] == 0) {
            startTs[0] = System.nanoTime();
        }

        List<CompletableFuture<R>> res = new ArrayList<>();

        for (Entry<Integer, List<E>> entry : mapped.entrySet()) {
            res.add(mapFun.apply(entry.getValue(), PartitionAwarenessProvider.of(entry.getKey()), mapped.size() > 1));
        }

        CompletableFutures.allOf(res).handle((ignored, err) -> {
            List<CompletableFuture<Void>> waitCommitFuts = List.of();
            if (!txns.isEmpty()) {
                if (err != null) {
                    boolean needRetry = rollbackIfNeeded(txns, res, log);

                    long nowRelative = System.nanoTime();
                    if (needRetry && nowRelative - startTs[0] < DEFAULT_IMPLICIT_GET_ALL_TIMEOUT_NANOS) {
                        startTs[0] = nowRelative;
                        txns.clear(); // The collection is re-filled on next map attempt.

                        mapAndRetry(mapFun, initialValue, reducer, txns, mapped, startTs, resFut, log);

                        return null;
                    }

                    resFut.completeExceptionally(err);

                    return null;
                }

                waitCommitFuts = unlockFragments(txns, log);
            } else {
                if (err != null) {
                    resFut.completeExceptionally(err);

                    return null;
                }
            }

            R in = initialValue;

            for (CompletableFuture<R> val : res) {
                in = reducer.reduce(in, val.getNow(null));
            }

            if (waitCommitFuts.isEmpty()) {
                resFut.complete(in);
            } else {
                R finalIn = in;
                CompletableFutures.allOf(waitCommitFuts).whenComplete((r, e) -> {
                    // Ignore errors.
                    resFut.complete(finalIn);
                });
            }

            return null;
        });
    }

    static <E> void mapAndRetry(
            MapFunction<E, List<E>> mapFun,
            Collection<E> keys,
            List<Transaction> txns,
            Map<Integer, Batch<E>> mapped,
            long[] startTs,
            CompletableFuture<List<E>> resFut,
            IgniteLogger log
    ) {
        if (startTs[0] == 0) {
            startTs[0] = System.nanoTime();
        }

        List<CompletableFuture<List<E>>> res = new ArrayList<>(mapped.size());
        List<Batch<E>> batches = new ArrayList<>(mapped.size());

        for (Entry<Integer, Batch<E>> entry : mapped.entrySet()) {
            res.add(mapFun.apply(entry.getValue().batch, PartitionAwarenessProvider.of(entry.getKey()), mapped.size() > 1));
            batches.add(entry.getValue());
        }

        CompletableFutures.allOf(res).handle((ignored, err) -> {
            List<CompletableFuture<Void>> waitCommitFuts = List.of();
            if (!txns.isEmpty()) {
                if (err != null) {
                    boolean needRetry = rollbackIfNeeded(txns, res, log);

                    long nowRelative = System.nanoTime();
                    if (needRetry && nowRelative - startTs[0] < DEFAULT_IMPLICIT_GET_ALL_TIMEOUT_NANOS) {
                        startTs[0] = nowRelative;
                        txns.clear(); // The collection is re-filled on next map attempt.

                        mapAndRetry(mapFun, keys, txns, mapped, startTs, resFut, log);

                        return null;
                    }

                    resFut.completeExceptionally(err);

                    return null;
                }

                waitCommitFuts = unlockFragments(txns, log);
            } else {
                if (err != null) {
                    resFut.completeExceptionally(err);

                    return null;
                }
            }

            var in = new ArrayList<E>(Collections.nCopies(keys.size(), null));

            for (int i = 0; i < res.size(); i++) {
                CompletableFuture<List<E>> f = res.get(i);
                reduceWithKeepOrder(in, f.getNow(null), batches.get(i).originalIndices);
            }

            if (waitCommitFuts.isEmpty()) {
                resFut.complete(in);
            } else {
                CompletableFutures.allOf(waitCommitFuts).whenComplete((r, e) -> {
                    // Ignore errors.
                    resFut.complete(in);
                });
            }

            return null;
        });
    }

    private static <E> boolean rollbackIfNeeded(
            List<Transaction> txns,
            List<CompletableFuture<E>> res,
            IgniteLogger log
    ) {
        boolean allRetryableExceptions = true;
        for (int i = 0; i < res.size(); i++) {
            CompletableFuture<?> fut0 = res.get(i);
            if (fut0.isCompletedExceptionally()) {
                try {
                    fut0.join();
                } catch (CompletionException e) {
                    allRetryableExceptions = ExceptionUtils.matchAny(unwrapCause(e), ACQUIRE_LOCK_ERR);
                }
            }
            Transaction tx0 = txns.get(i);
            tx0.rollbackAsync().whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Failed to rollback a transactional batch: [tx=" + tx0 + ']', e);
                }
            });
        }

        return allRetryableExceptions;
    }

    @NotNull
    private static List<CompletableFuture<Void>> unlockFragments(List<Transaction> txns, IgniteLogger log) {
        List<CompletableFuture<Void>> waitCommitFuts = new ArrayList<>();

        for (Transaction txn : txns) {
            ClientLazyTransaction tx0 = (ClientLazyTransaction) txn;
            CompletableFuture<Void> fut = tx0.commitAsync().whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Failed to commit a transactional batch: [tx=" + tx0 + ']', e);
                }
            });
            // Enforce sync commit to avoid lock conflicts then working in compatibility mode.
            if (!tx0.startedTx().channel().protocolContext().isFeatureSupported(TX_CLIENT_GETALL_SUPPORTS_PRIORITY)) {
                waitCommitFuts.add(fut);
            }
        }

        return waitCommitFuts;
    }

    private static <E> void reduceWithKeepOrder(List<E> agg, List<E> cur, List<Integer> originalIndices) {
        for (int i = 0; i < cur.size(); i++) {
            E val = cur.get(i);
            Integer orig = originalIndices.get(i);
            agg.set(orig, val);
        }
    }
}
