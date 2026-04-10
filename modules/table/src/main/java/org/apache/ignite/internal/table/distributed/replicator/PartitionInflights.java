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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Predicate;
import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The class is responsible to track partition enlistment operations in a thread safe way.
 * Its main purpose is to ensure absence of data races in case of concurrent transaction rollback and partition enlistment operation.
 *
 * <p>Partition operations register itself using {@link #addInflight(UUID, Predicate, RequestType)} method.
 *
 * <p>Before transaction cleanup {@link #lockForCleanup(UUID)} is called, which prevents enlistment of new operations and ensures all
 * current operations are completed.
 */
public class PartitionInflights {
    /** Hint for maximum concurrent txns. */
    private static final int MAX_CONCURRENT_TXNS_HINT = 1024;

    /** Field updater for inflights. */
    private static final AtomicLongFieldUpdater<CleanupContext> INFLIGHTS_UPDATER = newUpdater(CleanupContext.class, "inflights");

    /** Txn contexts. */
    private final ConcurrentHashMap<UUID, CleanupContext> txCtxMap = new ConcurrentHashMap<>(MAX_CONCURRENT_TXNS_HINT);

    /**
     * Registers the inflight for a transaction.
     *
     * @param txId The transaction id.
     * @param enlistPred A predicate to test enlistment possibility under a transaction lock.
     * @param requestType Request type.
     *
     * @return Cleanup context.
     */
    @Nullable CleanupContext addInflight(UUID txId, Predicate<UUID> enlistPred, RequestType requestType) {
        boolean[] res = {true};

        CleanupContext ctx0 = txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new CleanupContext();
            }

            if (ctx.finishFut != null || enlistPred.test(txId)) {
                res[0] = false;
            } else {
                INFLIGHTS_UPDATER.incrementAndGet(ctx);
                if (requestType.isWrite()) {
                    ctx.hasWrites = true;
                }
            }

            return ctx;
        });

        return res[0] ? ctx0 : null;
    }

    /**
     * Runs a closure under a transaction lock.
     *
     * @param txId Transaction id.
     * @param r Runnable.
     */
    void runClosure(UUID txId, Runnable r) {
        txCtxMap.compute(txId, (uuid, ctx) -> {
            r.run();

            return ctx;
        });
    }

    /**
     * Unregisters the inflight for a transaction.
     *
     * @param ctx Cleanup context.
     */
    static void removeInflight(CleanupContext ctx) {
        long val = INFLIGHTS_UPDATER.decrementAndGet(ctx);

        if (ctx.finishFut != null && val == 0) {
            // If finishFut is null, counter can only go down.
            ctx.finishFut.complete(null);
        }
    }

    /**
     * Locks a transaction for cleanup. This prevents new enlistments into the transaction.
     *
     * @param txId Transaction id.
     * @return The context.
     */
    @Nullable CleanupContext lockForCleanup(UUID txId) {
        return txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                return null;
            }

            if (ctx.finishFut == null) {
                ctx.finishFut = INFLIGHTS_UPDATER.get(ctx) == 0 ? nullCompletedFuture() : new CompletableFuture<>();

                // Avoiding a data race with a concurrent decrementing thread, which might not see finishFut publication.
                if (INFLIGHTS_UPDATER.get(ctx) == 0 && !ctx.finishFut.isDone()) {
                    ctx.finishFut = nullCompletedFuture();
                }
            }

            return ctx;
        });
    }

    /**
     * Cleanup inflights context for this transaction.
     *
     * @param uuid Tx id.
     */
    void erase(UUID uuid) {
        txCtxMap.remove(uuid);
    }

    /**
     * Check if the inflights map contains a given transaction.
     *
     * @param txId Tx id.
     * @return {@code True} if contains.
     */
    public boolean contains(UUID txId) {
        return txCtxMap.containsKey(txId);
    }

    /**
     * Shared cleanup context.
     */
    public static class CleanupContext {
        /** An enlistment guard. Not null value means enlistments are not allowed any more. */
        volatile CompletableFuture<Void> finishFut;
        /** Inflights counter. */
        volatile long inflights = 0;
        /** Flag to test if a transaction has writes. If no writes, cleanup message will be skipped. */
        volatile boolean hasWrites = false;
    }

    @TestOnly
    public ConcurrentHashMap<UUID, CleanupContext> map() {
        return txCtxMap;
    }
}
