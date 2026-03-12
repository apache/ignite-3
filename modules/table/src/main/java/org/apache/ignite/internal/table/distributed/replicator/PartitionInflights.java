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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.TestOnly;

/**
 * Client transaction inflights tracker.
 */
public class PartitionInflights {
    private static final IgniteLogger LOG = Loggers.forClass(PartitionInflights.class);

    /** Hint for maximum concurrent txns. */
    private static final int MAX_CONCURRENT_TXNS_HINT = 1024;

    /** Txn contexts. */
    private final ConcurrentHashMap<UUID, TxContext> txCtxMap = new ConcurrentHashMap<>(MAX_CONCURRENT_TXNS_HINT);

    /**
     * Registers the inflight update for a transaction.
     *
     * @param txId The transaction id.
     */
    public boolean addInflight(UUID txId, Predicate<UUID> testPred) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new TxContext();
            }

            //ctx.opFuts.add(new IgniteBiTuple<>(new Exception(), fut));

            if (ctx.finishFut != null || testPred.test(txId)) {
                res[0] = false;
            } else {
                ctx.addInflight();
            }

            return ctx;
        });

        return res[0];
    }

    /**
     * Unregisters the inflight for a transaction.
     *
     * @param txId The transaction id.
     */
    public void removeInflight(UUID txId) {
        var ctx0 = txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                throw new AssertionError("Illegal call for removeInflight: " + txId);
            }

            ctx.removeInflight(txId);

            return ctx;
        });

        // Avoid completion under lock.
        if (ctx0.finishFut != null && ctx0.inflights == 0) {
            ctx0.finishFut.complete(null);
        }
    }

    /**
     * Get finish future.
     *
     * @param txId Transaction id.
     * @return The future.
     */
    public CompletableFuture<Void> finishFuture(UUID txId) {
        // No new operations can be enlisted an this point, so concurrent inflights counter can only go down.
        TxContext ctx0 = txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new TxContext();
            }

            // LOG.info("DBG: finishFuture " + txId + " " + ctx.inflights);

            if (ctx.finishFut == null) {
                ctx.finishFut = ctx.inflights == 0 ? nullCompletedFuture() : new CompletableFuture<>();
            }

            return ctx;
        });

//        if (ctx0 == null) {
//            return nullCompletedFuture();
//        }

        return ctx0.finishFut;
    }

    /**
     * Cleanup inflights context for this transaction.
     *
     * @param uuid Tx id.
     */
    public void erase(UUID uuid) {
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
     * Transaction inflights context.
     */
    public static class TxContext {
        public CompletableFuture<Void> finishFut;
        public volatile long inflights = 0;

        void addInflight() {
            inflights++;
        }

        void removeInflight(UUID txId) {
            assert inflights > 0 : format("No inflights, cannot remove any [txId={}, ctx={}]", txId, this);

            inflights--;
        }
    }

    @TestOnly
    public ConcurrentHashMap<UUID, TxContext> map() {
        return txCtxMap;
    }
}
