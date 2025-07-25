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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC connection context.
 */
class JdbcConnectionContext {
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Object mux = new Object();

    private final TxManager txManager;

    private final ZoneId timeZoneId;

    private final String userName;

    private final ConcurrentMap<Long, CancelHandle> cancelHandles = new ConcurrentHashMap<>();

    private @Nullable TxWithTimeTracker txWithTimeTracker;

    JdbcConnectionContext(
            TxManager txManager,
            ZoneId timeZoneId,
            String userName
    ) {
        this.txManager = txManager;
        this.timeZoneId = timeZoneId;
        this.userName = userName;
    }

    ZoneId timeZoneId() {
        return timeZoneId;
    }

    String userName() {
        return userName;
    }

    /**
     * Gets the transaction associated with the current connection, starts a new one if it doesn't already exist.
     *
     * <p>NOTE: this method is not thread-safe and should only be called by a single thread.
     *
     * @return Transaction associated with the current connection.
     */
    InternalTransaction getOrStartTransaction(HybridTimestampTracker timestampTracker) {
        if (txWithTimeTracker == null) {
            InternalTransaction tx = txManager.beginExplicitRw(timestampTracker, InternalTxOptions.defaults());

            txWithTimeTracker = new TxWithTimeTracker(tx, timestampTracker);
        }

        return txWithTimeTracker.transaction();
    }

    /**
     * Finishes active transaction, if one exists.
     *
     * <p>NOTE: this method is not thread-safe and should only be called by a single thread.
     *
     * @param commit {@code True} to commit, {@code false} to rollback.
     * @return Future that represents the pending completion of the operation.
     */
    CompletableFuture<@Nullable HybridTimestamp> finishTransactionAsync(boolean commit) {
        TxWithTimeTracker txWithTimeTracker0 = txWithTimeTracker;

        txWithTimeTracker = null;

        if (txWithTimeTracker0 == null) {
            return nullCompletedFuture();
        }

        return commit
                ? txWithTimeTracker0.transaction().commitAsync().thenApply(ignore -> txWithTimeTracker0.observableTimestamp())
                : txWithTimeTracker0.transaction().rollbackAsync().thenApply(ignore -> null);
    }

    boolean valid() {
        return !closed.get();
    }

    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        synchronized (mux) {
            finishTransactionAsync(false);
        }
    }

    CancellationToken registerExecution(long token) {
        CancelHandle handle = CancelHandle.create();

        CancelHandle previousHandle = cancelHandles.putIfAbsent(token, handle);

        assert previousHandle == null;

        return handle.token();
    }

    void deregisterExecution(long token) {
        cancelHandles.remove(token);
    }

    CompletableFuture<Void> cancelExecution(long token) {
        CancelHandle handle = cancelHandles.remove(token);

        if (handle == null) {
            return nullCompletedFuture();
        }

        return handle.cancelAsync();
    }

    private static class TxWithTimeTracker {
        private final InternalTransaction transaction;
        private final HybridTimestampTracker tracker;

        private TxWithTimeTracker(InternalTransaction transaction, HybridTimestampTracker tracker) {
            this.transaction = transaction;
            this.tracker = tracker;
        }

        InternalTransaction transaction() {
            return transaction;
        }

        @Nullable HybridTimestamp observableTimestamp() {
            return tracker.get();
        }
    }
}
