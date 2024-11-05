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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.fsm.Result.Status;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a query initiated on current node.
 * 
 * <p>Encapsulates intermediate state populated throughout query lifecycle.
 */
class Query implements Runnable {
    // Below are attributes the query was initialized with
    final Instant createdAt;
    final @Nullable UUID parentId;
    final int statementNum;
    final UUID id;
    final String sql;
    final Object[] params;
    final QueryCancel cancel;
    final QueryExecutor executor;
    final SqlProperties properties;
    final QueryTransactionContext txContext;
    final @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture;

    // Below is volatile state populated during processing of particular stage for single statement execution
    volatile @Nullable ParsedResult parsedResult = null;
    volatile @Nullable SqlOperationContext operationContext = null;
    volatile @Nullable QueryPlan plan = null;
    volatile @Nullable InternalTransaction usedTransaction = null;
    volatile @Nullable AsyncSqlCursor<InternalSqlRow> cursor = null;

    // Below is volatile state for script processing
    volatile @Nullable List<ParsedResult> parsedScript = null;

    // Below are internal attributes
    private final ConcurrentMap<ExecutionPhase, CompletableFuture<Void>> onPhaseStartedCallback = new ConcurrentHashMap<>();
    private final Object mux = new Object();

    private volatile ExecutionPhase currentPhase = ExecutionPhase.REGISTERED;

    Query(
            Instant createdAt,
            QueryExecutor executor,
            UUID id,
            String sql,
            SqlProperties properties,
            QueryTransactionContext txContext,
            @Nullable CancellationToken cancellationToken,
            Object[] params,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture
    ) {
        this.createdAt = createdAt;
        this.executor = executor;
        this.id = id;
        this.sql = sql;
        this.properties = properties;
        this.txContext = txContext;
        this.cancel = new QueryCancel(cancellationToken);
        this.params = params;
        this.nextCursorFuture = nextCursorFuture;

        this.parentId = null;
        this.statementNum = -1;
    }

    Query(
            Instant createdAt,
            Query parent,
            ParsedResult parsedResult,
            int statementNum,
            UUID id,
            QueryTransactionContext txContext,
            Object[] params,
            @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture
    ) {
        this.createdAt = createdAt;
        this.executor = parent.executor;
        this.parentId = parent.id;
        this.statementNum = statementNum;
        this.id = id;
        this.sql = parsedResult.originalQuery();
        this.properties = parent.properties;
        this.txContext = txContext;
        this.cancel = new QueryCancel();
        this.params = params;
        this.nextCursorFuture = nextCursorFuture;

        this.parsedResult = parsedResult;
    }

    @Override
    public void run() {
        Result result;
        do {
            ExecutionPhase phaseBefore = currentPhase;

            try {
                result = phaseBefore.evaluate(this);
            } catch (Throwable th) {
                // handles exception from synchronous part of phase evaluation

                onError(th);

                return;
            }

            if (IgniteUtils.assertionsEnabled()
                    && result.status() == Result.Status.PROCEED_IMMEDIATELY
                    && currentPhase == phaseBefore) {
                throw new AssertionError("Attempt to immediately execute the same state. Did you forget to move query to next state?");
            }

            if (result.status() == Status.STOP) {
                break;
            }

            if (result.status() == Status.SCHEDULE) {
                CompletableFuture<Void> awaitFuture = result.await();

                assert awaitFuture != null;

                // reschedule only if required computation has not been done yet or it was completed exceptionally
                if (!awaitFuture.isDone() || awaitFuture.isCompletedExceptionally()) {
                    awaitFuture
                            .whenComplete((ignored, ex) -> {
                                if (ex != null) {
                                    // handles exception from asynchronous part of phase evaluation
                                    onError(ex);
                                }
                            })
                            .thenRunAsync(this, executor::execute);

                    return;
                }
            }
        } while (true);
    }

    CompletableFuture<Void> onPhaseStarted(ExecutionPhase phase) {
        return onPhaseStartedCallback.computeIfAbsent(phase, k -> new CompletableFuture<>());
    }

    /** Moves the query to a given state. */ 
    void moveTo(ExecutionPhase newPhase) {
        synchronized (mux) {
            assert currentPhase.transitionAllowed(newPhase) : "currentPhase=" + currentPhase + ", newPhase=" + newPhase;

            currentPhase = newPhase;
        }

        onPhaseStartedCallback.computeIfAbsent(newPhase, k -> new CompletableFuture<>()).complete(null);
    }

    ExecutionPhase currentPhase() {
        return currentPhase;
    }

    void onError(Throwable th) {
        onPhaseStartedCallback.values().forEach(f -> f.completeExceptionally(th));

        moveTo(ExecutionPhase.TERMINATED);
    }
}
