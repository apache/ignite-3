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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a query initiated on current node.
 *
 * <p>Encapsulates intermediate state populated throughout query lifecycle.
 */
class Query {
    private static final IgniteLogger LOG = Loggers.forClass(Query.class);
    private static final int MAX_ATTEMPTS_COUNT = 1024;

    final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    final AtomicReference<@Nullable ProgramExecutionHandle> activeProgram = new AtomicReference<>();

    // Below are attributes the query was initialized with
    final Instant createdAt;
    final @Nullable UUID parentId;
    final int statementNum;
    final UUID id;
    final String sql;
    final Object[] params;
    final QueryCancel cancel = new QueryCancel();
    final QueryExecutor executor;
    final SqlProperties properties;
    final QueryTransactionContext txContext;
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final @Nullable CompletableFuture<AsyncSqlCursor<InternalSqlRow>> nextCursorFuture;

    // Below is volatile state populated during processing of particular stage for single statement execution
    volatile @Nullable ParsedResult parsedResult = null;
    volatile @Nullable SqlOperationContext operationContext = null;
    volatile @Nullable QueryPlan plan = null;
    volatile @Nullable QueryTransactionWrapper usedTransaction = null;
    volatile @Nullable AsyncSqlCursor<InternalSqlRow> cursor = null;

    // Below is volatile state for script processing
    volatile @Nullable List<ParsedResult> parsedScript = null;

    // Below are internal attributes
    private volatile ExecutionPhase currentPhase = ExecutionPhase.REGISTERED;

    /** Constructs the query. */
    Query(
            Instant createdAt,
            QueryExecutor executor,
            UUID id,
            String sql,
            SqlProperties properties,
            QueryTransactionContext txContext,
            Object[] params
    ) {
        this.createdAt = createdAt;
        this.executor = executor;
        this.id = id;
        this.sql = sql;
        this.properties = properties;
        this.txContext = txContext;
        this.params = params;

        this.parentId = null;
        this.statementNum = -1;
        this.nextCursorFuture = null;
    }

    /** Constructs the child query. */
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
        this.params = params;
        this.nextCursorFuture = nextCursorFuture;
        this.parsedResult = parsedResult;
    }

    <ResultT> CompletableFuture<ResultT> runProgram(Program<ResultT> program) {
        ProgramExecutionState<ResultT> state = program.createState();

        ProgramExecutionHandle currentProgram = activeProgram.compareAndExchange(null, state);
        if (currentProgram != null) {
            String message = format(
                    "Attempt to run query program while another is still active [runningProgram={}, newProgram={}].",
                    currentProgram, program
            );
            throw new SqlException(Common.INTERNAL_ERR, message);
        }

        program.run(this, state);

        return state.resultHolder;
    }

    /**
     * Initiates a graceful termination of the current query.
     *
     * <p>If the query is idle, moves it to {@link ExecutionPhase#TERMINATED} phase immediately. Otherwise, the method waits for the
     * currently active program to complete before proceeding. Note that termination may not take effect immediately upon return. To wait
     * for the actual completion of termination, use {@link #terminationFuture}.
     */
    void terminate() {
        tryTerminate(1);
    }

    private void tryTerminate(int attemptNo) {
        if (attemptNo >= MAX_ATTEMPTS_COUNT) {
            // Exception thrown from this place most probably will be swallowed,
            // therefore logging is chosen.
            LOG.warn("Unable to terminate query after several attempts. Try to cancel it explicitly using KILL statement," 
                    + "or restart the node as some resources mays still be held by this query [queryId={}, attempts={}].", id, attemptNo);

            return;
        }

        // Already terminated, nothing to do.
        if (currentPhase == ExecutionPhase.TERMINATED) {
            return;
        }

        // Optimistically assume there is no active program, just try to run TERMINATION program.
        if (tryRunTerminationProgram()) {
            return;
        }

        ProgramExecutionHandle handle = activeProgram.get();

        // Handle may be null if active program completes a moment after we tried to run TERMINATION program.
        if (handle != null) {
            // If active program still exists, wait for completion and try again.
            handle.completionFuture()
                    .whenComplete((r, e) -> tryTerminate(attemptNo + 1));

            return;
        }

        // Active program competed concurrently, just try again.
        tryTerminate(attemptNo + 1);
    }

    /** Moves the query to a given state. */
    void moveTo(ExecutionPhase newPhase) {
        currentPhase = newPhase;

        if (newPhase == ExecutionPhase.TERMINATED) {
            terminationFuture.complete(null);
        }
    }

    ExecutionPhase currentPhase() {
        return currentPhase;
    }

    void terminateExceptionally(Throwable th) {
        setError(th);

        ProgramExecutionHandle handle = activeProgram.get();
        if (handle != null) {
            handle.notifyError(th);
        }

        terminate();
    }

    private boolean tryRunTerminationProgram() {
        // Create fake state to prevent concurrent program execution.
        ProgramExecutionState<Void> state = new ProgramExecutionState<>("QUERY_TERMINATION");

        if (activeProgram.compareAndSet(null, state)) {
            moveTo(ExecutionPhase.TERMINATED);

            activeProgram.set(null);

            return true;
        }

        return false;
    }

    void setError(Throwable err) {
        Throwable prevErr = error.compareAndExchange(null, err);

        if (prevErr != null && prevErr != err) {
            error.get().addSuppressed(err);
        }
    }

    /**
     * Cancels this query.
     *
     * @return Future that completes when the query is cancelled and is removed from the running queries registry.
     */
    CompletableFuture<Void> cancel() {
        cancel.cancel();

        return terminationFuture;
    }

    void reset() {
        error.set(null);
    }
}
