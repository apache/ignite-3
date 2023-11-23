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

package org.apache.ignite.internal.sql.engine.tx;

import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper over transaction, which is managed by SQL engine via {@link SqlQueryType#TX_CONTROL} statements.
 */
public class ScriptManagedTransactionWrapper implements QueryTransactionWrapper {
    private final CompletableFuture<Void> finishTxFuture = new CompletableFuture<>();
    private final AtomicInteger remainingCursors = new AtomicInteger(1);
    private final InternalTransaction transaction;
    private volatile boolean waitForTxFinish;

    ScriptManagedTransactionWrapper(InternalTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public CompletableFuture<Void> onCursorClose() {
        return onCursorCloseInternal(null);
    }

    @Override
    public InternalTransaction unwrap() {
        return transaction;
    }

    @Override
    public CompletableFuture<Void> commitImplicit() {
        return waitForTxFinish ? finishTxFuture : Commons.completedFuture();
    }

    @Override
    public CompletableFuture<Void> rollback(Throwable cause) {
        return onCursorCloseInternal(cause);
    }

    ScriptManagedTransactionWrapper forCommit() {
        onCursorCloseInternal(null);

        waitForTxFinish = true;

        return this;
    }

    private CompletableFuture<Void> onCursorCloseInternal(@Nullable Throwable cause) {
        if (cause != null) {
            return transaction.rollbackAsync()
                    .whenComplete((r, e) -> {
                        SqlException ex =
                                new SqlException(EXECUTION_CANCELLED_ERR, "Execution was canceled due to transaction rollback.", cause);

                        if (e != null) {
                            ex.addSuppressed(e);
                        }

                        finishTxFuture.completeExceptionally(ex);
                    });
        }

        if (remainingCursors.decrementAndGet() == 0 && !finishTxFuture.isDone()) {
            return transaction.commitAsync()
                    .whenComplete((r, e) -> finishTxFuture.complete(null));
        }

        return Commons.completedFuture();
    }

    boolean trackStatementCursor(SqlQueryType queryType) {
        if (queryType == SqlQueryType.QUERY && !transaction.isReadOnly()) {
            remainingCursors.incrementAndGet();

            return true;
        }

        return false;
    }
}
