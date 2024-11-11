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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor.PrefetchCallback;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.AsyncDataCursor;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;

/** Handler that acquires data cursor and saves it to {@link Query query state}. */
class CursorInitializationPhaseHandler implements ExecutionPhaseHandler {
    static final ExecutionPhaseHandler INSTANCE = new CursorInitializationPhaseHandler();

    private CursorInitializationPhaseHandler() { }

    @Override
    public Result handle(Query query) {
        QueryPlan plan = query.plan;
        SqlOperationContext context = query.operationContext;

        assert plan != null;
        assert context != null;

        AsyncDataCursor<InternalSqlRow> dataCursor = query.executor.executePlan(context, plan);

        SqlQueryType queryType = plan.type();

        PrefetchCallback prefetchCallback = context.prefetchCallback();

        assert prefetchCallback != null;

        AsyncSqlCursorImpl<InternalSqlRow> cursor = new AsyncSqlCursorImpl<>(
                queryType,
                plan.metadata(),
                dataCursor,
                query.nextCursorFuture
        );

        query.cursor = cursor;

        QueryTransactionContext txContext = query.txContext;

        assert txContext != null;

        if (queryType == SqlQueryType.QUERY) {
            query.moveTo(ExecutionPhase.EXECUTING);

            // Postpone scheduling for TERMINATED phase to not to miss EXECUTING phase,
            // because cursor may be already closed at this point.
            cursor.onClose()
                    .whenComplete((ignored, ex) -> query.moveTo(ExecutionPhase.TERMINATED));

            // preserve lazy execution for statements that only reads
            return Result.proceedImmediately();
        }

        // for other types let's wait for the first page to make sure premature
        // close of the cursor won't cancel an entire operation
        CompletableFuture<Void> awaitFuture = cursor.onFirstPageReady()
                .thenApply(none -> {
                    query.moveTo(ExecutionPhase.EXECUTING);

                    // Postpone scheduling for TERMINATED phase to not to miss EXECUTING phase,
                    // because cursor may be already closed at this point.
                    cursor.onClose()
                            .whenComplete((ignored, ex) -> query.moveTo(ExecutionPhase.TERMINATED));

                    return null;
                });

        return Result.proceedAfter(awaitFuture);
    }
}
