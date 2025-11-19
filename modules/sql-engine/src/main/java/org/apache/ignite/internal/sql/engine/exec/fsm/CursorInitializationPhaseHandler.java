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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;

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

        SqlQueryType queryType = plan.type();

        query.cancel.throwIfCancelled();

        CompletableFuture<Void> awaitFuture = query.executor.executePlan(context, plan)
                .thenCompose(dataCursor -> {
                    AsyncSqlCursor<InternalSqlRow> cursor;
                    try {
                        cursor = query.executor.createAndSaveSqlCursor(query, dataCursor);
                    } catch (QueryCancelledException ignored) {
                        // Cancellation should be triggered inside of `createAndSaveSqlCursor`.
                        return dataCursor.onClose();
                    }

                    if (queryType == SqlQueryType.QUERY) {
                        // preserve lazy execution for statements that only reads
                        return nullCompletedFuture();
                    }

                    // for other types let's wait for the first page to make sure premature
                    // close of the cursor won't cancel an entire operation
                    return cursor.onFirstPageReady();
                });

        return Result.proceedAfter(awaitFuture);
    }
}
