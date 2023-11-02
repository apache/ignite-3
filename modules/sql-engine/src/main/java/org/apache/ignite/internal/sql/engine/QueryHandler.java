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

package org.apache.ignite.internal.sql.engine;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.exec.ExecutionService;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.session.Session;
import org.apache.ignite.internal.sql.engine.util.BaseQueryContext;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * TODO Blah blah blah.
 */
abstract class QueryHandler<T> {
    private final SchemaSyncService schemaSyncService;
    private final SqlSchemaManager sqlSchemaManager;
    private final ExecutionService executionSrvc;

    /** Counter to keep track of the current number of live SQL cursors. */
    private final AtomicInteger numberOfOpenCursors;

    QueryHandler(
            SchemaSyncService schemaSyncService,
            SqlSchemaManager sqlSchemaManager,
            ExecutionService executionSrvc,
            AtomicInteger numberOfOpenCursors
    ) {
        this.schemaSyncService = schemaSyncService;
        this.sqlSchemaManager = sqlSchemaManager;
        this.numberOfOpenCursors = numberOfOpenCursors;
        this.executionSrvc = executionSrvc;
    }

    /**
     * TODO Blah blah blah.
     */
    abstract CompletableFuture<T> executeQuery(
            Session session,
            IgniteTransactions transactions,
            InternalTransaction outerTx,
            String sql,
            Set<SqlQueryType> allowedQueryTypes,
            QueryCancel queryCancel,
            Object... params
    );

    CompletableFuture<SchemaPlus> waitForActualSchema(String schemaName, HybridTimestamp timestamp) {
        try {
            return schemaSyncService.waitForMetadataCompleteness(timestamp).thenApply(unused -> {
                SchemaPlus schema = sqlSchemaManager.schema(timestamp.longValue()).getSubSchema(schemaName);

                if (schema == null) {
                    throw new SchemaNotFoundException(schemaName);
                }

                return schema;
            });
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    AsyncSqlCursor<List<Object>> executePlan(
            Session session,
            QueryTransactionWrapper txWrapper,
            BaseQueryContext ctx,
            QueryPlan plan
    ) {
        var dataCursor = executionSrvc.executePlan(txWrapper.unwrap(), plan, ctx);

        SqlQueryType queryType = plan.type();
        assert queryType != null : "Expected a full plan but got a fragment: " + plan;

        numberOfOpenCursors.incrementAndGet();

        return new AsyncSqlCursorImpl<>(
                queryType,
                plan.metadata(),
                txWrapper,
                new AsyncCursor<>() {
                    private final AtomicBoolean finished = new AtomicBoolean(false);

                    @Override
                    public CompletableFuture<BatchedResult<List<Object>>> requestNextAsync(int rows) {
                        session.touch();

                        return dataCursor.requestNextAsync(rows);
                    }

                    @Override
                    public CompletableFuture<Void> closeAsync() {
                        session.touch();

                        if (finished.compareAndSet(false, true)) {
                            numberOfOpenCursors.decrementAndGet();

                            return dataCursor.closeAsync();
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                }
        );
    }
}
