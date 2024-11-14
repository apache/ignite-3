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

import static org.apache.ignite.internal.sql.engine.QueryProperty.ALLOWED_QUERY_TYPES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Set of test cases for query cancellation. */
public class ItQueryCancelTest extends BaseSqlIntegrationTest {

    /** Calling {@link CancelHandle#cancel()} should cancel execution of a single query. */
    @Test
    public void testCancelSingleQuery()  {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        StringBuilder query = new StringBuilder();

        query.append("SELECT v FROM (VALUES ");
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                query.append(", ");
            }
            query.append("(");
            query.append(i);
            query.append(")");
        }
        query.append(" ) t(v)");

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AsyncSqlCursor<InternalSqlRow> query1 = qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ).join();

        assertEquals(1, qryProc.runningQueries());

        // Request cancellation.
        CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();

        // Obverse cancellation error
        expectQueryCancelled(new DrainCursor(query1));

        cancelled.join();

        assertEquals(0, qryProc.runningQueries());
    }

    /** Calling {@link CancelHandle#cancel()} should cancel execution of multiple queries. */
    @Test
    public void testCancelMultipleQueries()  {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        StringBuilder query = new StringBuilder();

        query.append("SELECT v FROM (VALUES ");
        for (int i = 0; i < 100; i++) {
            if (i > 0) {
                query.append(", ");
            }
            query.append("(");
            query.append(i);
            query.append(")");
        }
        query.append(" ) t(v)");

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AsyncSqlCursor<InternalSqlRow> query1 = qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ).join();

        AsyncSqlCursor<InternalSqlRow> query2 = qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ).join();

        assertEquals(2, qryProc.runningQueries());

        // Request cancellation.
        CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();

        // Obverse cancellation errors
        expectQueryCancelled(new DrainCursor(query1));
        expectQueryCancelled(new DrainCursor(query2));

        cancelled.join();

        assertEquals(0, qryProc.runningQueries());
    }

    /** Starting a query with a cancelled token should trigger query cancellation. */
    @Test
    public void testQueryWontStartWhenHandleIsCancelled() {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();

        Runnable run = () -> qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                "SELECT 1"
        ).join();

        expectQueryCancelledWithQueryCancelledException(run);

        assertEquals(0, qryProc.runningQueries());
    }

    /** Calling {@link CancelHandle#cancel()} should cancel execution of queries that use executable plans. */
    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT * FROM t WHERE id = 1",
            "INSERT INTO t VALUES (1, 1)",
            "SELECT COUNT(*) FROM t",
    })
    public void testExecutablePlans(String query) {
        sql("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, val INT)");

        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY, SqlQueryType.DML))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        Runnable run = () -> qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query
        ).join();

        // Request cancellation
        CompletableFuture<Void> f = cancelHandle.cancelAsync();

        // Obverse cancellation error
        expectQueryCancelledWithQueryCancelledException(run);

        f.join();

        assertEquals(0, qryProc.runningQueries());
    }

    private static void expectQueryCancelled(Runnable action) {
        CompletionException err = assertThrows(CompletionException.class, action::run);
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code(), sqlErr.toString());
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-23637 remove this method and use expectQueryCancelled instead.
    private static void expectQueryCancelledWithQueryCancelledException(Runnable action) {
        CompletionException err = assertThrows(CompletionException.class, action::run);
        assertInstanceOf(QueryCancelledException.class, err.getCause());
    }

    private static class DrainCursor implements Runnable {
        final AsyncSqlCursor<?> cursor;

        DrainCursor(AsyncSqlCursor<?> cursor) {
            this.cursor = cursor;
        }

        @Override
        public void run() {
            BatchedResult<?> batch;
            do {
                batch = cursor.requestNextAsync(1).join();
            } while (!batch.items().isEmpty());
        }
    }
}
