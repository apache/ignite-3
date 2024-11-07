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

package org.apache.ignite.internal.sql.api;

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
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlCancellationToken;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Set of test cases for query cancellation. */
public class ItQueryCancelTest extends BaseSqlIntegrationTest {

    @Test
    public void testQueryCancel()  {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        QueryProcessor qryProc = queryProcessor();

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
        SqlCancellationToken token = new SqlCancellationToken(cancelHandle.token());

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

        // Request cancellation.
        CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();

        expectQueryCancelled(() -> query1.requestNextAsync(1).join());
        expectQueryCancelled(() -> query2.requestNextAsync(1).join());

        cancelled.join();
    }

    @Test
    public void testQueryWontStartWhenHandleIsCancelled() {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        QueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        SqlCancellationToken token = new SqlCancellationToken(cancelHandle.token());

        cancelHandle.cancel();

        Runnable run = () -> qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                "SELECT 1"
        ).join();

        expectQueryCancelled(run);
    }

    @Test
    public void testPrepareWontStartWhenHandleIsCancelled() {
        SqlProperties properties = SqlPropertiesHelper.newBuilder()
                .set(ALLOWED_QUERY_TYPES, Set.of(SqlQueryType.QUERY))
                .build();

        QueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        SqlCancellationToken token = new SqlCancellationToken(cancelHandle.token());

        cancelHandle.cancelAsync().join();

        Runnable run = () -> qryProc.prepareSingleAsync(
                properties,
                null,
                token,
                "SELECT 1"
        ).join();

        expectQueryCancelled(run);
    }

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

        QueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        SqlCancellationToken token = new SqlCancellationToken(cancelHandle.token());

        Runnable run = () -> qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query
        ).join();

        CompletableFuture<Void> f = cancelHandle.cancelAsync();

        expectQueryCancelled(run);

        f.join();
    }

    private static void expectQueryCancelled(Runnable action) {
        CompletionException err = assertThrows(CompletionException.class, action::run);
        SqlException sqlErr = assertInstanceOf(SqlException.class, err.getCause());
        assertEquals(Sql.EXECUTION_CANCELLED_ERR, sqlErr.code(), sqlErr.toString());
    }
}
