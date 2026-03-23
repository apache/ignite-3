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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Set of test cases for query cancellation. */
public class ItCancelQueryTest extends BaseSqlIntegrationTest {
    /** Calling {@link CancelHandle#cancel()} should cancel execution of a single query. */
    @Test
    public void testCancelSingleQuery()  {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(Set.of(SqlQueryType.QUERY));

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

        AsyncSqlCursor<InternalSqlRow> query1 = await(qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ));

        assertEquals(1, qryProc.runningQueries().size());

        // Request cancellation.
        CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();

        // Obverse cancellation error
        expectQueryCancelled(new DrainCursor(query1));

        await(cancelled);

        waitUntilRunningQueriesCount(is(0));
    }

    /** Calling {@link CancelHandle#cancel()} should cancel execution of multiple queries. */
    @Test
    public void testCancelMultipleQueries()  {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(Set.of(SqlQueryType.QUERY));

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

        AsyncSqlCursor<InternalSqlRow> query1 = await(qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ));

        AsyncSqlCursor<InternalSqlRow> query2 = await(qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query.toString()
        ));

        assertEquals(2, qryProc.runningQueries().size());

        // Request cancellation.
        CompletableFuture<Void> cancelled = cancelHandle.cancelAsync();

        // Obverse cancellation errors
        expectQueryCancelled(new DrainCursor(query1));
        expectQueryCancelled(new DrainCursor(query2));

        await(cancelled);

        waitUntilRunningQueriesCount(is(0));
    }

    /** Starting a query with a cancelled token should trigger query cancellation. */
    @Test
    public void testQueryWontStartWhenHandleIsCancelled() {
        IgniteImpl igniteImpl = TestWrappers.unwrapIgniteImpl(CLUSTER.node(0));

        SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(Set.of(SqlQueryType.QUERY));

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();

        Executable run = () -> await(qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                "SELECT 1"
        ));

        expectQueryCancelled(run);

        waitUntilRunningQueriesCount(is(0));
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

        SqlProperties properties = new SqlProperties()
                .allowedQueryTypes(Set.of(SqlQueryType.QUERY, SqlQueryType.DML));

        HybridTimestampTracker hybridTimestampTracker = igniteImpl.observableTimeTracker();

        SqlQueryProcessor qryProc = queryProcessor();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        Executable run = () -> await(qryProc.queryAsync(
                properties,
                hybridTimestampTracker,
                null,
                token,
                query
        ));

        // Request cancellation
        CompletableFuture<Void> f = cancelHandle.cancelAsync();

        // Obverse cancellation error
        expectQueryCancelled(run);

        await(f);

        waitUntilRunningQueriesCount(is(0));
    }
}
