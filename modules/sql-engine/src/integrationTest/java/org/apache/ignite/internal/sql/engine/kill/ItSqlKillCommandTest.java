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

package org.apache.ignite.internal.sql.engine.kill;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for SQL '{@code KILL}' statement.
 */
public class ItSqlKillCommandTest extends BaseSqlIntegrationTest {
    @AfterEach
    public void checkResourceLeak() {
        assertThat(txManager().pending(), is(0));
        assertThat(queryProcessor().openedCursors(), is(0));
    }

    @Test
    public void killQueryMetadata() {
        assertQuery("KILL QUERY '00000000-0000-0000-0000-000000000000'")
                .columnMetadata(
                        new MetadataMatcher().name("APPLIED").type(ColumnType.BOOLEAN).nullable(false)
                )
                .check();
    }

    @Test
    public void killWithInvalidQueryIdentifier() {
        SqlException err = assertThrowsSqlException(
                SqlException.class,
                Sql.RUNTIME_ERR,
                "Invalid operation ID format [operationId=123, type=QUERY]",
                () -> await(igniteSql().executeAsync(null, "KILL QUERY '123'"))
        );

        assertThat(err.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(err.getCause().getMessage(), equalTo("Invalid UUID string: 123"));
    }

    @Test
    public void killNonExistentQuery() {
        checkKillQuery(CLUSTER.aliveNode(), UUID.randomUUID(), false);
    }

    @Test
    public void killQueryFromLocal() {
        Ignite node = CLUSTER.aliveNode();

        AsyncSqlCursor<InternalSqlRow> cursor = executeQueryInternal(node, "SELECT 1");

        List<QueryInfo> queries = runningQueries();
        assertThat(queries.size(), is(1));
        UUID targetQueryId = queries.get(0).id();

        checkKillQuery(node, targetQueryId, true);

        assertThat(runningQueries(), is(empty()));
        expectQueryCancelled(new DrainCursor(cursor));

        checkKillQuery(node, targetQueryId, false);
        checkKillQuery(node, targetQueryId, true, true);
    }

    @Test
    public void killQueryFromRemote() {
        Ignite local = CLUSTER.node(0);
        Ignite remote = CLUSTER.node(2);

        AsyncSqlCursor<InternalSqlRow> cursor = executeQueryInternal(local, "SELECT 1");

        List<QueryInfo> queries = runningQueries();
        assertThat(queries.size(), is(1));
        UUID targetQueryId = queries.get(0).id();

        checkKillQuery(remote, targetQueryId, true);

        assertThat(runningQueries(), is(empty()));
        expectQueryCancelled(new DrainCursor(cursor));

        checkKillQuery(remote, targetQueryId, false);
        checkKillQuery(local, targetQueryId, false);
    }

    private static void checkKillQuery(Ignite node, UUID queryId, boolean expectedResult) {
        checkKillQuery(node, queryId, expectedResult, false);
    }

    private static void checkKillQuery(Ignite node, UUID queryId, boolean expectedResult, boolean noWait) {
        String query = IgniteStringFormatter
                .format("KILL QUERY '{}'{}", queryId, noWait ? " NO WAIT" : "");

        try (ResultSet<SqlRow> res = node.sql().execute(null, query)) {
            assertThat(res.hasRowSet(), is(false));
            assertThat(res.wasApplied(), is(expectedResult));
        }
    }

    private static List<QueryInfo> runningQueries() {
        return CLUSTER.runningNodes()
                .flatMap(node -> ((SqlQueryProcessor) unwrapIgniteImpl(node).queryEngine()).runningQueries().stream())
                .collect(Collectors.toList());
    }

    private static AsyncSqlCursor<InternalSqlRow> executeQueryInternal(Ignite node, String query) {
        IgniteImpl ignite = unwrapIgniteImpl(node);

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut = ignite.queryEngine().queryAsync(
                SqlQueryProcessor.DEFAULT_PROPERTIES,
                ignite.observableTimeTracker(),
                null,
                null,
                query
        );

        return await(fut);
    }
}
