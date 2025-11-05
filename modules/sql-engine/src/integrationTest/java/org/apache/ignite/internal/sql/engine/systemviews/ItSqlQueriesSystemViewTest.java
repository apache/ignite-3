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

package org.apache.ignite.internal.sql.engine.systemviews;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.sql.engine.SqlQueriesViewProvider.SCRIPT_QUERY_TYPE;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasLength;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to verify {@code SQL_QUERIES} system view.
 */
public class ItSqlQueriesSystemViewTest extends AbstractSystemViewTest {
    @BeforeAll
    void beforeAll() {

        sql("CREATE TABLE test(id INT PRIMARY KEY)");
    }

    @AfterEach
    void cleanup() {
        sql("DELETE FROM test");

        checkNoPendingQueries();
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    public void testMetadata() {
        assertQuery("SELECT * FROM SYSTEM.SQL_QUERIES")
                .columnMetadata(
                        new MetadataMatcher().name("INITIATOR_NODE").type(ColumnType.STRING).nullable(false),
                        new MetadataMatcher().name("QUERY_ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("QUERY_PHASE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_DEFAULT_SCHEMA").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("SQL").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("QUERY_START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                        new MetadataMatcher().name("TRANSACTION_ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("PARENT_QUERY_ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("QUERY_STATEMENT_ORDINAL").type(ColumnType.INT32).nullable(true),

                        // Legacy columns.
                        new MetadataMatcher().name("ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("PHASE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TYPE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("SCHEMA").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("START_TIME").type(ColumnType.TIMESTAMP).nullable(true),
                        new MetadataMatcher().name("PARENT_ID").type(ColumnType.STRING).precision(36).nullable(true),
                        new MetadataMatcher().name("STATEMENT_NUM").type(ColumnType.INT32).nullable(true)
                )
                .returnRowCount(1)
                .check();
    }

    @Test
    public void singleStatement() {
        String query = "SELECT * FROM SQL_QUERIES ORDER BY START_TIME";

        Ignite initiator = CLUSTER.aliveNode();

        ClockService clockService = unwrapIgniteImpl(initiator).clockService();

        String schema = "SYSTEM";

        // Test with explicit tx.
        InternalTransaction tx = (InternalTransaction) initiator.transactions().begin();

        checkNoPendingQueries();

        try {
            long tsBefore = clockService.now().getPhysical();

            List<List<Object>> res = sql(initiator, tx, schema, null, query);

            long tsAfter = clockService.now().getPhysical();

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), schema, query, tsBefore, tsAfter,
                    equalTo(tx.id().toString()), SqlQueryType.QUERY.name(), null);
        } finally {
            tx.rollback();
        }

        checkNoPendingQueries();

        // Implicit tx.
        {
            long tsBefore = clockService.now().getPhysical();

            List<List<Object>> res = sql(initiator, null, schema, null, query);

            long tsAfter = clockService.now().getPhysical();

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), schema, query, tsBefore, tsAfter,
                    hasLength(36), SqlQueryType.QUERY.name(), null);
        }
    }

    @Test
    public void multiStatement() {
        Ignite initiator = CLUSTER.node(0);
        ClockService clockService = unwrapIgniteImpl(initiator).clockService();

        String queryText = "SELECT x FROM TABLE(SYSTEM_RANGE(0, 2));"
                + "INSERT INTO test VALUES (0), (1);"
                + "SELECT x FROM TABLE(SYSTEM_RANGE(3, 5));";

        long timeBefore = clockService.now().getPhysical();
        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(runScript(queryText));
        long timeAfter = clockService.now().getPhysical();

        assertThat(cursors, hasSize(3));
        assertThat(queryProcessor().runningQueries().size(), is(4));

        // Verify script query info.
        {
            String sql = "SELECT * FROM SYSTEM.SQL_QUERIES WHERE TYPE=?";
            List<List<Object>> res = sql(initiator, null, null, null, sql, SCRIPT_QUERY_TYPE);

            assertThat(res, hasSize(1));

            verifyQueryInfo(res.get(0), initiator.name(), SqlCommon.DEFAULT_SCHEMA_NAME, queryText, timeBefore, timeAfter,
                    is(nullValue(CharSequence.class)), SCRIPT_QUERY_TYPE, null);
        }

        // Verify script statement query info.
        {
            String sql = "SELECT * FROM SYSTEM.SQL_QUERIES "
                    + "WHERE PARENT_ID=(SELECT ID FROM SYSTEM.SQL_QUERIES WHERE TYPE=?) "
                    + "ORDER BY STATEMENT_NUM";

            List<List<Object>> res = sql(0, sql, SCRIPT_QUERY_TYPE);

            assertThat(res, hasSize(3));

            Set<String> transactionIds = new HashSet<>();
            List<String> expectedQueries = List.of(
                    "SELECT x FROM TABLE(SYSTEM_RANGE(0, 2));",
                    "INSERT INTO test VALUES (0), (1);",
                    "SELECT x FROM TABLE(SYSTEM_RANGE(3, 5));"
            );

            for (int i = 0; i < res.size(); i++) {
                List<Object> row = res.get(i);

                verifyQueryInfo(row, initiator.name(), SqlCommon.DEFAULT_SCHEMA_NAME, expectedQueries.get(i), timeBefore, timeAfter,
                        hasLength(36), (i == 1 ? SqlQueryType.DML : SqlQueryType.QUERY).name(), i);

                transactionIds.add((String) row.get(7));
            }

            // Each statement uses it's own implicit transaction.
            assertThat(transactionIds, hasSize(3));
        }

        // Closing cursors.
        await(cursors.get(0).closeAsync());
        waitUntilRunningQueriesCount(is(3));

        await(cursors.get(1).closeAsync());
        waitUntilRunningQueriesCount(is(2));

        await(cursors.get(2).closeAsync());
        checkNoPendingQueries();
    }

    @Test
    public void multiStatementWithTransaction() throws InterruptedException {
        Ignite initiator = CLUSTER.node(0);
        ClockService clockService = unwrapIgniteImpl(initiator).clockService();

        String queryText = "START TRANSACTION;"
                + "INSERT INTO test VALUES (0), (1);"
                + "EXPLAIN PLAN FOR SELECT * FROM test;"
                + "SELECT * FROM test;"
                + "INSERT INTO test VALUES (2), (3);"
                + "COMMIT;";

        long timeBefore = clockService.now().getPhysical();
        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchCursors(runScript(queryText), 5, false);
        long timeAfter = clockService.now().getPhysical();

        // "DDL" and "EXPLAIN" queries close cursor automatically.
        waitUntilRunningQueriesCount(is(4));

        String sql = "SELECT * FROM SYSTEM.SQL_QUERIES "
                + "WHERE PARENT_ID=(SELECT ID FROM SYSTEM.SQL_QUERIES WHERE TYPE='SCRIPT') "
                + "ORDER BY STATEMENT_NUM";

        List<List<Object>> res = sql(0, sql);

        assertThat(res, hasSize(3));

        List<Object> row = res.get(0);

        // Expecting 3 queries with same transaction.
        verifyQueryInfo(row, initiator.name(), SqlCommon.DEFAULT_SCHEMA_NAME, "INSERT INTO test VALUES (0), (1);",
                timeBefore, timeAfter, hasLength(36), SqlQueryType.DML.name(), 1);

        verifyQueryInfo(res.get(1), initiator.name(), SqlCommon.DEFAULT_SCHEMA_NAME, "SELECT * FROM test;",
                timeBefore, timeAfter, equalTo((String) row.get(7)), SqlQueryType.QUERY.name(), 3);

        verifyQueryInfo(res.get(2), initiator.name(), SqlCommon.DEFAULT_SCHEMA_NAME, "INSERT INTO test VALUES (2), (3);",
                timeBefore, timeAfter, equalTo((String) row.get(7)), SqlQueryType.DML.name(), 4);

        for (AsyncSqlCursor<InternalSqlRow> cursor : cursors) {
            await(cursor.closeAsync());
        }

        // Commit transaction.
        await(await(cursors.get(cursors.size() - 1).nextResult()).closeAsync());
        checkNoPendingQueries();
    }

    @Test
    public void checkCleanupOnError() throws InterruptedException {
        // Parsing error.
        {
            assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query", () -> sql("CREATE TABLE b"));
            checkNoPendingQueries();

            assertThrowsSqlException(Sql.STMT_PARSE_ERR, "Failed to parse query", () -> runScript("CREATE TABLE b"));
            checkNoPendingQueries();
        }

        // Validation.
        {
            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Failed to validate query", () -> sql("insert into test values ('a')"));
            checkNoPendingQueries();

            assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "Failed to validate query",
                    () -> igniteSql().executeScript("insert into test values ('a')"));
            checkNoPendingQueries();
        }

        // Constraint violation.
        {
            assertThrowsSqlException(Sql.CONSTRAINT_VIOLATION_ERR, "PK unique constraint is violated",
                    () -> sql("insert into test values (1),(1)"));
            checkNoPendingQueries();

            assertThrowsSqlException(Sql.CONSTRAINT_VIOLATION_ERR, "PK unique constraint is violated",
                    () -> igniteSql().executeScript("insert into test values (1),(1)"));
            checkNoPendingQueries();
        }

        sql("INSERT INTO test VALUES(-1),(0)");

        // Runtime error.
        {
            assertThrowsSqlException(Sql.RUNTIME_ERR, "Division by zero", () -> sql("SELECT 1/id FROM test"));
            checkNoPendingQueries();

            AsyncSqlCursor<InternalSqlRow> cursor = runScript("SELECT 1/id FROM test");

            assertThrowsSqlException(Sql.RUNTIME_ERR, "Division by zero", () -> await(cursor.requestNextAsync(100)));
            checkNoPendingQueries();
        }
    }

    private void checkNoPendingQueries() {
        SqlTestUtils.waitUntilRunningQueriesCount(CLUSTER, is(0));
    }

    private static void verifyQueryInfo(
            List<Object> row,
            String nodeName,
            String schema,
            String query,
            long tsBefore,
            long tsAfter,
            Matcher<CharSequence> txIdMatcher,
            String queryType,
            @Nullable Integer statementNum
    ) {
        int idx = 0;

        // INITIATOR_NODE
        assertThat(row.get(idx++), equalTo(nodeName));

        // ID
        assertThat((String) row.get(idx++), hasLength(36));

        // PHASE
        assertThat(row.get(idx++), equalTo("EXECUTION"));

        // TYPE
        assertThat(row.get(idx++), equalTo(queryType));

        // SCHEMA
        assertThat(row.get(idx++), equalTo(schema));

        // SQL
        assertThat(row.get(idx++), equalTo(query));

        // START_TIME
        assertThat(((Instant) row.get(idx++)).toEpochMilli(), Matchers.allOf(greaterThanOrEqualTo(tsBefore), lessThanOrEqualTo(tsAfter)));

        // TRANSACTION_ID
        assertThat((String) row.get(idx++), txIdMatcher);

        // PARENT_ID
        assertThat((String) row.get(idx++), statementNum == null ? is(nullValue(CharSequence.class)) : hasLength(36));

        // STATEMENT_NUM
        assertThat(row.get(idx), statementNum == null ? is(nullValue()) : equalTo(statementNum));
    }
}
