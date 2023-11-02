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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TODO Blah-blah-blah.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlMultiStatementTest extends BaseSqlIntegrationTest {
    private static final String BASIC_MULTISTATEMENT_QUERY = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
            + "INSERT INTO test VALUES (0, 0);"
            + "SELECT * FROM test";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void dropTables() {
        dropAllTables();
    }

    @Test
    void queryWithIncorrectNumberOfDynamicParametersFails() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, ?);";

        String expectedMessage = "Unexpected number of query parameters";

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                expectedMessage,
                () -> await(runMultiStatementQuery(sql, 0)));

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                expectedMessage,
                () -> await(runMultiStatementQuery(sql, 0, 1, 2, 3, 4, 5)));
    }

    @Test
    void basicDynamicParameters() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, ?);";

        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> futItr = runMultiStatementQuery(sql, 0, 1, 2, 3);

        AsyncSqlCursorIterator<List<Object>> itr = await(futItr);

        while (itr.hasNext()) {
            await(itr.next());
        }

        assertQuery("SELECT * FROM test")
                .returns(0, 1)
                .returns(2, 3)
                .check();
    }

    @Test
    void scriptStopsExecutionOnRuntimeError() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT); select 2/0; INSERT INTO test VALUES (0, 0)";

        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> curItrFut = runMultiStatementQuery(sql);

        AsyncSqlCursorIterator<List<Object>> itr = await(curItrFut);

        await(itr.next());
        assertThrowsSqlException(Sql.RUNTIME_ERR, "/ by zero", () -> await(itr.next()));
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "execution was canceled", () -> await(itr.next()));

        assertQuery("SELECT count(*) FROM test").returns(0L).check();
    }

    @Test
    void scriptStopsExecutionOnValidationError() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT); INSERT INTO test VALUES (0, ?); INSERT INTO test VALUES (1, 1)";

        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> curItrFut = runMultiStatementQuery(sql, "incorrect param");

        AsyncSqlCursorIterator<List<Object>> itr = await(curItrFut);

        await(itr.next());
        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, "operator must have compatible types", () -> await(itr.next()));
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "execution was canceled", () -> await(itr.next()));

        assertQuery("SELECT count(*) FROM test").returns(0L).check();
    }


    @Test
    void testTxStatementsNotCreateCursors() {
        sql("CREATE TABLE test (id INT PRIMARY KEY);");
        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> curItrFut = runMultiStatementQuery("START TRANSACTION; COMMIT");

        AsyncSqlCursorIterator<?> curItr = curItrFut.join();

        assertFalse(curItr.hasNext());

        curItrFut = runMultiStatementQuery("START TRANSACTION; INSERT INTO test VALUES (0); COMMIT");

        curItr = curItrFut.join();

        assertTrue(curItr.hasNext());
        await(curItr.next());
        assertFalse(curItr.hasNext());

        assertQuery("SELECT count(*) FROM test")
                .returns(1L)
                .check();
    }

    @Test
    void testBasicMultiStatementFetchNone() throws InterruptedException {
        await(runMultiStatementQuery(BASIC_MULTISTATEMENT_QUERY));

        boolean success = waitForCondition(() -> {
            try {
                return sql("select * from test").size() == 1;
            } catch (SqlException e) {
                assertEquals(Sql.STMT_VALIDATION_ERR, e.code());

                return false;
            }
        }, 5_000);

        assertTrue(success);
    }

    @Test
    void testBasicMultiStatementFetchCursorsOnly() {
        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> curItrFut = runMultiStatementQuery(BASIC_MULTISTATEMENT_QUERY);

        AsyncSqlCursorIterator<List<Object>> curItr = curItrFut.join();

        while (curItr.hasNext()) {
            curItr.next().join().closeAsync();
        }

        assertQuery("select * from test").returns(0, 0).check();
    }

    @Test
    void testBasicMultiStatementFetchAll() {
        CompletableFuture<AsyncSqlCursorIterator<List<Object>>> curItrFut = runMultiStatementQuery(BASIC_MULTISTATEMENT_QUERY);

        AsyncSqlCursorIterator<List<Object>> curItr = curItrFut.join();

        while (curItr.hasNext()) {
            AsyncSqlCursor<List<Object>> cur = await(curItr.next());

            cur.requestNextAsync(1).join();

            cur.closeAsync();
        }
    }

    private static CompletableFuture<AsyncSqlCursorIterator<List<Object>>> runMultiStatementQuery(String query, Object ... params) {
        IgniteImpl ignite = CLUSTER.aliveNode();

        QueryProcessor qryProc = ignite.queryEngine();

        SessionId sessionId = qryProc.createSession(PropertiesHelper.emptyHolder());

        QueryContext context = QueryContext.create(SqlQueryType.SINGLE_STMT_TYPES);

        return qryProc.queryScriptAsync(sessionId, context, ignite.transactions(), query, params);
    }
}
