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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.QueryHasNoMoreResultsException;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify the execution of queries with multiple statements.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlMultiStatementTest extends BaseSqlIntegrationTest {
    @AfterEach
    void dropTables() {
        dropAllTables();
    }

    @AfterEach
    void checkNoPendingTransactions() {
        assertEquals(0, txManager().pending());
    }

    @Test
    void basicMultiStatementQuery() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES (0, 0);"
                + "EXPLAIN PLAN FOR SELECT * FROM test;"
                + "SELECT * FROM test";

        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(runScript(sql));
        assertNotNull(cursors);

        Iterator<AsyncSqlCursor<List<Object>>> curItr = cursors.iterator();

        validateSingleResult(curItr.next(), true);
        validateSingleResult(curItr.next(), 1L);
        assertNotNull(curItr.next()); // skip EXPLAIN.
        validateSingleResult(curItr.next(), 0, 0);

        assertFalse(curItr.hasNext());

        // Ensures that the script is executed completely, even if the cursor data has not been read.
        fetchAllCursors(runScript("INSERT INTO test VALUES (1, 1);"
                + "INSERT INTO test VALUES (2, 2);"
                + "SELECT * FROM test;"
                + "INSERT INTO test VALUES (3, 3);"));

        assertQuery("select * from test")
                .returns(0, 0)
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .check();
    }

    /** Checks single statement execution using multi-statement API. */
    @Test
    void singleStatementQuery() {
        AsyncSqlCursor<List<Object>> cursor = runScript("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        assertNotNull(cursor);
        validateSingleResult(cursor, true);
        assertFalse(cursor.hasNextResult());
        assertThrows(QueryHasNoMoreResultsException.class, cursor::nextResult, "Query has no more results");

        AsyncSqlCursor<List<Object>> cursor2 = runScript("INSERT INTO test VALUES (0, 0)");
        assertNotNull(cursor2);
        validateSingleResult(cursor2, 1L);
        assertFalse(cursor2.hasNextResult());

        assertQuery("SELECT * FROM test").returns(0, 0).check();
    }

    @Test
    void queryWithDynamicParameters() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR DEFAULT '3');"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, DEFAULT);"
                + "INSERT INTO test VALUES(?, ?);";

        fetchAllCursors(runScript(sql, null, 0, "1", 2, 4, "5"));

        assertQuery("SELECT * FROM test")
                .returns(0, "1")
                .returns(2, "3")
                .returns(4, "5")
                .check();
    }

    @Test
    void queryWithIncorrectNumberOfDynamicParametersFailsWithValidationError() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, ?);";

        String expectedMessage = "Unexpected number of query parameters";

        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, null, 0));
        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, null, 0, 1, 2, 3, 4, 5));
    }

    @Test
    void transactionControlStatementDoesNotCreateCursor() {
        assertThat(runScript("START TRANSACTION; COMMIT"), nullValue());

        AsyncSqlCursor<List<Object>> cursor = runScript(
                "START TRANSACTION;"
                        + "SELECT 1;"
                        + "COMMIT"
        );

        assertNotNull(cursor);
        validateSingleResult(cursor, 1);

        assertFalse(cursor.hasNextResult());
    }

    @Test
    void scriptStopsExecutionOnError() {
        // Runtime error.
        AsyncSqlCursor<List<Object>> cursor = runScript(
                "CREATE TABLE test (id INT PRIMARY KEY);"
                + "SELECT 2/0;" // Runtime error.
                + "INSERT INTO test VALUES (0)"
        );
        assertNotNull(cursor);
        assertTrue(cursor.hasNextResult());

        CompletableFuture<AsyncSqlCursor<List<Object>>> curFut0 = cursor.nextResult();
        assertThrowsSqlException(RUNTIME_ERR, "/ by zero", () -> await(curFut0));

        // Validation error.
        assertThrowsSqlException(STMT_VALIDATION_ERR, "operator must have compatible types",
                () -> runScript("INSERT INTO test VALUES (?); INSERT INTO test VALUES (1)", null, "Incompatible param"));

        assertQuery("SELECT count(*) FROM test")
                .returns(0L)
                .check();

        // Internal error.
        cursor = runScript(
                "INSERT INTO test VALUES(0);"
                + "INSERT INTO test VALUES(1);"
                + "SELECT (SELECT id FROM test);" // Internal error.
                + "INSERT INTO test VALUES(2);"
        );
        assertNotNull(cursor);
        assertTrue(cursor.hasNextResult());

        cursor = await(cursor.nextResult());
        assertNotNull(cursor);
        assertTrue(cursor.hasNextResult());

        CompletableFuture<AsyncSqlCursor<List<Object>>> cursFut = cursor.nextResult();
        assertThrowsSqlException(INTERNAL_ERR, "Subquery returned more than 1 value", () -> await(cursFut));

        assertQuery("SELECT * FROM test")
                .returns(0)
                .returns(1)
                .check();

        // Internal error due to transaction exception.
        Transaction tx = igniteTx().begin();
        sql(tx, "INSERT INTO test VALUES(2);");
        tx.commit();

        assertThrowsSqlException(
                TX_FAILED_READ_WRITE_OPERATION_ERR,
                "Transaction is already finished",
                () -> runScript(
                        "INSERT INTO test VALUES(3); INSERT INTO test VALUES(4);",
                        (InternalTransaction) tx
                )
        );

        assertQuery("SELECT * FROM test")
                .returns(0)
                .returns(1)
                .returns(2)
                .check();

        // DDL inside external transaction.
        assertThrowsSqlException(STMT_VALIDATION_ERR, "DDL doesn't support transactions.",
                () -> runScript("CREATE TABLE test2 (id INT PRIMARY KEY)", (InternalTransaction) tx));
    }

    private @Nullable AsyncSqlCursor<List<Object>> runScript(String query) {
        return runScript(query, null);
    }

    private @Nullable AsyncSqlCursor<List<Object>> runScript(
            String query,
            @Nullable InternalTransaction tx,
            Object ... params
    ) {
        QueryProcessor qryProc = queryProcessor();

        return await(qryProc.queryScriptAsync(SqlPropertiesHelper.emptyProperties(), igniteTx(), tx, query, params));
    }

    private static @Nullable List<AsyncSqlCursor<List<Object>>> fetchAllCursors(@Nullable AsyncSqlCursor<List<Object>> cursor) {
        if (cursor == null) {
            return null;
        }

        List<AsyncSqlCursor<List<Object>>> cursors = new ArrayList<>();

        cursors.add(cursor);

        while (cursor.hasNextResult()) {
            cursor = await(cursor.nextResult());

            assertNotNull(cursor);

            cursors.add(cursor);
        }

        return cursors;
    }

    private static void validateSingleResult(AsyncSqlCursor<List<Object>> cursor, Object... expected) {
        BatchedResult<List<Object>> res = await(cursor.requestNextAsync(1));
        assertNotNull(res);
        assertEquals(List.of(List.of(expected)), res.items());
        assertFalse(res.hasMore());

        cursor.closeAsync();
    }
}
