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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.EXECUTION_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_FAILED_READ_WRITE_OPERATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.QueryProcessor.AsyncSqlCursorIterator;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify the execution of queries with multiple statements.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlMultiStatementTest extends BaseSqlIntegrationTest {
    private static final String CANCELLED_ERR_MSG = "script execution was canceled";

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

        AsyncSqlCursorIterator<List<Object>> itr = runScript(sql);

        validateSingleResult(itr.next(), true);
        validateSingleResult(itr.next(), 1L);
        assertThat(itr.next(), willCompleteSuccessfully()); // Skip explain.
        validateSingleResult(itr.next(), 0, 0);

        assertFalse(itr.hasNext());

        // Ensures that the script is executed completely, even if the cursor data has not been read.
        sql = "INSERT INTO test VALUES (1, 1);"
                + "INSERT INTO test VALUES (2, 2);"
                + "SELECT * FROM test;"
                + "INSERT INTO test VALUES (3, 3);";

        itr = runScript(sql);

        List<AsyncSqlCursor<List<Object>>> cursorsToClose = new ArrayList<>();

        while (itr.hasNext()) {
            AsyncSqlCursor<List<Object>> cursor = await(itr.next());

            cursorsToClose.add(cursor);
        }

        assertQuery("select * from test")
                .returns(0, 0)
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .check();

        cursorsToClose.forEach(AsyncCursor::closeAsync);
    }

    /** Checks single statement execution using multi-statement API. */
    @Test
    void singleStatementQuery() {
        AsyncSqlCursorIterator<List<Object>> itr = runScript("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        validateSingleResult(itr.next(), true);
        assertFalse(itr.hasNext());

        itr = runScript("INSERT INTO test VALUES (0, 0)");
        validateSingleResult(itr.next(), 1L);
        assertFalse(itr.hasNext());
    }

    @Test
    void queryWithDynamicParameters() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR DEFAULT '3');"
                + "INSERT INTO test VALUES(?, ?);"
                + "INSERT INTO test VALUES(?, DEFAULT);"
                + "INSERT INTO test VALUES(?, ?);";

        AsyncSqlCursorIterator<List<Object>> itr = runScript(sql, null, 0, "1", 2, 4, "5");
        assertThat(itr, notNullValue());

        while (itr.hasNext()) {
            assertThat(itr.next(), willCompleteSuccessfully());
        }

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
        assertFalse(runScript("START TRANSACTION; COMMIT").hasNext());

        AsyncSqlCursorIterator<List<Object>> curItr = runScript(
                "START TRANSACTION;"
                        + "SELECT 1;"
                        + "COMMIT"
        );

        validateSingleResult(curItr.next(), 1);

        assertFalse(curItr.hasNext());
    }

    @Test
    void scriptStopsExecutionOnError() {
        // Runtime error.
        String sql = "CREATE TABLE test (id INT PRIMARY KEY);"
                + "SELECT 2/0;"
                + "INSERT INTO test VALUES (0)";

        AsyncSqlCursorIterator<List<Object>> itr0 = runScript(sql);

        assertThat(itr0.next(), willCompleteSuccessfully());
        assertThrowsSqlException(RUNTIME_ERR, "/ by zero", () -> await(itr0.next()));
        assertThrowsSqlException(EXECUTION_CANCELLED_ERR, "execution was canceled", () -> await(itr0.next()));

        // Validation error.
        sql = "INSERT INTO test VALUES (?);"
                + "INSERT INTO test VALUES (1)";

        AsyncSqlCursorIterator<List<Object>> itr1 = runScript(sql, null, "Incompatible param");

        assertThrowsSqlException(STMT_VALIDATION_ERR, "operator must have compatible types", () -> await(itr1.next()));
        assertThrowsSqlException(EXECUTION_CANCELLED_ERR, CANCELLED_ERR_MSG, () -> await(itr1.next()));

        assertQuery("SELECT count(*) FROM test").returns(0L).check();

        // Internal error.
        sql = "INSERT INTO test VALUES(0);"
                + "INSERT INTO test VALUES(1);"
                + "SELECT (SELECT id FROM test);"
                + "INSERT INTO test VALUES(2);";

        AsyncSqlCursorIterator<List<Object>> itr2 = runScript(sql);

        for (int i = 0; i < 2; i++) {
            assertTrue(itr2.hasNext());
            assertThat(itr2.next(), willCompleteSuccessfully());
        }

        assertTrue(itr2.hasNext());

        assertThrowsSqlException(INTERNAL_ERR, "Subquery returned more than 1 value", () -> await(itr2.next()));
        assertThrowsSqlException(EXECUTION_CANCELLED_ERR, CANCELLED_ERR_MSG, () -> await(itr2.next()));

        assertQuery("SELECT * FROM test")
                .returns(0)
                .returns(1)
                .check();

        // Internal error due to transaction exception.
        Transaction tx = igniteTx().begin();
        sql(tx, "INSERT INTO test VALUES(2);");
        tx.commit();

        sql = "INSERT INTO test VALUES(3);"
                + "INSERT INTO test VALUES(4);"
                + "SELECT 1;";

        AsyncSqlCursorIterator<List<Object>> itr3 = runScript(sql, (InternalTransaction) tx);

        assertThrowsSqlException(TX_FAILED_READ_WRITE_OPERATION_ERR, "Transaction is already finished", () -> await(itr3.next()));
        assertThrowsSqlException(EXECUTION_CANCELLED_ERR, CANCELLED_ERR_MSG, () -> await(itr3.next()));
        assertThrowsSqlException(EXECUTION_CANCELLED_ERR, CANCELLED_ERR_MSG, () -> await(itr3.next()));

        assertFalse(itr3.hasNext());

        // DDL inside outer transaction.
        assertThrowsSqlException(STMT_VALIDATION_ERR, "DDL doesn't support transactions.",
                () -> await(runScript("CREATE TABLE test2 (id INT PRIMARY KEY)", (InternalTransaction) tx).next()));
    }

    private AsyncSqlCursorIterator<List<Object>> runScript(String query) {
        return runScript(query, null);
    }

    private AsyncSqlCursorIterator<List<Object>> runScript(
            String query,
            @Nullable InternalTransaction tx,
            Object ... params
    ) {
        QueryProcessor qryProc = queryProcessor();

        return Objects.requireNonNull(
                await(qryProc.queryScriptAsync(SqlPropertiesHelper.emptyProperties(), igniteTx(), tx, query, params)),
                "Unexpected 'null' result"
        );
    }

    private static void validateSingleResult(CompletableFuture<AsyncSqlCursor<List<Object>>> cursorFut, Object... expected) {
        AsyncSqlCursor<List<Object>> cursor = await(cursorFut);
        assertNotNull(cursor);

        BatchedResult<List<Object>> res = await(cursor.requestNextAsync(1));
        assertNotNull(res);
        assertEquals(List.of(List.of(expected)), res.items());
        assertFalse(res.hasMore());

        cursor.closeAsync();
    }
}
