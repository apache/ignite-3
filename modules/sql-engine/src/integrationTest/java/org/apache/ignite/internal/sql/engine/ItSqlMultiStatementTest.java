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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify the execution of queries with multiple statements.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlMultiStatementTest extends BaseSqlMultiStatementTest {
    @AfterEach
    void dropTables() {
        dropAllTables();
    }

    @Test
    void basicMultiStatementQuery() {
        String sql = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES (0, 0);"
                + "UPDATE test SET val=1 where id=0;"
                + "EXPLAIN PLAN FOR SELECT * FROM test;"
                + "SELECT * FROM test;"
                + "DELETE FROM test";

        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(runScript(sql));
        Iterator<AsyncSqlCursor<InternalSqlRow>> curItr = cursors.iterator();

        validateSingleResult(curItr.next(), true);
        validateSingleResult(curItr.next(), 1L);
        validateSingleResult(curItr.next(), 1L);
        assertNotNull(curItr.next()); // skip EXPLAIN.
        validateSingleResult(curItr.next(), 0, 1);
        validateSingleResult(curItr.next(), 1L);

        assertFalse(curItr.hasNext());

        cursors.forEach(AsyncSqlCursor::closeAsync);

        // Ensures that the script is executed completely, even if the cursor data has not been read.
        executeScript("INSERT INTO test VALUES (1, 1);"
                + "INSERT INTO test VALUES (2, 2);"
                + "SELECT * FROM test;"
                + "INSERT INTO test VALUES (3, 3);");

        assertQuery("select * from test")
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .check();
    }

    /** Checks single statement execution using multi-statement API. */
    @Test
    void singleStatementQuery() {
        AsyncSqlCursor<InternalSqlRow> cursor = runScript("CREATE TABLE test (id INT PRIMARY KEY, val INT)");
        validateSingleResult(cursor, true);
        assertFalse(cursor.hasNextResult());
        assertThrows(NoSuchElementException.class, cursor::nextResult, "Query has no more results");

        AsyncSqlCursor<InternalSqlRow> cursor2 = runScript("INSERT INTO test VALUES (0, 0)");
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

        executeScript(sql, 0, "1", 2, 4, "5");

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

        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, 0));
        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, 0, 1, 2, 3, 4, 5));
    }

    @Test
    void scriptStopsExecutionOnError() {
        // Runtime error.
        {
            assertThrowsSqlException(
                    RUNTIME_ERR,
                    "Division by zero",
                    () -> executeScript(
                            "CREATE TABLE test (id INT PRIMARY KEY);"
                                    + "INSERT INTO test VALUES (2/0);" // Runtime error.
                                    + "INSERT INTO test VALUES (0)"
                    )
            );

            assertQuery("SELECT count(*) FROM test")
                    .returns(0L)
                    .check();
        }

        // Validation error.
        {
            assertThrowsSqlException(
                    STMT_VALIDATION_ERR,
                    "Values passed to VALUES operator must have compatible types",
                    () -> executeScript(
                            "INSERT INTO test VALUES (?), (?)",
                            "1", 2
                    )
            );

            assertQuery("SELECT count(*) FROM test")
                    .returns(0L)
                    .check();
        }

        // Internal error.
        {
            assertThrowsSqlException(
                    RUNTIME_ERR,
                    "Subquery returned more than 1 value",
                    () -> executeScript(
                            "INSERT INTO test VALUES(0);"
                                    + "INSERT INTO test VALUES(1);"
                                    + "DELETE FROM test WHERE id = (SELECT id FROM test);" // Internal error.
                                    + "INSERT INTO test VALUES(2);"
                    )
            );

            assertQuery("SELECT * FROM test")
                    .returns(0)
                    .returns(1)
                    .check();
        }

        // Internal error due to transaction exception.
        {
            Transaction tx = igniteTx().begin();
            sql(tx, "INSERT INTO test VALUES(2);");
            tx.commit();

            assertThrowsSqlException(
                    TX_ALREADY_FINISHED_ERR,
                    "Transaction is already finished",
                    () -> executeScript(
                            "INSERT INTO test VALUES(3); INSERT INTO test VALUES(4);",
                            (InternalTransaction) tx
                    )
            );

            assertQuery("SELECT * FROM test")
                    .returns(0)
                    .returns(1)
                    .returns(2)
                    .check();
        }
    }

    /**
     * Test verifies that if an error occurs within the script,
     * previous statements will not be forcibly cancelled.
     *
     * <p>Note: this behavior is important for {@code JdbcStatement.executeBatch} method,
     *          because it executes a batch of statements as a script, and if an error
     *          occurs somewhere, it is impossible to correctly count the number of
     *          updates (for a batch update exception).
     */
    @Test
    void precedingStatementsAreNotAbortedOnError() throws InterruptedException {
        String script = "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test VALUES (0, 0), (1, 1), (2, 2);"
                + "UPDATE test SET val = val + 1;"
                + "INSERT INTO test VALUES (3, 3/0);";

        AsyncSqlCursor<InternalSqlRow> ddlCursor = runScript(script);
        AsyncSqlCursor<InternalSqlRow> dmlCursor1 = await(ddlCursor.nextResult());
        AsyncSqlCursor<InternalSqlRow> dmlCursor2 = await(dmlCursor1.nextResult());

        assertThrowsSqlException(
                RUNTIME_ERR,
                "Division by zero",
                () -> await(dmlCursor2.nextResult())
        );

        // Put some delay to ensure no one tries to close the cursors in the background.
        Thread.sleep(100);

        validateSingleResult(ddlCursor, true);
        validateSingleResult(dmlCursor1, 3L);
        validateSingleResult(dmlCursor2, 3L);

        // Cursors are already closed by validateSingleResult above.
    }

    @Test
    void concurrentExecutionDoesntAffectSelectWithImplicitTx() {
        long tableSize = 1_000;

        @SuppressWarnings("ConcatenationWithEmptyString")
        String script = ""
                + "CREATE TABLE integers (i INT PRIMARY KEY);"
                + "INSERT INTO integers SELECT * FROM TABLE(system_range(1, " + tableSize + "));"
                + "SELECT count(*) FROM integers;"
                + "DELETE FROM integers;"
                + "DROP TABLE integers;";

        AsyncSqlCursor<InternalSqlRow> cursor = runScript(script); // CREATE TABLE...
        cursor.closeAsync();

        cursor = await(cursor.nextResult()); // INSERT...

        assertThat(cursor, notNullValue());
        cursor.closeAsync();

        cursor = await(cursor.nextResult()); // SELECT...

        assertThat(cursor, notNullValue());

        BatchedResult<InternalSqlRow> batch = await(cursor.requestNextAsync(1));

        assertThat(batch, notNullValue());

        assertThat(batch.items().get(0).get(0), is(tableSize));

        iterateThroughResultsAndCloseThem(cursor);
    }

    @Test
    void indexesAvailableAfterScriptExecutionAndBuiltProperly() {
        long tableSize = 1_000;

        @SuppressWarnings("ConcatenationWithEmptyString")
        String script = ""
                + "CREATE TABLE integers (id INT PRIMARY KEY, val_1 INT, val_2 INT);"
                + "CREATE INDEX integers_val_1_ind ON integers(val_1);"
                + "INSERT INTO integers SELECT x, x, x FROM system_range(1, " + tableSize + ");"
                + "CREATE INDEX integers_val_2_ind ON integers(val_2);";

        AsyncSqlCursor<InternalSqlRow> cursor = runScript(script);

        iterateThroughResultsAndCloseThem(cursor);

        assertQuery("SELECT /*+ FORCE_INDEX(INTEGERS_VAL_1_IND) */ COUNT(*) FROM integers WHERE val_1 > 0")
                .matches(containsIndexScan("PUBLIC", "INTEGERS", "INTEGERS_VAL_1_IND"))
                .returns(tableSize)
                .check();

        assertQuery("SELECT /*+ FORCE_INDEX(INTEGERS_VAL_2_IND) */ COUNT(*) FROM integers WHERE val_2 > 0")
                .matches(containsIndexScan("PUBLIC", "INTEGERS", "INTEGERS_VAL_2_IND"))
                .returns(tableSize)
                .check();
    }

    @Test
    void batchedAlterTableProcessedCorrectly() {
        long tableSize = 1_000;

        {
            AsyncSqlCursor<InternalSqlRow> cursor = runScript(
                    "CREATE TABLE test1 (id INT PRIMARY KEY, val INT);"
                            + "ALTER TABLE test1 DROP COLUMN val;"
                            + "ALTER TABLE test1 ADD COLUMN val INT;"
                            + "INSERT INTO test1 SELECT x, x FROM system_range(1, " + tableSize + ");"
            );

            iterateThroughResultsAndCloseThem(cursor);

            assertQuery("SELECT COUNT(*) FROM test1")
                    .returns(tableSize)
                    .check();
        }

        {
            sql("CREATE TABLE test2 (id INT PRIMARY KEY, val INT);");

            AsyncSqlCursor<InternalSqlRow> cursor = runScript(
                    "ALTER TABLE test2 DROP COLUMN val;"
                            + "ALTER TABLE test2 ADD COLUMN val INT;"
                            + "INSERT INTO test2 SELECT x, x FROM system_range(1, " + tableSize + ");"
            );

            iterateThroughResultsAndCloseThem(cursor);

            assertQuery("SELECT COUNT(*) FROM test2")
                    .returns(tableSize)
                    .check();
        }
    }

    @Test
    void statementRestrictedByQueryType() {
        BiFunction<SqlProperties, String, AsyncSqlCursor<InternalSqlRow>> runner = (props, query) ->
                await(queryProcessor().queryAsync(props, observableTimeTracker(), null, null, query));

        SqlProperties properties = new SqlProperties()
                .allowMultiStatement(true)
                .allowedQueryTypes(EnumSet.of(SqlQueryType.QUERY));

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Statement of type \"DML\" is not allowed in current context",
                () -> runner.apply(properties, "UPDATE xxx SET id = 1; SELECT 1;")
        );

        assertThrowsSqlException(
                STMT_VALIDATION_ERR,
                "Statement of type \"Transaction control statement\" is not allowed in current context",
                () -> runner.apply(properties, "START TRANSACTION; SELECT 1; COMMIT;")
        );

        fetchCursors(runner.apply(properties, "SELECT 1; SELECT 2;"), -1, true);
    }
}
