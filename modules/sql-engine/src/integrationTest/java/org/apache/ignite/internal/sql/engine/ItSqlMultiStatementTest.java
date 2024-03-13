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
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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

        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, null, 0));
        assertThrowsSqlException(STMT_VALIDATION_ERR, expectedMessage, () -> runScript(sql, null, 0, 1, 2, 3, 4, 5));
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
                    "operator must have compatible types",
                    () -> executeScript(
                            "INSERT INTO test VALUES (?);"
                                    + "INSERT INTO test VALUES (1)",
                            "Incompatible param"
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
}
