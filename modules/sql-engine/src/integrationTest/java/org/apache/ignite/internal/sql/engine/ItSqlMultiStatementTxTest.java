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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests to verify the execution of queries with transaction control statements.
 *
 * @see SqlQueryType#TX_CONTROL
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItSqlMultiStatementTxTest extends BaseSqlMultiStatementTest {
    /** Default number of rows in the big table. */
    private static final int BIG_TABLE_ROWS_COUNT = Commons.IN_BUFFER_SIZE * 6;

    /** Number of completed transactions before the test started. */
    private int numberOfFinishedTransactionsOnStart;

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE test (id INT PRIMARY KEY)");

        createTable("big", 1, 1);
        sql("INSERT INTO big (id) SELECT x FROM TABLE(SYSTEM_RANGE(1, " + BIG_TABLE_ROWS_COUNT + "));");
    }

    @AfterEach
    void clearTestTable() {
        sql("DELETE FROM test");
    }

    @BeforeEach
    void saveFinishedTxCount() {
        numberOfFinishedTransactionsOnStart = txManager().finished();
    }

    @Test
    void emptyTransactionControlStatement() {
        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(
                runScript("START TRANSACTION; COMMIT"));

        assertThat(cursors, hasSize(2));

        AsyncSqlCursor<InternalSqlRow> cur = cursors.get(0);
        assertTrue(cur.hasNextResult());
        validateSingleResult(cur);

        cur = cursors.get(1);
        assertFalse(cur.hasNextResult());
        validateSingleResult(cur);

        verifyFinishedTxCount(1);
    }

    @Test
    void basicTxStatements() {
        executeScript("START TRANSACTION;"
                + "INSERT INTO test VALUES(0);"
                + "INSERT INTO test VALUES(1);"
                + "COMMIT;"

                + "START TRANSACTION;"
                + "COMMIT;"

                + "START TRANSACTION;"
                + "INSERT INTO test VALUES(2);"
                + "COMMIT;");

        verifyFinishedTxCount(3);

        assertQuery("select count(id) from test")
                .returns(3L).check();
    }

    @ParameterizedTest(name = "ReadOnly: {0}")
    @ValueSource(booleans = {true, false})
    void readMoreRowsThanCanBePrefetched(boolean readOnly) {
        String txOptions = readOnly ? "READ ONLY" : "READ WRITE";
        String specificStatement = readOnly ? "SELECT 1::BIGINT" : "UPDATE big SET salary=1 WHERE id=1";
        String query = format("START TRANSACTION {};"
                + "{};"
                + "SELECT id FROM big;"
                + "SELECT id FROM big;"
                + "COMMIT;", txOptions, specificStatement);

        AsyncSqlCursor<InternalSqlRow> cursor = runScript(query);

        assertTrue(cursor.hasNextResult());

        AsyncSqlCursor<InternalSqlRow> updateCursor = await(cursor.nextResult());
        assertNotNull(updateCursor);
        validateSingleResult(updateCursor, 1L);
        assertTrue(updateCursor.hasNextResult());

        AsyncSqlCursor<InternalSqlRow> selectCursor0 = await(updateCursor.nextResult());
        assertNotNull(selectCursor0);

        BatchedResult<InternalSqlRow> res = await(selectCursor0.requestNextAsync(BIG_TABLE_ROWS_COUNT / 2));
        assertNotNull(res);
        assertThat(res.items(), hasSize(BIG_TABLE_ROWS_COUNT / 2));
        assertEquals(1, txManager().pending(), "Transaction must not finished until the cursor is closed.");

        AsyncSqlCursor<InternalSqlRow> selectCursor1 = await(selectCursor0.nextResult());
        assertNotNull(selectCursor1);

        res = await(selectCursor1.requestNextAsync(BIG_TABLE_ROWS_COUNT * 2));
        assertNotNull(res);
        assertThat(res.items(), hasSize(BIG_TABLE_ROWS_COUNT));
        assertEquals(1, txManager().pending(), "Transaction must not finished until the cursor is closed.");

        await(selectCursor0.requestNextAsync(BIG_TABLE_ROWS_COUNT)); // Cursor must close implicitly.
        assertFalse(res.hasMore());
        verifyFinishedTxCount(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"READ ONLY", "READ WRITE"})
    void openedScriptTransactionRollsBackImplicitly(String txOptions) {
        String startTxStatement = format("START TRANSACTION {};", txOptions);

        {
            runScript(startTxStatement);

            verifyFinishedTxCount(1);
        }

        {
            List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(
                    runScript(startTxStatement
                            + "SELECT * FROM TEST;"
                            + "SELECT * FROM TEST;"
                    )
            );

            assertThat(cursors, hasSize(3));

            // The transaction depends on the cursors of the SELECT statement,
            // so it waits for them to close.
            assertEquals(1, txManager().pending());

            cursors.forEach(AsyncSqlCursor::closeAsync);
            verifyFinishedTxCount(2);
        }
    }

    @Test
    void dmlScriptRollsBackImplicitly() throws InterruptedException {
        AsyncSqlCursor<InternalSqlRow> cur = runScript("START TRANSACTION READ WRITE;"
                + "INSERT INTO test VALUES(0);"
                + "INSERT INTO test VALUES(1);"
                + "COMMIT;"

                + "START TRANSACTION READ WRITE;"
                + "SELECT * FROM BIG;"
                + "INSERT INTO test VALUES(2);"
        );

        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchCursors(cur, 3, false);
        assertThat(cursors, hasSize(3));

        // Set last cursor.
        cur = cursors.get(2);
        assertEquals(1, txManager().pending());

        assertTrue(cur.hasNextResult());
        assertFalse(cur.nextResult().isDone());

        cursors.forEach(AsyncSqlCursor::closeAsync);

        cur = await(cur.nextResult());
        assertNotNull(cur);

        // Fetch remaining.
        cursors = fetchAllCursors(cur);
        assertThat(cursors, hasSize(4));

        assertEquals(1, txManager().pending());

        // Rollback is performed asynchronously.
        cursors.forEach(c -> await(c.closeAsync()));

        // 1 COMMIT + 1 ROLLBACK.
        verifyFinishedTxCount(2);

        waitForCondition(() -> txManager().lockManager().isEmpty(), 2_000);

        // Make sure that the last transaction was rolled back.
        assertQuery("select count(id) from test")
                .returns(2L).check();
    }

    @Test
    void openedScriptTransactionRollsBackOnError() {
        String script = "START TRANSACTION READ WRITE;"
                + "INSERT INTO test VALUES(2);"
                + "INSERT INTO test VALUES(2/0);"
                + "SELECT 1;"
                + "COMMIT;";

        assertThrowsSqlException(RUNTIME_ERR, "Division by zero", () -> executeScript(script));

        verifyFinishedTxCount(1);

        assertQuery("select count(id) from test")
                .returns(0L).check();
    }

    @Test
    void commitWithoutOpenTransactionDoesNothing() {
        List<AsyncSqlCursor<InternalSqlRow>> cursors = fetchAllCursors(
                runScript("COMMIT; COMMIT; COMMIT;"));

        assertThat(cursors, hasSize(3));

        verifyFinishedTxCount(0);
    }

    @Test
    void ddlInsideExplicitTransactionFails() {
        String ddlStatement = "CREATE TABLE foo (id INT PRIMARY KEY)";

        {
            InternalTransaction tx = (InternalTransaction) igniteTx().begin();

            assertThrowsSqlException(RUNTIME_ERR, "DDL doesn't support transactions.",
                    () -> runScript(ddlStatement, tx));

            assertEquals(1, txManager().pending());
            tx.rollback();

            verifyFinishedTxCount(1);
        }

        {
            assertThrowsSqlException(RUNTIME_ERR, "DDL doesn't support transactions.",
                    () -> fetchAllCursors(runScript("START TRANSACTION;" + ddlStatement)));

            verifyFinishedTxCount(2);
        }
    }

    @Test
    void nestedTransactionStartFails() {
        AsyncSqlCursor<InternalSqlRow> cursor = runScript("START TRANSACTION; SELECT 1; START TRANSACTION;");

        AsyncSqlCursor<InternalSqlRow> startTxCur = await(cursor.nextResult());
        assertNotNull(startTxCur);

        assertThrowsSqlException(RUNTIME_ERR, "Nested transactions are not supported.", () -> await(startTxCur.nextResult()));

        verifyFinishedTxCount(1);
    }

    @Test
    void dmlFailsOnReadOnlyTransaction() {
        AsyncSqlCursor<InternalSqlRow> cursor = runScript("START TRANSACTION READ ONLY;"
                + "SELECT 1;"
                + "INSERT INTO test VALUES(0);"
                + "COMMIT;");

        AsyncSqlCursor<InternalSqlRow> insCur = await(cursor.nextResult());
        assertNotNull(insCur);

        assertThrowsSqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.",
                () -> await(insCur.nextResult()));

        verifyFinishedTxCount(1);
    }

    @Test
    void concurrentExecutionDoesntAffectSelectWithExplicitTx() {
        long tableSize = BIG_TABLE_ROWS_COUNT;

        @SuppressWarnings("ConcatenationWithEmptyString")
        String script = ""
                + "CREATE TABLE integers (i INT PRIMARY KEY);"
                + "START TRANSACTION;"
                + "INSERT INTO integers SELECT * FROM TABLE(system_range(1, " + tableSize + "));"
                + "SELECT count(*) FROM integers;"
                + "DELETE FROM integers;"
                + "COMMIT;"
                + "DROP TABLE integers;";

        AsyncSqlCursor<InternalSqlRow> cursor = runScript(script); // CREATE TABLE...
        cursor.closeAsync();

        cursor = await(cursor.nextResult()); // BEGIN...

        assertThat(cursor, notNullValue());
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
    void transactionControlStatementFailsWithExternalTransaction() {
        InternalTransaction tx1 = (InternalTransaction) igniteTx().begin();
        assertThrowsExactly(TxControlInsideExternalTxNotSupportedException.class, () -> runScript("COMMIT", tx1));
        assertEquals(1, txManager().pending());
        tx1.rollback();

        InternalTransaction tx2 = (InternalTransaction) igniteTx().begin();
        assertThrowsExactly(TxControlInsideExternalTxNotSupportedException.class, () -> runScript("START TRANSACTION", tx2));
        assertEquals(1, txManager().pending());
        tx2.rollback();

        verifyFinishedTxCount(2);
    }

    private void verifyFinishedTxCount(int expected) {
        int expectedTotal = numberOfFinishedTransactionsOnStart + expected;

        boolean success;

        try {
            success = waitForCondition(() -> expectedTotal == txManager().finished(), 2_000);

            if (!success) {
                assertEquals(expectedTotal, txManager().finished());
            }

            assertEquals(0, txManager().pending());
        } catch (InterruptedException e) {
            fail("Thread has been interrupted", e);
        }
    }
}
