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
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.ignite.internal.sql.BaseSqlMultiStatementTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.ExternalTransactionNotSupportedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(
                runScript("START TRANSACTION; COMMIT"));

        assertThat(cursors, hasSize(2));

        validateSingleResult(cursors.get(0));
        validateSingleResult(cursors.get(1));

        verifyFinishedTxCount(1);
    }

    @Test
    void basicTxStatements() {
        fetchAllCursors(runScript(
                "START TRANSACTION;"
                        + "INSERT INTO test VALUES(0);"
                        + "INSERT INTO test VALUES(1);"
                        + "COMMIT;"

                        + "START TRANSACTION;"
                        + "COMMIT;"

                        + "START TRANSACTION;"
                        + "INSERT INTO test VALUES(2);"
                        + "COMMIT;"
        ));

        verifyFinishedTxCount(3);

        assertQuery("select count(id) from test")
                .returns(3L).check();
    }

    @Test
    void readMoreRowsThanCanBePrefetchedReadOnlyTx() {
        String query = "START TRANSACTION READ ONLY;"
                + "SELECT 1;"
                + "SELECT id FROM big;"
                + "COMMIT;";

        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(runScript(query));
        assertThat(cursors, hasSize(4));

        BatchedResult<List<Object>> res = await(cursors.get(2).requestNextAsync(BIG_TABLE_ROWS_COUNT * 2));
        assertNotNull(res);
        assertThat(res.items(), hasSize(BIG_TABLE_ROWS_COUNT));

        verifyFinishedTxCount(1);
    }

    @Test
    void readMoreRowsThanCanBePrefetchedReadWriteTx() {
        String query = "START TRANSACTION;"
                + "UPDATE big SET salary=1 WHERE id=1;"
                + "SELECT id FROM big;"
                + "COMMIT;";

        AsyncSqlCursor<List<Object>> cursor = runScript(query);

        assertTrue(cursor.hasNextResult());

        AsyncSqlCursor<List<Object>> updateCursor = await(cursor.nextResult());
        assertNotNull(updateCursor);
        validateSingleResult(updateCursor, 1L);
        assertTrue(updateCursor.hasNextResult());

        AsyncSqlCursor<List<Object>> selectCursor = await(updateCursor.nextResult());
        assertNotNull(selectCursor);

        BatchedResult<List<Object>> res = await(
                selectCursor.requestNextAsync(BIG_TABLE_ROWS_COUNT / 2));

        assertNotNull(res);
        assertThat(res.items(), hasSize(BIG_TABLE_ROWS_COUNT / 2));
        assertEquals(1, txManager().pending(), "Transaction must not finished until the cursor is closed.");
        assertFalse(selectCursor.nextResult().isDone());

        await(selectCursor.requestNextAsync(BIG_TABLE_ROWS_COUNT)); // Cursor must close implicitly.
        verifyFinishedTxCount(1);
    }

    @Test
    void openedScriptTransactionRollsBackImplicitly() {
        {
            fetchAllCursors(runScript("START TRANSACTION;"));
            verifyFinishedTxCount(1);
        }

        {
            fetchAllCursors(runScript("START TRANSACTION;"
                    + "INSERT INTO test VALUES(0);"
                    + "INSERT INTO test VALUES(1);"
                    + "COMMIT;"
                    + "START TRANSACTION;"
                    + "INSERT INTO test VALUES(2);"));

            verifyFinishedTxCount(3);

            assertQuery("select count(id) from test")
                    .returns(2L).check();
        }
    }

    @Test
    void openedScriptTransactionRollsBackOnError() {
        {
            AsyncSqlCursor<List<Object>> cursor = runScript(
                    "START TRANSACTION READ WRITE;"
                    + "INSERT INTO test VALUES(2);"
                    + "INSERT INTO test VALUES(2/0);"
                    + "SELECT 1;"
                    + "COMMIT;"
            );

            assertThrowsSqlException(RUNTIME_ERR, "/ by zero", () -> fetchAllCursors(cursor));

            verifyFinishedTxCount(1);

            assertQuery("select count(id) from test")
                    .returns(0L).check();
        }
    }

    @Test
    void commitWithoutOpenTransactionDoesNothing() {
        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(
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
        AsyncSqlCursor<List<Object>> cursor = runScript("START TRANSACTION; START TRANSACTION;");

        validateSingleResult(cursor);
        assertTrue(cursor.hasNextResult());

        assertThrowsSqlException(RUNTIME_ERR, "Nested transactions are not supported.", () -> await(cursor.nextResult()));

        verifyFinishedTxCount(1);
    }

    @Test
    void dmlFailsOnReadOnlyTransaction() {
        AsyncSqlCursor<List<Object>> cursor = runScript("START TRANSACTION READ ONLY;"
                + "INSERT INTO test VALUES(0);"
                + "COMMIT;");

        assertThrowsSqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.",
                () -> await(cursor.nextResult()));

        verifyFinishedTxCount(1);
    }

    @Test
    void transactionControlStatementFailsWithExternalTransaction() {
        InternalTransaction tx1 = (InternalTransaction) igniteTx().begin();
        assertThrowsExactly(ExternalTransactionNotSupportedException.class, () -> runScript("COMMIT", tx1));
        assertEquals(1, txManager().pending());
        tx1.rollback();

        InternalTransaction tx2 = (InternalTransaction) igniteTx().begin();
        assertThrowsExactly(ExternalTransactionNotSupportedException.class, () -> runScript("START TRANSACTION", tx2));
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
