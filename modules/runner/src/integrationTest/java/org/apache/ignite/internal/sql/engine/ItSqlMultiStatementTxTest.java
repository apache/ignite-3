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
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.sql.BaseSqlMultiStatementTest;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.AsyncCursor;
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
    /** Number of completed transactions before the test started. */
    private int numberOfFinishedTransactionsOnStart;

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE test (id INT PRIMARY KEY)");
    }

    @AfterEach
    void clearTestTable() {
        sql("DELETE FROM test");
    }

    @BeforeEach
    void saveTxCount() {
        numberOfFinishedTransactionsOnStart = txManager().finished();
    }

    void checkFinished(int expected) {
        assertEquals(numberOfFinishedTransactionsOnStart + expected, txManager().finished());
    }

    @Test
    void emptyTransactionControlStatement() {
        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(
                runScript("START TRANSACTION; COMMIT"));

        assertThat(cursors, hasSize(2));

        validateSingleResult(cursors.get(0));
        validateSingleResult(cursors.get(1));

        checkFinished(1);
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

        checkFinished(3);

        assertQuery("select count(id) from test")
                .returns(3L).check();
    }

    @Test
    void transactionIsClosedWhenAllCursorsClosed() throws InterruptedException {
        int rowsCount = Commons.IN_BUFFER_SIZE * 6;

        createTable("big", 1, 1);

        try {
            {
                String query = "START TRANSACTION;"
                        + "INSERT INTO big (id) SELECT x FROM TABLE(SYSTEM_RANGE(1, " + rowsCount + "));"
                        + "SELECT id FROM big;"
                        + "SELECT id FROM big;"
                        + "COMMIT;";

                AsyncSqlCursor<List<Object>> cursor = runScript(query);

                assertTrue(cursor.hasNextResult());

                AsyncSqlCursor<List<Object>> insCursor = await(cursor.nextResult());
                validateSingleResult(insCursor, (long) rowsCount);
                assertTrue(insCursor.hasNextResult());

                AsyncSqlCursor<List<Object>> selectCursor1 = await(insCursor.nextResult());
                AsyncSqlCursor<List<Object>> selectCursor2 = await(selectCursor1.nextResult());

                // Constant delay - to make sure that the transaction is not closed before all the results are read.
                assertFalse(waitForCondition(() -> selectCursor2.nextResult().isDone(), 1_000));

                await(selectCursor2.requestNextAsync(rowsCount));
                await(selectCursor1.requestNextAsync(rowsCount));

                selectCursor1.closeAsync();
                selectCursor2.closeAsync();

                await(selectCursor2.nextResult());

                assertEquals(0, txManager().pending());
            }

            {
                String query = "START TRANSACTION;"
                        + "INSERT INTO big (id) values (0);"
                        + "SELECT id FROM big;";

                AsyncSqlCursor<List<Object>> startTxCursor = runScript(query);

                AsyncSqlCursor<List<Object>> insCursor = await(startTxCursor.nextResult());

                AsyncSqlCursor<List<Object>> selectCursor = await(insCursor.nextResult());
                assertFalse(selectCursor.hasNextResult());

                AsyncCursor.BatchedResult<List<Object>> res = await(
                        selectCursor.requestNextAsync(rowsCount * 2));

                assertEquals(rowsCount + 1, res.items().size());

                assertEquals(0, txManager().pending());

                assertQuery("select count(*) from big where id=0").returns(0L).check();
            }
        } finally {
            sql("DROP TABLE big");
        }
    }

    @Test
    void scriptTransactionRollsBackImplicitly() {
        {
            fetchAllCursors(runScript("START TRANSACTION;"));
            checkNoPendingTransactions();
            checkFinished(1);
        }

        {
            fetchAllCursors(runScript("START TRANSACTION;"
                    + "INSERT INTO test VALUES(0);"
                    + "INSERT INTO test VALUES(1);"
                    + "COMMIT;"
                    + "START TRANSACTION;"
                    + "INSERT INTO test VALUES(2);"));

            checkFinished(3);

            checkNoPendingTransactions();

            assertQuery("select count(id) from test")
                    .returns(2L).check();
        }
    }

    @Test
    void scriptTransactionRollsBackOnError() {
        {
            AsyncSqlCursor<List<Object>> cursor = runScript(
                    "START TRANSACTION;"
                    + "INSERT INTO test VALUES(2);"
                    + "INSERT INTO test VALUES(2/0);"
                    + "COMMIT;"
            );

            assertThrowsSqlException(RUNTIME_ERR, "/ by zero", () -> fetchAllCursors(cursor));

            checkNoPendingTransactions();
            checkFinished(1);

            assertQuery("select count(id) from test")
                    .returns(0L).check();
        }
    }

    @Test
    void commitWithoutOpenTransactionDoesNothing() {
        List<AsyncSqlCursor<List<Object>>> cursors = fetchAllCursors(
                runScript("COMMIT; COMMIT; COMMIT;"));

        assertThat(cursors, hasSize(3));

        checkFinished(0);
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

            checkFinished(1);
        }

        {
            assertThrowsSqlException(RUNTIME_ERR, "DDL doesn't support transactions.",
                    () -> fetchAllCursors(runScript("START TRANSACTION;" + ddlStatement)));

            checkFinished(2);
        }
    }

    @Test
    void nestedTransactionStartFails() {
        AsyncSqlCursor<List<Object>> cursor = runScript("START TRANSACTION; START TRANSACTION;");

        validateSingleResult(cursor);
        assertTrue(cursor.hasNextResult());

        assertThrowsSqlException(RUNTIME_ERR, "Nested transactions are not supported.", () -> await(cursor.nextResult()));

        checkFinished(1);
    }

    @Test
    void dmlFailsOnReadOnlyTransaction() {
        AsyncSqlCursor<List<Object>> cursor = runScript("START TRANSACTION READ ONLY;"
                + "INSERT INTO test VALUES(0);"
                + "COMMIT;");

        assertThrowsSqlException(RUNTIME_ERR, "DML query cannot be started by using read only transactions.",
                () -> await(cursor.nextResult()));

        checkFinished(1);
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

        checkFinished(2);
    }
}
