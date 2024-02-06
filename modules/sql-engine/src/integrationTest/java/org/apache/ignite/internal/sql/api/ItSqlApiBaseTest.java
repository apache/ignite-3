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

package org.apache.ignite.internal.sql.api;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.asStream;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.api.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.NoRowSetExpectedException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for SQL API.
 * Tests will be run through synchronous, asynchronous API and client entry points.
 * By default, any SQL API test should be added to the base class and use special provided methods to interact
 * with the API in a API-type-independent manner. For any API-specific test, should be used the appropriate subclass.
 */
public abstract class ItSqlApiBaseTest extends BaseSqlIntegrationTest {
    protected static final int ROW_COUNT = 16;

    @AfterEach
    public void dropTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    @Test
    public void ddl() throws Exception {
        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        // CREATE TABLE
        checkDdl(true, ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.TEST' already exists",
                ses,
                "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)"
        );
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL' specified more than once",
                ses,
                "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL INT, VAL INT)"
        );
        checkDdl(false, ses, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

        // ADD COLUMN
        checkDdl(true, ses, "ALTER TABLE TEST ADD COLUMN VAL1 VARCHAR");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.NOT_EXISTS_TABLE' not found",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL1' already exists",
                ses,
                "ALTER TABLE TEST ADD COLUMN VAL1 INT"
        );

        // CREATE INDEX
        checkDdl(true, ses, "CREATE INDEX TEST_IDX ON TEST(VAL0)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Index with name 'PUBLIC.TEST_IDX' already exists",
                ses,
                "CREATE INDEX TEST_IDX ON TEST(VAL1)"
        );
        checkDdl(false, ses, "CREATE INDEX IF NOT EXISTS TEST_IDX ON TEST(VAL1)");

        checkDdl(true, ses, "DROP INDEX TESt_iDX");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX1 ON TEST(VAL0)");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX2 ON TEST(VAL0)");
        checkDdl(true, ses, "CREATE INDEX TEST_IDX3 ON TEST(ID, VAL0, VAL1)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL0' specified more than once",
                ses,
                "CREATE INDEX TEST_IDX4 ON TEST(VAL0, VAL0)"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column 'VAL1' used by index(es) [TEST_IDX3], it is not allowed",
                ses,
                "ALTER TABLE TEST DROP COLUMN val1"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column 'VAL0' used by index(es) [TEST_IDX1, TEST_IDX2, TEST_IDX3], it is not allowed",
                ses,
                "ALTER TABLE TEST DROP COLUMN (val0, val1)"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column `ID` belonging to primary key is not allowed",
                ses,
                "ALTER TABLE TEST DROP COLUMN id"
        );

        checkDdl(true, ses, "DROP INDEX TESt_iDX3");

        // DROP COLUMNS
        checkDdl(true, ses, "ALTER TABLE TEST DROP COLUMN VAL1");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.NOT_EXISTS_TABLE' not found",
                ses,
                "ALTER TABLE NOT_EXISTS_TABLE DROP COLUMN VAL1"
        );
        checkDdl(false, ses, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE DROP COLUMN VAL1");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL1' not found in table 'PUBLIC.TEST'",
                ses,
                "ALTER TABLE TEST DROP COLUMN VAL1"
        );

        // DROP TABLE
        checkDdl(false, ses, "DROP TABLE IF EXISTS NOT_EXISTS_TABLE");

        checkDdl(true, ses, "DROP TABLE TEST");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.TEST' not found",
                ses,
                "DROP TABLE TEST"
        );

        checkDdl(false, ses, "DROP INDEX IF EXISTS TEST_IDX");

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Index with name 'PUBLIC.TEST_IDX' not found",
                ses,
                "DROP INDEX TEST_IDX"
        );
    }

    /** Check all transactions are processed correctly even with case of sql Exception raised. */
    @Test
    public void implicitTransactionsStates() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManager = txManager();

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Table with name 'PUBLIC.TEST' already exists",
                    () -> execute(ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)")
            );
        }

        // No new transactions through ddl.
        assertEquals(0, txManager.pending());
    }

    /** Check correctness of implicit and explicit transactions. */
    @Test
    public void checkTransactionsWithDml() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        TxManager txManagerInternal = txManager();

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = ROW_COUNT; i < 2 * ROW_COUNT; ++i) {
            checkDml(1, outerTx, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        commit(outerTx);

        // Outdated tx.
        Transaction outerTx0 = outerTx;
        assertThrowsSqlException(
                Transactions.TX_ALREADY_FINISHED_ERR,
                "Transaction is already finished",
                () -> checkDml(1, outerTx0, ses, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

        ResultSet<SqlRow> rs = executeForRead(ses, "SELECT VAL0 FROM TEST ORDER BY VAL0");

        assertEquals(2 * ROW_COUNT, asStream(rs).count());

        rs.close();

        outerTx = igniteTx().begin();

        rs = executeForRead(ses, outerTx, "SELECT VAL0 FROM TEST ORDER BY VAL0");

        assertEquals(2 * ROW_COUNT, asStream(rs).count());

        rs.close();

        outerTx.commit();

        checkDml(2 * ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(2 * ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");

        assertEquals(ROW_COUNT + 1 + 1 + 1 + 1 + 1 + 1, txManagerInternal.finished() - txPrevCnt);

        assertEquals(0, txManagerInternal.pending());
    }

    /** Check correctness of explicit transaction rollback. */
    @Test
    public void checkExplicitTxRollback() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Session ses = sql.createSession();

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, outerTx, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        rollback(outerTx);

        ResultSet<SqlRow> rs = executeForRead(ses, "SELECT VAL0 FROM TEST ORDER BY VAL0");

        asStream(rs);
        assertEquals(0, asStream(rs).count());

        rs.close();
    }

    /** Check correctness of rw and ro transactions for table scan. */
    @Test
    public void checkMixedTransactionsForTable() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Matcher<String> planMatcher = containsTableScan("PUBLIC", "TEST");

        checkMixedTransactions(planMatcher);
    }


    /** Check correctness of rw and ro transactions for index scan. */
    @Test
    public void checkMixedTransactionsForIndex() throws Exception {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        sql("CREATE INDEX TEST_IDX ON TEST(VAL0)");

        Matcher<String> planMatcher = containsIndexScan("PUBLIC", "TEST", "TEST_IDX");

        checkMixedTransactions(planMatcher);
    }

    private void checkMixedTransactions(Matcher<String> planMatcher) {
        IgniteSql sql = igniteSql();

        Session ses = sql.createSession();

        for (int i = 0; i < ROW_COUNT; ++i) {
            executeForRead(ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        List<Boolean> booleanList = List.of(Boolean.TRUE, Boolean.FALSE);
        for (boolean roTx : booleanList) {
            for (boolean commit : booleanList) {
                for (boolean explicit : booleanList) {
                    checkTx(ses, roTx, commit, explicit, planMatcher);
                }
            }
        }
    }

    private void checkTx(Session ses, boolean readOnly, boolean commit, boolean explicit, Matcher<String> planMatcher) {
        Transaction outerTx = explicit ? igniteTx().begin(new TransactionOptions().readOnly(readOnly)) : null;

        String queryRo = "SELECT VAL0 FROM TEST ORDER BY VAL0";

        assertQuery(queryRo).matches(planMatcher).check();

        ResultSet<SqlRow> rs = executeForRead(ses, outerTx, queryRo);

        assertEquals(ROW_COUNT, asStream(rs).count());

        rs.close();

        String queryRw = "UPDATE TEST SET VAL0=VAL0+1";
        if (explicit && readOnly) {
            assertThrowsSqlException(Sql.RUNTIME_ERR, "DML query cannot be started by using read only transactions.",
                    () -> execute(outerTx, ses, queryRw));
        } else {
            checkDml(ROW_COUNT, outerTx, ses, queryRw);
        }

        if (outerTx != null) {
            if (commit) {
                outerTx.commit();
            } else {
                outerTx.rollback();
            }
        }
    }

    @Test
    public void metadata() {
        sql("CREATE TABLE TEST(COL0 BIGINT PRIMARY KEY, COL1 VARCHAR NOT NULL)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        execute(ses, "INSERT INTO TEST VALUES (?, ?)", 1L, "some string");

        ResultSet<SqlRow> rs = executeForRead(ses, "SELECT COL1, COL0 FROM TEST");

        // Validate columns metadata.
        ResultSetMetadata meta = rs.metadata();

        assertNotNull(meta);
        assertEquals(-1, meta.indexOf("COL"));
        assertEquals(0, meta.indexOf("COL1"));
        assertEquals(1, meta.indexOf("COL0"));

        checkMetadata(new ColumnMetadataImpl(
                        "COL1",
                        ColumnType.STRING,
                        CatalogUtils.DEFAULT_VARLEN_LENGTH,
                        ColumnMetadata.UNDEFINED_SCALE,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL1")),
                meta.columns().get(0));
        checkMetadata(new ColumnMetadataImpl(
                        "COL0",
                        ColumnType.INT64,
                        19,
                        0,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "COL0")),
                meta.columns().get(1));

        // Validate result columns types.
        assertTrue(rs.hasRowSet());

        SqlRow row = rs.next();

        rs.close();

        assertInstanceOf(meta.columns().get(0).valueClass(), row.value(0));
        assertInstanceOf(meta.columns().get(1).valueClass(), row.value(1));
    }

    @Test
    public void sqlRow() {
        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().build();

        ResultSet<SqlRow> rs = executeForRead(ses, "SELECT 1 as COL_A, 2 as COL_B");

        SqlRow r = rs.next();

        assertEquals(2, r.columnCount());
        assertEquals(0, r.columnIndex("COL_A"));
        assertEquals(0, r.columnIndex("col_a"));
        assertEquals(1, r.columnIndex("COL_B"));
        assertEquals(-1, r.columnIndex("notExistColumn"));

        assertEquals(1, r.intValue("COL_A"));
        assertEquals(1, r.intValue("COL_a"));
        assertEquals(2, r.intValue("COL_B"));

        assertThrowsWithCause(
                () -> r.intValue("notExistColumn"),
                IllegalArgumentException.class,
                "Column doesn't exist [name=notExistColumn]"
        );

        assertEquals(1, r.intValue(0));
        assertEquals(2, r.intValue(1));
        assertThrowsWithCause(() -> r.intValue(-2), IndexOutOfBoundsException.class);
        assertThrowsWithCause(() -> r.intValue(10), IndexOutOfBoundsException.class);

        rs.close();
    }

    @Test
    public void closeSession() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        ResultSet rs = executeForRead(ses, "SELECT ID FROM TEST");

        ses.close();

        IgniteTestUtils.assertThrowsWithCode(
                CursorClosedException.class,
                Common.CURSOR_CLOSED_ERR,
                () -> rs.forEachRemaining(System.out::println),
                "Cursor is closed");

        assertThrowsSqlException(Sql.SESSION_CLOSED_ERR, "Session is closed", () -> execute(ses, "SELECT ID FROM TEST"));
    }

    @Test
    public void errors() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT NOT NULL)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        // Parse error.
        checkSqlError(Sql.STMT_PARSE_ERR, "Failed to parse query", ses, "SELECT ID FROM");

        // Validation errors.
        checkSqlError(Sql.STMT_VALIDATION_ERR, "Column 'VAL0' does not allow NULLs", ses,
                "INSERT INTO TEST VALUES (2, NULL)");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Object 'NOT_EXISTING_TABLE' not found", ses,
                "SELECT * FROM NOT_EXISTING_TABLE");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Column 'NOT_EXISTING_COLUMN' not found", ses,
                "SELECT NOT_EXISTING_COLUMN FROM TEST");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Multiple statements are not allowed", ses, "SELECT 1; SELECT 2");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported", ses,
                "CREATE TABLE TEST2 (VAL INT)");

        // Execute error.
        checkSqlError(Sql.RUNTIME_ERR, "Division by zero", ses, "SELECT 1 / ?", 0);
        checkSqlError(Sql.RUNTIME_ERR, "Division by zero", ses, "UPDATE TEST SET val0 = val0/(val0 - ?) + " + ROW_COUNT, 0);
        checkSqlError(Sql.RUNTIME_ERR, "negative substring length not allowed", ses, "SELECT SUBSTRING('foo', 1, -3)");

        // No result set error.
        {
            ResultSet rs = executeForRead(ses, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)");
            assertThrowsSqlException(
                    NoRowSetExpectedException.class,
                    Sql.QUERY_NO_RESULT_SET_ERR, "Query has no result set",
                    () -> rs.next());
        }

        // Cursor closed error.
        {
            ResultSet rs = executeForRead(ses, "SELECT * FROM TEST");
            Thread.sleep(300); // ResultSetImpl fetches next page in background, wait to it to complete to avoid flakiness.
            rs.close();

            IgniteTestUtils.assertThrowsWithCode(
                    CursorClosedException.class,
                    Common.CURSOR_CLOSED_ERR,
                    () -> rs.forEachRemaining(Object::hashCode),
                    "Cursor is closed");
        }
    }

    @Test
    public void dml() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        TxManager txManager = txManager();

        int txPrevCnt = txManager.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        assertEquals(ROW_COUNT, txManager.finished() - txPrevCnt);
        // No new transactions through ddl.
        assertEquals(0, txManager.pending());

        checkDml(ROW_COUNT, ses, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, ses, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    @Test
    public void select() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 4).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            ses.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        ResultProcessor resultProcessor = execute(4, ses, "SELECT ID FROM TEST");

        Set<Integer> rs = resultProcessor.result().stream().map(r -> r.intValue(0)).collect(Collectors.toSet());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(rs.remove(i), "Results invalid: " + resultProcessor.result());
        }

        assertTrue(rs.isEmpty());
    }

    @Test
    public void batch() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = CLUSTER.aliveNode().sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        BatchedArguments args = BatchedArguments.of(0, 0);

        for (int i = 1; i < ROW_COUNT; ++i) {
            args.add(i, i);
        }

        long[] batchRes = executeBatch(ses, "INSERT INTO TEST VALUES (?, ?)", args);

        Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));

        // Check that data are inserted OK
        List<List<Object>> res = sql("SELECT ID FROM TEST ORDER BY ID");
        IntStream.range(0, ROW_COUNT).forEach(i -> assertEquals(i, res.get(i).get(0)));

        // Check invalid query type
        assertThrowsSqlException(
                SqlBatchException.class,
                Sql.STMT_VALIDATION_ERR,
                "Invalid SQL statement type",
                () -> executeBatch(ses, "SELECT * FROM TEST", args));

        assertThrowsSqlException(
                SqlBatchException.class,
                Sql.STMT_VALIDATION_ERR,
                "Invalid SQL statement type",
                () -> executeBatch(ses, "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args));
    }

    @Test
    public void batchIncomplete() {
        int err = ROW_COUNT / 2;

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = CLUSTER.aliveNode().sql();
        Session ses = sql.sessionBuilder().defaultPageSize(ROW_COUNT / 2).build();

        BatchedArguments args = BatchedArguments.of(0, 0);

        for (int i = 1; i < ROW_COUNT; ++i) {
            if (i == err) {
                args.add(1, 1);
            } else {
                args.add(i, i);
            }
        }

        SqlBatchException ex = assertThrowsSqlException(
                SqlBatchException.class,
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> executeBatch(ses, "INSERT INTO TEST VALUES (?, ?)", args)
        );

        assertEquals(err, ex.updateCounters().length);
        IntStream.range(0, ex.updateCounters().length).forEach(i -> assertEquals(1, ex.updateCounters()[i]));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO tst VALUES (2, ?)",
            "SELECT * FROM tst WHERE id = ? "
    })
    public void runtimeErrorInDmlCausesTransactionToFail(String query) {
        sql("CREATE TABLE tst(id INTEGER PRIMARY KEY, val INTEGER)");

        sql("INSERT INTO tst VALUES (?,?)", 1, 1);

        try (Session ses = igniteSql().createSession()) {
            Transaction tx = igniteTx().begin();
            String dmlQuery = "UPDATE tst SET val = val/(val - ?) + 1";

            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "Division by zero",
                    () -> execute(tx, ses, dmlQuery, 1).affectedRows());

            IgniteException err = assertThrows(IgniteException.class, () -> {
                ResultSet<SqlRow> rs = executeForRead(ses, tx, query, 2);
                if (rs.hasRowSet()) {
                    assertTrue(rs.hasNext());
                } else {
                    assertTrue(rs.wasApplied());
                }
            });

            assertEquals(Transactions.TX_ALREADY_FINISHED_ERR, err.code(), err.toString());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO tst VALUES (2, ?)",
            "SELECT * FROM tst WHERE id = ? "
    })
    public void runtimeErrorInQueryCausesTransactionToFail(String query) {
        sql("CREATE TABLE tst(id INTEGER PRIMARY KEY, val INTEGER)");

        sql("INSERT INTO tst VALUES (?,?)", 1, 1);

        try (Session ses = igniteSql().createSession()) {
            Transaction tx = igniteTx().begin();

            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "Division by zero",
                    () -> execute(tx, ses, "SELECT val/? FROM tst WHERE id=?", 0, 1));

            IgniteException err = assertThrows(IgniteException.class, () -> {
                ResultSet<SqlRow> rs = executeForRead(ses, tx, query, 2);
                if (rs.hasRowSet()) {
                    assertTrue(rs.hasNext());
                } else {
                    assertTrue(rs.wasApplied());
                }
            });

            assertEquals(Transactions.TX_ALREADY_FINISHED_ERR, err.code(), err.toString());
        }
    }

    @Test
    public void testLockIsNotReleasedAfterTxRollback() {
        IgniteSql sql = igniteSql();

        try (Session ses = sql.createSession()) {
            checkDdl(true, ses, "CREATE TABLE IF NOT EXISTS tst(id INTEGER PRIMARY KEY, val INTEGER)");
        }

        try (Session session = sql.createSession()) {
            Transaction tx = igniteTx().begin();

            assertThrows(RuntimeException.class, () -> execute(tx, session, "SELECT 1/0"));

            tx.rollback();

            assertThrowsSqlException(
                    Transactions.TX_ALREADY_FINISHED_ERR,
                    "Transaction is already finished",
                    () -> session.execute(tx, "INSERT INTO tst VALUES (1, 1)")
            );
        }

        try (Session session = sql.createSession()) {
            Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(false));

            execute(tx, session, "INSERT INTO tst VALUES (1, 1)");
            tx.commit();
        }
    }

    @Test
    public void resultSetFullReadShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        // Fetch all data in one read.
        Session ses = sql.sessionBuilder().defaultPageSize(100).build();
        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        execute(1, ses, "SELECT * FROM TEST");

        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    /**
     * DDL is non-transactional.
     */
    @Test
    public void ddlInTransaction() {
        Session ses = igniteSql().createSession();
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        {
            Transaction tx = igniteTx().begin();
            try {
                assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "DDL doesn't support transactions.",
                        () -> execute(tx, ses, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")
                );
            } finally {
                tx.rollback();
            }
        }
        {
            Transaction tx = igniteTx().begin();
            ResultProcessor result = execute(tx, ses, "INSERT INTO TEST VALUES (?, ?)", -1, -1);
            assertEquals(1, result.affectedRows());

            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "DDL doesn't support transactions.",
                    () -> ses.execute(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")
            );
            tx.commit();

            assertEquals(1, execute(ses, "SELECT ID FROM TEST WHERE ID = -1").result().size());
        }

        assertEquals(0, txManager().pending());
    }

    @Test
    public void resultSetCloseShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();
        Session ses = sql.sessionBuilder().defaultPageSize(2).build();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(ses, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        ResultSet<?> rs = executeForRead(ses, "SELECT * FROM TEST");
        assertEquals(1, txManager().pending());
        rs.close();
        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    @Test
    public void runScriptThatCompletesSuccessfully() {
        IgniteSql sql = igniteSql();

        try (Session session = sql.createSession()) {
            executeScript(session,
                    "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                            + "INSERT INTO test VALUES(1, 0); "
                            + "UPDATE test SET step = 1; "
                            + "SELECT * FROM test; "
                            + "UPDATE test SET step = 2; ");

            ResultProcessor result = execute(session, "SELECT step FROM test");
            assertEquals(1, result.result().size());
            assertEquals(2, result.result().get(0).intValue(0));
        }
    }

    @Test
    public void runScriptWithTransactionThatCompletesSuccessfully() {
        IgniteSql sql = igniteSql();

        try (Session session = sql.createSession()) {
            executeScript(session,
                    "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                            + "START TRANSACTION; "
                            + "INSERT INTO test VALUES(1, 0); "
                            + "INSERT INTO test VALUES(2, 0); "
                            + "UPDATE test SET step = 1; "
                            + "SELECT * FROM test; "
                            + "UPDATE test SET step = 2; "
                            + "COMMIT; "
                            + "DELETE FROM test WHERE id = 2");

            ResultProcessor result = execute(session, "SELECT step FROM test");
            assertEquals(1, result.result().size());
            assertEquals(2, result.result().get(0).intValue(0));
        }
    }

    @Test
    public void runScriptThatFails() {
        IgniteSql sql = igniteSql();

        try (Session session = sql.createSession()) {
            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "Division by zero",
                    () -> executeScript(session,
                            "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                                    + "INSERT INTO test VALUES(1, 0); "
                                    + "UPDATE test SET step = 1; "
                                    + "UPDATE test SET step = 3 WHERE step > 1/0; "
                                    + "UPDATE test SET step = 2; "
                    )
            );

            ResultProcessor result = execute(session, "SELECT step FROM test");
            assertEquals(1, result.result().size());
            assertEquals(1, result.result().get(0).intValue(0));
        }
    }

    protected ResultSet<SqlRow> executeForRead(Session ses, String query, Object... args) {
        return executeForRead(ses, null, query, args);
    }

    protected abstract ResultSet<SqlRow> executeForRead(Session ses, @Nullable Transaction tx, String query, Object... args);

    protected <T extends IgniteException> T checkError(Class<T> expCls, Integer code, String msg, Session ses, String sql,
            Object... args) {
        T ex = assertThrows(expCls, () -> execute(ses, sql, args));

        if (code != null) {
            assertEquals(new IgniteException(code).codeAsString(), ex.codeAsString());
        }

        if (msg != null) {
            assertThat(ex.getMessage(), containsString(msg));
        }

        return ex;
    }

    protected SqlException checkSqlError(
            int code,
            String msg,
            Session ses,
            String sql,
            Object... args
    ) {
        return assertThrowsSqlException(code, msg, () -> execute(ses, sql, args));
    }

    protected abstract long[] executeBatch(Session ses, String sql, BatchedArguments args);

    protected abstract ResultProcessor execute(Integer expectedPages, Transaction tx, Session ses, String sql, Object... args);

    protected ResultProcessor execute(int expectedPages, Session ses, String sql, Object... args) {
        return execute(expectedPages, null, ses, sql, args);
    }

    protected ResultProcessor execute(Transaction tx, Session ses, String sql, Object... args) {
        return execute(null, tx, ses, sql, args);
    }

    protected ResultProcessor execute(Session ses, String sql, Object... args) {
        return execute(null, null, ses, sql, args);
    }

    protected abstract void executeScript(Session ses, String sql, Object... args);

    protected abstract void rollback(Transaction outerTx);

    protected abstract void commit(Transaction outerTx);

    protected void checkDml(int expectedAffectedRows, Transaction tx, Session ses, String sql, Object... args) {

    }

    protected void checkDml(int expectedAffectedRows, Session ses, String sql, Object... args) {
        checkDml(expectedAffectedRows, null, ses, sql, args);
    }

    protected void checkDdl(boolean expectedApplied, Session ses, String sql) {
        checkDdl(expectedApplied, ses, sql, null);
    }

    protected abstract void checkDdl(boolean expectedApplied, Session ses, String sql, Transaction tx);

    /** Represent result of running SQL query to hide implementation specific for different version of tests. */
    protected interface ResultProcessor {
        List<SqlRow> result();

        long affectedRows();
    }

    /**
     * Gets client connector addresses for the specified nodes.
     *
     * @param nodes Nodes.
     * @return List of client addresses.
     */
    static List<String> getClientAddresses(List<Ignite> nodes) {
        return nodes.stream()
                .map(ignite -> ((IgniteImpl) ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }
}
