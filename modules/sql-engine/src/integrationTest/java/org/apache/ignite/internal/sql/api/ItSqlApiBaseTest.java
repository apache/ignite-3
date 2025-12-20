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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScanIgnoreBounds;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.asStream;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.expectQueryCancelled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.fsm.QueryInfo;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.CancelHandle;
import org.apache.ignite.lang.CancellationToken;
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
import org.apache.ignite.sql.SqlBatchException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AssertionFailureBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
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
    public void dropTablesAndSchemas() {
        dropAllTables();
        dropAllSchemas();
    }

    @Test
    public void ddl() {
        IgniteSql sql = igniteSql();

        // CREATE TABLE
        checkDdl(true, sql, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.TEST' already exists",
                sql,
                "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)"
        );
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL' specified more than once",
                sql,
                "CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL INT, VAL INT)"
        );
        checkDdl(false, sql, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

        // ADD COLUMN
        checkDdl(true, sql, "ALTER TABLE TEST ADD COLUMN VAL1 VARCHAR");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.NOT_EXISTS_TABLE' not found",
                sql,
                "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"
        );
        checkDdl(false, sql, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL1' already exists",
                sql,
                "ALTER TABLE TEST ADD COLUMN VAL1 INT"
        );

        // CREATE INDEX
        checkDdl(true, sql, "CREATE INDEX TEST_IDX ON TEST(VAL0)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Index with name 'PUBLIC.TEST_IDX' already exists",
                sql,
                "CREATE INDEX TEST_IDX ON TEST(VAL1)"
        );
        checkDdl(false, sql, "CREATE INDEX IF NOT EXISTS TEST_IDX ON TEST(VAL1)");

        checkDdl(true, sql, "DROP INDEX TESt_iDX");
        checkDdl(true, sql, "CREATE INDEX TEST_IDX1 ON TEST(VAL0)");
        checkDdl(true, sql, "CREATE INDEX TEST_IDX2 ON TEST(VAL0)");
        checkDdl(true, sql, "CREATE INDEX TEST_IDX3 ON TEST(ID, VAL0, VAL1)");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL0' specified more than once",
                sql,
                "CREATE INDEX TEST_IDX4 ON TEST(VAL0, VAL0)"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column 'VAL1' used by index(es) [TEST_IDX3], it is not allowed",
                sql,
                "ALTER TABLE TEST DROP COLUMN val1"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column 'VAL0' used by index(es) [TEST_IDX1, TEST_IDX2, TEST_IDX3], it is not allowed",
                sql,
                "ALTER TABLE TEST DROP COLUMN (val0, val1)"
        );

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Deleting column `ID` belonging to primary key is not allowed",
                sql,
                "ALTER TABLE TEST DROP COLUMN id"
        );

        checkDdl(true, sql, "DROP INDEX TESt_iDX3");

        // DROP COLUMNS
        checkDdl(true, sql, "ALTER TABLE TEST DROP COLUMN VAL1");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.NOT_EXISTS_TABLE' not found",
                sql,
                "ALTER TABLE NOT_EXISTS_TABLE DROP COLUMN VAL1"
        );
        checkDdl(false, sql, "ALTER TABLE IF EXISTS NOT_EXISTS_TABLE DROP COLUMN VAL1");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Column with name 'VAL1' not found in table 'PUBLIC.TEST'",
                sql,
                "ALTER TABLE TEST DROP COLUMN VAL1"
        );

        // DROP TABLE
        checkDdl(false, sql, "DROP TABLE IF EXISTS NOT_EXISTS_TABLE");

        checkDdl(true, sql, "DROP TABLE TEST");
        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.TEST' not found",
                sql,
                "DROP TABLE TEST"
        );

        checkDdl(false, sql, "DROP INDEX IF EXISTS TEST_IDX");

        checkSqlError(
                Sql.STMT_VALIDATION_ERR,
                "Index with name 'PUBLIC.TEST_IDX' not found",
                sql,
                "DROP INDEX TEST_IDX"
        );
    }

    /** Check all transactions are processed correctly even with case of sql Exception raised. */
    @Test
    public void implicitTransactionsStates() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        TxManager txManager = txManager();

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Table with name 'PUBLIC.TEST' already exists",
                    () -> execute(sql, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)")
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

        TxManager txManagerInternal = txManager();

        int txPrevCnt = txManagerInternal.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = ROW_COUNT; i < 2 * ROW_COUNT; ++i) {
            checkDml(1, outerTx, sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        commit(outerTx);

        // Outdated tx.
        Transaction outerTx0 = outerTx;
        assertThrowsSqlException(
                Transactions.TX_ALREADY_FINISHED_ERR,
                "Transaction is already finished",
                () -> checkDml(1, outerTx0, sql, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> checkDml(1, sql, "INSERT INTO TEST VALUES (?, ?)", ROW_COUNT, Integer.MAX_VALUE));

        ResultSet<SqlRow> rs = executeForRead(sql, "SELECT VAL0 FROM TEST ORDER BY VAL0");

        assertEquals(2 * ROW_COUNT, asStream(rs).count());

        rs.close();

        outerTx = igniteTx().begin();

        rs = executeForRead(sql, outerTx, "SELECT VAL0 FROM TEST ORDER BY VAL0");

        assertEquals(2 * ROW_COUNT, asStream(rs).count());

        rs.close();

        outerTx.commit();

        checkDml(2 * ROW_COUNT, sql, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(2 * ROW_COUNT, sql, "DELETE FROM TEST WHERE VAL0 >= 0");

        assertEquals(ROW_COUNT + 1 + 1 + 1 + 1 + 1 + 1, txManagerInternal.finished() - txPrevCnt);

        assertEquals(0, txManagerInternal.pending());
    }

    /** Check correctness of explicit transaction rollback. */
    @Test
    public void checkExplicitTxRollback() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        // Outer tx with further commit.
        Transaction outerTx = igniteTx().begin();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, outerTx, sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        rollback(outerTx);

        ResultSet<SqlRow> rs = executeForRead(sql, "SELECT VAL0 FROM TEST ORDER BY VAL0");

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

        Matcher<String> planMatcher = containsIndexScanIgnoreBounds("PUBLIC", "TEST", "TEST_IDX");

        checkMixedTransactions(planMatcher);
    }

    private void checkMixedTransactions(Matcher<String> planMatcher) {
        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            executeForRead(sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        List<Boolean> booleanList = List.of(Boolean.TRUE, Boolean.FALSE);
        for (boolean roTx : booleanList) {
            for (boolean commit : booleanList) {
                for (boolean explicit : booleanList) {
                    checkTx(sql, roTx, commit, explicit, planMatcher);
                }
            }
        }
    }

    private void checkTx(IgniteSql sql, boolean readOnly, boolean commit, boolean explicit, Matcher<String> planMatcher) {
        Transaction outerTx = explicit ? igniteTx().begin(new TransactionOptions().readOnly(readOnly)) : null;

        String queryRo = "SELECT VAL0 FROM TEST ORDER BY VAL0";

        assertQuery(queryRo).matches(planMatcher).check();

        ResultSet<SqlRow> rs = executeForRead(sql, outerTx, queryRo);

        assertEquals(ROW_COUNT, asStream(rs).count());

        rs.close();

        String queryRw = "UPDATE TEST SET VAL0=VAL0+1";
        if (explicit && readOnly) {
            assertThrowsSqlException(Sql.RUNTIME_ERR, "DML cannot be started by using read only transactions.",
                    () -> execute(outerTx, sql, queryRw));
        } else {
            checkDml(ROW_COUNT, outerTx, sql, queryRw);
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
        sql("CREATE TABLE TEST(COL0 BIGINT PRIMARY KEY, \"Col1\" VARCHAR NOT NULL)");

        IgniteSql sql = igniteSql();

        execute(sql, "INSERT INTO TEST VALUES (?, ?)", 1L, "some string");

        ResultSet<SqlRow> rs = executeForRead(sql, "SELECT \"Col1\", COL0 FROM TEST");

        // Validate columns metadata.
        ResultSetMetadata meta = rs.metadata();

        assertNotNull(meta);
        assertEquals(-1, meta.indexOf("COL"));
        assertEquals(0, meta.indexOf("\"Col1\""));
        assertEquals(1, meta.indexOf("COL0"));

        assertEquals(0, meta.indexOf(meta.columns().get(0).name()));
        assertEquals(1, meta.indexOf(meta.columns().get(1).name()));

        checkMetadata(new ColumnMetadataImpl(
                        "Col1",
                        ColumnType.STRING,
                        CatalogUtils.DEFAULT_VARLEN_LENGTH,
                        ColumnMetadata.UNDEFINED_SCALE,
                        false,
                        new ColumnOriginImpl("PUBLIC", "TEST", "Col1")),
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

        ResultSet<SqlRow> rs = executeForRead(sql, "SELECT 1 as COL_A, 2 as COL_B");

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
    public void errors() throws InterruptedException {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT NOT NULL)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        // Parse error.
        checkSqlError(Sql.STMT_PARSE_ERR, "Failed to parse query", sql, "SELECT ID FROM");

        // Validation errors.
        checkSqlError(Sql.CONSTRAINT_VIOLATION_ERR, "Column 'VAL0' does not allow NULLs", sql,
                "INSERT INTO TEST VALUES (2, NULL)");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Object 'NOT_EXISTING_TABLE' not found", sql,
                "SELECT * FROM NOT_EXISTING_TABLE");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Column 'NOT_EXISTING_COLUMN' not found", sql,
                "SELECT NOT_EXISTING_COLUMN FROM TEST");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Multiple statements are not allowed", sql, "SELECT 1; SELECT 2");

        checkSqlError(Sql.STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported", sql,
                "CREATE TABLE TEST2 (VAL INT)");

        // Execute error.
        checkSqlError(Sql.RUNTIME_ERR, "Division by zero", sql, "SELECT 1 / ?", 0);
        checkSqlError(Sql.RUNTIME_ERR, "Division by zero", sql, "UPDATE TEST SET val0 = val0/(val0 - ?) + " + ROW_COUNT, 0);
        checkSqlError(Sql.RUNTIME_ERR, "negative substring length not allowed", sql, "SELECT SUBSTRING('foo', 1, -3)");

        // No result set error.
        {
            ResultSet rs = executeForRead(sql, "CREATE TABLE TEST3 (ID INT PRIMARY KEY)");
            assertThrowsSqlException(
                    NoRowSetExpectedException.class,
                    Sql.QUERY_NO_RESULT_SET_ERR, "Query has no result set",
                    () -> rs.next());
        }

        // Cursor closed error.
        {
            Statement statement = sql.statementBuilder()
                    .query("SELECT * FROM TEST")
                    .pageSize(2)
                    .build();

            ResultSet rs = executeForRead(sql, statement);
            Thread.sleep(300); // ResultSetImpl fetches next page in background, wait to it to complete to avoid flakiness.
            rs.close();

            assertThrowsWithCode(
                    CursorClosedException.class,
                    Common.CURSOR_ALREADY_CLOSED_ERR,
                    () -> rs.forEachRemaining(Object::hashCode),
                    "Cursor is closed");
        }
    }

    @Test
    public void dml() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        TxManager txManager = txManager();

        int txPrevCnt = txManager.finished();

        for (int i = 0; i < ROW_COUNT; ++i) {
            checkDml(1, sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        assertEquals(ROW_COUNT, txManager.finished() - txPrevCnt);
        // No new transactions through ddl.
        assertEquals(0, txManager.pending());

        checkDml(ROW_COUNT, sql, "UPDATE TEST SET VAL0 = VAL0 + ?", 1);

        checkDml(ROW_COUNT, sql, "DELETE FROM TEST WHERE VAL0 >= 0");
    }

    @Test
    public void select() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            sql.execute(null, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        Statement statement = sql.statementBuilder()
                .query("SELECT ID FROM TEST")
                .pageSize(ROW_COUNT / 4)
                .build();

        ResultProcessor resultProcessor = execute(4, null, sql, statement);

        Set<Integer> rs = resultProcessor.result().stream().map(r -> r.intValue(0)).collect(Collectors.toSet());

        for (int i = 0; i < ROW_COUNT; ++i) {
            assertTrue(rs.remove(i), "Results invalid: " + resultProcessor.result());
        }

        assertTrue(rs.isEmpty());
    }

    @Test
    public void batch() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        // Execute batch using query.
        {
            BatchedArguments args = BatchedArguments.of(0, 0);

            for (int i = 1; i < ROW_COUNT; ++i) {
                args.add(i, i);
            }

            long[] batchRes = executeBatch("INSERT INTO TEST VALUES (?, ?)", args);

            Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));
        }

        // Execute batch using statement.
        {
            BatchedArguments args = BatchedArguments.of(ROW_COUNT, ROW_COUNT);

            for (int i = ROW_COUNT + 1; i < ROW_COUNT * 2; ++i) {
                args.add(i, i);
            }

            Statement statement = igniteSql().createStatement("INSERT INTO TEST VALUES (?, ?)");

            long[] batchRes = executeBatch(statement, args);

            Arrays.stream(batchRes).forEach(r -> assertEquals(1L, r));
        }

        // Check that data are inserted OK
        List<SqlRow> res = execute(igniteSql(), "SELECT ID FROM TEST ORDER BY ID").result();
        IntStream.range(0, ROW_COUNT * 2).forEach(i -> assertEquals(i, res.get(i).intValue((0))));

        BatchedArguments args = BatchedArguments.of(-1, -1);

        // Check invalid query type
        assertThrowsSqlException(
                SqlBatchException.class,
                Sql.STMT_VALIDATION_ERR,
                "Statement of type \"Query\" is not allowed in current context",
                () -> executeBatch("SELECT * FROM TEST", args));

        assertThrowsSqlException(
                SqlBatchException.class,
                Sql.STMT_VALIDATION_ERR,
                "Statement of type \"DDL\" is not allowed in current context",
                () -> executeBatch("CREATE TABLE TEST1(ID INT PRIMARY KEY, VAL0 INT)", args));

        // Check that statement parameters taken into account.
        Statement statement = igniteSql().statementBuilder()
                .defaultSchema("NON_EXISTING_SCHEMA")
                .query("INSERT INTO TEST VALUES (?, ?)")
                .build();

        assertThrowsSqlException(
                SqlBatchException.class,
                Sql.STMT_VALIDATION_ERR,
                "Object 'TEST' not found",
                () -> executeBatch(statement, args));
    }

    @Test
    public void batchIncomplete() {
        int err = ROW_COUNT / 2;

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

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
                () -> executeBatch("INSERT INTO TEST VALUES (?, ?)", args)
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

        IgniteSql sql = igniteSql();

        Transaction tx = igniteTx().begin();
        String dmlQuery = "UPDATE tst SET val = val/(val - ?) + 1";

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Division by zero",
                () -> execute(tx, sql, dmlQuery, 1).affectedRows());

        assertThrowsWithCode(
                IgniteException.class,
                Transactions.TX_ALREADY_FINISHED_ERR,
                () -> executeForRead(sql, tx, query, 2),
                "Transaction is already finished");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO tst VALUES (2, ?)",
            "SELECT * FROM tst WHERE id = ? "
    })
    public void runtimeErrorInQueryCausesTransactionToFail(String query) {
        sql("CREATE TABLE tst(id INTEGER PRIMARY KEY, val INTEGER)");

        sql("INSERT INTO tst VALUES (?,?)", 1, 1);

        IgniteSql sql = igniteSql();

        Transaction tx = igniteTx().begin();

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Division by zero",
                () -> execute(tx, sql, "SELECT val/? FROM tst WHERE id=?", 0, 1));

        assertThrowsWithCode(
                IgniteException.class,
                Transactions.TX_ALREADY_FINISHED_ERR,
                () -> executeForRead(sql, tx, query, 2),
                "Transaction is already finished");
    }

    @Test
    public void testLockIsNotReleasedAfterTxRollback() {
        IgniteSql sql = igniteSql();

        checkDdl(true, sql, "CREATE TABLE IF NOT EXISTS tst(id INTEGER PRIMARY KEY, val INTEGER)");

        {
            Transaction tx = igniteTx().begin();

            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "Division by zero",
                    () -> execute(tx, sql, "SELECT 1/0"));

            tx.rollback();

            assertThrowsSqlException(
                    Transactions.TX_ALREADY_FINISHED_ERR,
                    "Transaction is already finished",
                    () -> sql.execute(tx, "INSERT INTO tst VALUES (1, 1)")
            );
        }

        {
            Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(false));

            execute(tx, sql, "INSERT INTO tst VALUES (1, 1)");
            tx.commit();
        }
    }

    @Test
    public void resultSetFullReadShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        execute(1, sql, "SELECT * FROM TEST");

        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    /**
     * DDL is non-transactional.
     */
    @Test
    public void ddlInTransaction() {
        IgniteSql sql = igniteSql();
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        {
            Transaction tx = igniteTx().begin();
            try {
                assertThrowsSqlException(
                        Sql.RUNTIME_ERR,
                        "DDL doesn't support transactions.",
                        () -> execute(tx, sql, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")
                );
            } finally {
                tx.rollback();
            }
        }
        {
            Transaction tx = igniteTx().begin();
            ResultProcessor result = execute(tx, sql, "INSERT INTO TEST VALUES (?, ?)", -1, -1);
            assertEquals(1, result.affectedRows());

            assertThrowsSqlException(
                    Sql.RUNTIME_ERR,
                    "DDL doesn't support transactions.",
                    () -> sql.execute(tx, "CREATE TABLE TEST2(ID INT PRIMARY KEY, VAL0 INT)")
            );
            tx.commit();

            assertEquals(1, execute(sql, "SELECT ID FROM TEST WHERE ID = -1").result().size());
        }

        assertEquals(0, txManager().pending());
    }

    @Test
    public void resultSetCloseShouldFinishImplicitTransaction() {
        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        IgniteSql sql = igniteSql();

        for (int i = 0; i < ROW_COUNT; ++i) {
            execute(sql, "INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        Statement statement = sql.statementBuilder()
                .query("SELECT * FROM TEST")
                .pageSize(2)
                .build();

        ResultSet<?> rs = executeForRead(sql, statement);
        assertEquals(1, txManager().pending());
        rs.close();
        assertEquals(0, txManager().pending(), "Expected no pending transactions");
    }

    @Test
    public void runScriptThatCompletesSuccessfully() {
        IgniteSql sql = igniteSql();

        executeScript(sql,
                "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                        + "INSERT INTO test VALUES(1, 0); "
                        + "UPDATE test SET step = 1; "
                        + "SELECT * FROM test; "
                        + "UPDATE test SET step = 2; ");

        ResultProcessor result = execute(sql, "SELECT step FROM test");
        assertEquals(1, result.result().size());
        assertEquals(2, result.result().get(0).intValue(0));
    }

    @Test
    public void runScriptWithTransactionThatCompletesSuccessfully() {
        IgniteSql sql = igniteSql();

        executeScript(sql,
                "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                        + "START TRANSACTION; "
                        + "INSERT INTO test VALUES(1, 0); "
                        + "INSERT INTO test VALUES(2, 0); "
                        + "UPDATE test SET step = 1; "
                        + "SELECT * FROM test; "
                        + "UPDATE test SET step = 2; "
                        + "COMMIT; "
                        + "DELETE FROM test WHERE id = 2");

        ResultProcessor result = execute(sql, "SELECT step FROM test");
        assertEquals(1, result.result().size());
        assertEquals(2, result.result().get(0).intValue(0));
    }

    @Test
    public void runScriptThatFails() {
        IgniteSql sql = igniteSql();

        assertThrowsSqlException(
                Sql.RUNTIME_ERR,
                "Division by zero",
                () -> executeScript(sql,
                        "CREATE TABLE test (id INT PRIMARY KEY, step INTEGER); "
                                + "INSERT INTO test VALUES(1, 0); "
                                + "UPDATE test SET step = 1; "
                                + "UPDATE test SET step = 3 WHERE step > 1/0; "
                                + "UPDATE test SET step = 2; "
                )
        );

        ResultProcessor result = execute(sql, "SELECT step FROM test");
        assertEquals(1, result.result().size());
        assertEquals(1, result.result().get(0).intValue(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "UTC", "Europe/Athens", "America/New_York", "Asia/Tokyo"})
    public void testTimeZoneId(String timeZoneId) {
        ZoneId zoneId = timeZoneId.isEmpty() ? ZoneId.systemDefault() : ZoneId.of(timeZoneId);

        StatementBuilder builder = igniteSql().statementBuilder()
                .query("SELECT CURRENT_TIMESTAMP")
                .timeZoneId(zoneId);

        long momentBefore = Instant.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        ResultSet<SqlRow> resultSet = igniteSql().execute(null, builder.build());
        SqlRow row = resultSet.next();

        long momentAfter = Instant.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        Instant ts = row.value(0);
        assertNotNull(ts);

        long tsMillis = ts.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        assertTrue(momentBefore <= tsMillis && momentAfter >= tsMillis);
    }

    @Test
    public void testEarlyQueryTimeout() {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(1, TimeUnit.MILLISECONDS)
                .build();

        // Do not have enough time to do anything.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            executeForRead(igniteSql(), stmt);
        });
    }

    @Test
    public void testQueryTimeout() {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(100, TimeUnit.MILLISECONDS)
                .build();

        // Run ignoring any timeout util we get some result.
        ResultSet<SqlRow> resultSet = runIgnoringExecutionErrors(stmt);
        assertNotNull(resultSet);

        // Read data until timeout exception occurs.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            while (resultSet.hasNext()) {
                resultSet.next();
            }
        });
    }

    @Test
    public void cancelScript() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE test (id INT PRIMARY KEY);");

        // DML is used because the cursor will be closed as soon as the first page is ready.
        String script =
                "INSERT INTO test SELECT x FROM system_range(0, 10000000000);"
                + "SELECT 1;";

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        CompletableFuture<Void> scriptFut = IgniteTestUtils.runAsync(() -> executeScript(sql, token, script));

        // Wait until FIRST script statement is started to execute.
        waitUntilRunningQueriesCount(greaterThan(1));
        // We have to make sure that execution is started.
        waitUntilActiveTransactionsCount(is(1));

        assertThat(scriptFut.isDone(), is(false));

        cancelHandle.cancel();

        expectQueryCancelled(() -> await(scriptFut));

        waitUntilRunningQueriesCount(is(0));
        assertThat(txManager().pending(), is(0));

        // Checks the exception that is thrown if a query is canceled before a cursor is obtained.
        expectQueryCancelled(() -> executeScript(sql, token, "SELECT 1; SELECT 2;"));

        waitUntilRunningQueriesCount(is(0));
        assertThat(txManager().pending(), is(0));
    }

    @Test
    public void cancelLongRunningStatement() {
        IgniteSql sql = igniteSql();

        sql("CREATE TABLE test (id INT PRIMARY KEY)");

        // Long running DML query uses implicit RW transaction.
        String query = "INSERT INTO test SELECT x FROM system_range(0, 10000000000);";

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        // Run long DML query.

        CompletableFuture<?> f = IgniteTestUtils.runAsync(() -> execute(sql, null, token, query));

        // Wait until the query starts executing.
        waitUntilQueriesInCursorPublicationPhaseCount(greaterThan(0));

        // Wait for query cancel.
        cancelHandle.cancel();

        // Query was actually cancelled.
        waitUntilRunningQueriesCount(is(0));
        expectQueryCancelled(() -> await(f));
        assertThat(txManager().pending(), is(0));
    }

    @ParameterizedTest
    @EnumSource(value = JoinRelType.class, mode = Mode.INCLUDE, names = {"INNER", "LEFT", "RIGHT", "FULL"})
    public void cancelLongRunningJoinStatement(JoinRelType joinType) throws InterruptedException {
        IgniteSql sql = igniteSql();

        sql.executeScript("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL INT)");
        for (int i = 0; i < 10; i++) {
            sql.executeScript("INSERT INTO TEST VALUES (?, ?)", i, i);
        }

        String join10x = String.format("SELECT count(*) FROM ("
                + "SELECT "
                + "t1.ID AS ID1, t2.ID AS ID2, t3.ID AS ID3, t4.ID AS ID4, t5.ID AS ID5, "
                + "t6.ID AS ID6, t7.ID AS ID7, t8.ID AS ID8, t9.ID AS ID9, t10.ID AS ID10 "
                + "FROM TEST t1 "
                + "%s JOIN TEST t2  ON 1 = 1 "
                + "%s JOIN TEST t3  ON 1 = 1 "
                + "%s JOIN TEST t4  ON 1 = 1 "
                + "%s JOIN TEST t5  ON 1 = 1 "
                + "%s JOIN TEST t6  ON 1 = 1 "
                + "%s JOIN TEST t7  ON 1 = 1 "
                + "%s JOIN TEST t8  ON 1 = 1 "
                + "%s JOIN TEST t9  ON 1 = 1 "
                + "%s JOIN TEST t10 ON 1 = 1 "
                + ")", joinType, joinType, joinType, joinType, joinType, joinType, joinType, joinType, joinType);

        {
            Statement statement = sql.statementBuilder()
                    .query(join10x)
                    .build();

            CancelHandle cancelHandle = CancelHandle.create();
            CompletableFuture<?> fut = sql.executeAsync(null, cancelHandle.token(), statement);

            // Wait until the query starts executing.
            waitUntilRunningQueriesCount(greaterThan(0));
            // Wait a bit more to improve failure rate.
            Thread.sleep(500);

            cancelHandle.cancel();

            // Query was actually cancelled.
            waitUntilRunningQueriesCount(is(0));
            expectQueryCancelled(() -> await(fut));
            assertThat(txManager().pending(), is(0));
        }
    }

    @Test
    public void testQueryTimeoutIsPropagatedFromTheServer() throws Exception {
        Statement stmt = igniteSql().statementBuilder()
                .query("SELECT * FROM TABLE(SYSTEM_RANGE(1, 1000000000000000))")
                .queryTimeout(100, TimeUnit.MILLISECONDS)
                .build();

        // Run ignoring any timeout to get some results.
        ResultSet<SqlRow> resultSet = runIgnoringExecutionErrors(stmt);

        assertTrue(resultSet.hasNext());
        assertNotNull(resultSet.next());

        // wait sometime until the time is right for a timeout to occur.
        // then start retrieving the remaining data to trigger timeout exception.
        TimeUnit.SECONDS.sleep(2);

        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            while (resultSet.hasNext()) {
                resultSet.next();
            }
        });
    }

    private ResultSet<SqlRow> runIgnoringExecutionErrors(Statement stmt) {
        SqlException lastError = null;

        for (int i = 0; i < 100; i++) {
            try {
                return executeForRead(igniteSql(), stmt);
            } catch (SqlException e) {
                // Ignore all execution cancelled error. We assume that all these errors are transient (timeouts),
                // and we will eventually get a result set.
                if (e.code() == Sql.EXECUTION_CANCELLED_ERR) {
                    lastError = e;
                    continue;
                }

                fail(e.getMessage());
            }
        }

        throw AssertionFailureBuilder.assertionFailure()
                .message("Failed to execute the statement without timeouts.")
                .cause(lastError)
                .build();
    }

    @Test
    public void testDdlTimeout() {
        IgniteSql igniteSql = igniteSql();
        int timeoutMillis = 1;

        Statement stmt = igniteSql.statementBuilder()
                .query("CREATE TABLE test (ID INT PRIMARY KEY, VAL0 INT)")
                .queryTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .build();

        // Trigger query timeout from the planner or the parser.
        assertThrowsSqlException(Sql.EXECUTION_CANCELLED_ERR, "Query timeout", () -> {
            executeForRead(igniteSql, stmt);
        });
    }

    @Test
    public void testKillCommand() {
        IgniteSql sql = igniteSql();

        try (ResultSet<SqlRow> rs = executeLazy(sql, null, "SELECT x FROM system_range(0, 100000)")) {
            assertThat(rs.hasNext(), is(true));

            List<QueryInfo> queries = queryProcessor().runningQueries();

            assertThat(queries, hasSize(1));

            UUID existingQuery = queries.get(0).id();

            String killQuery = "KILL QUERY '" + existingQuery + '\'';

            // Kill existing query.
            try (ResultSet<SqlRow> killResultset = sql.execute(null, killQuery)) {
                assertThat(killResultset.hasRowSet(), is(false));
                assertThat(killResultset.wasApplied(), is(true));
            }

            waitUntilRunningQueriesCount(is(0));

            assertThrowsSqlException(
                    Sql.EXECUTION_CANCELLED_ERR,
                    QueryCancelledException.CANCEL_MSG,
                    () -> {
                        while (rs.hasNext()) {
                            rs.next();
                        }
                    }
            );

            // Kill non-existing query.
            try (ResultSet<SqlRow> killResultset = sql.execute(null, killQuery)) {
                assertThat(killResultset.hasRowSet(), is(false));
                assertThat(killResultset.wasApplied(), is(false));
            }
        }
    }

    @Test
    public void useNonDefaultSchema() {
        IgniteSql sql = igniteSql();

        sql("CREATE SCHEMA schema1");
        sql("CREATE TABLE schema1.t1 (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO schema1.t1 VALUES (1, 1), (2, 2)");

        // Schema 2 has t1 as well

        sql("CREATE SCHEMA schema2");
        sql("CREATE TABLE schema2.t1 (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO schema2.t1 VALUES (1, 1), (2, 2), (3, 3)");

        {
            Statement stmt = sql.statementBuilder()
                    .query("SELECT COUNT(*) FROM schema1.t1")
                    .build();

            try (ResultSet<SqlRow> rs = executeForRead(sql, stmt)) {
                assertEquals(2, rs.next().longValue(0));
            }
        }

        {
            Statement stmt = sql.statementBuilder()
                    .defaultSchema("schema1")
                    .query("SELECT COUNT(*) FROM t1")
                    .build();

            try (ResultSet<SqlRow> rs = executeForRead(sql, stmt)) {
                assertEquals(2, rs.next().longValue(0));
            }
        }

        // Check schema 2

        {
            Statement stmt = sql.statementBuilder()
                    .defaultSchema("schema2")
                    .query(format("SELECT COUNT(*) FROM t1"))
                    .build();

            try (ResultSet<SqlRow> rs = executeForRead(sql, stmt)) {
                assertEquals(3, rs.next().longValue(0));
            }
        }
    }

    @Test
    public void useNonDefaultSchemaWithQuotedName() {
        IgniteSql sql = igniteSql();

        sql("CREATE SCHEMA schema1");
        sql("CREATE TABLE schema1.\"T 1\" (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO schema1.\"T 1\" VALUES (1, 1), (2, 2)");

        // Schema 2 has T1 as well

        sql("CREATE SCHEMA \"ScheMa1\"");
        sql("CREATE TABLE \"ScheMa1\".\"T 1\" (id INT PRIMARY KEY, val INT)");
        sql("INSERT INTO \"ScheMa1\".\"T 1\" VALUES (1, 1), (2, 2), (3, 3)");

        // Check schema 1

        {
            Statement stmt = sql.statementBuilder()
                    .defaultSchema("schema1")
                    .query("SELECT COUNT(*) FROM \"T 1\"")
                    .build();

            try (ResultSet<SqlRow> rs = executeForRead(sql, stmt)) {
                assertEquals(2, rs.next().longValue(0));
            }
        }

        // Check schema 2

        {
            Statement stmt = sql.statementBuilder()
                    .defaultSchema("\"ScheMa1\"")
                    .query(format("SELECT COUNT(*) FROM \"T 1\""))
                    .build();

            try (ResultSet<SqlRow> rs = executeForRead(sql, stmt)) {
                assertEquals(3, rs.next().longValue(0));
            }
        }
    }

    @Test
    public void rowToString() {
        SqlRow row = executeForRead(igniteSql(), "SELECT 1 as COL_A, '2' as COL_B").next();

        assertEquals(row.getClass().getSimpleName() + " [COL_A=1, COL_B=2]", row.toString());
    }

    @Test
    public abstract void cancelStatement() throws InterruptedException;

    @Test
    public abstract void cancelQueryString() throws InterruptedException;

    @Test
    public abstract void cancelBatch() throws InterruptedException;

    @Test
    public void cancelDdlScript() {
        IgniteSql sql = igniteSql();

        String script =
                "CREATE TABLE test1 (id INT PRIMARY KEY);"
                        + "CREATE TABLE test2 (id INT PRIMARY KEY);"
                        + "CREATE TABLE test3 (id INT PRIMARY KEY);";

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        CompletableFuture<Void> scriptFut = IgniteTestUtils.runAsync(() -> executeScript(sql, token, script));

        waitUntilRunningQueriesCount(greaterThan(0));

        cancelHandle.cancel();

        expectQueryCancelled(() -> await(scriptFut));

        waitUntilRunningQueriesCount(is(0));
    }

    @Test
    public void cancelQueryBeforeExecution() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        cancelHandle.cancel();

        expectQueryCancelled(() -> executeLazy(igniteSql(), token, "SELECT 1"));
    }

    @Test
    public void cancelMultipleQueriesUsingSameToken() {
        IgniteSql sql = igniteSql();
        Statement statement = sql.statementBuilder()
                .query("SELECT * FROM system_range(0, 10000000000)")
                .pageSize(1)
                .build();

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        ResultSet<SqlRow> query1rs = executeLazy(sql, token, statement);
        ResultSet<SqlRow> query2rs = executeLazy(sql, token, statement);
        ResultSet<SqlRow> query3rs = executeLazy(sql, token, statement);

        CompletableFuture<Void> cancelFut = cancelHandle.cancelAsync();

        expectQueryCancelled(() -> query1rs.forEachRemaining(r -> {}));
        expectQueryCancelled(() -> query2rs.forEachRemaining(r -> {}));
        expectQueryCancelled(() -> query3rs.forEachRemaining(r -> {}));

        await(cancelFut);
    }

    /**
     * The test ensures that in the case of an asynchronous cancellation call (either before or after the query is started),
     * the query will either not be started or will be cancelled. That is, it is impossible for a remote cancellation request
     * to be processed by the server before the query itself is started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void cancelQueryDuringExecution() throws Exception {
        IgniteSql sql = igniteSql();
        String query = "SELECT * FROM system_range(0, 10000000000)";

        int triesCount = 10;
        CyclicBarrier startBarrier = new CyclicBarrier(2);

        for (int i = triesCount - 1; i >= 0; i--) {
            CancelHandle cancelHandle = CancelHandle.create();
            CancellationToken token = cancelHandle.token();
            long delay = i;

            CompletableFuture<Void> cancelFut = IgniteTestUtils.runAsync(() -> {
                startBarrier.await();

                if (delay > 0) {
                    Thread.sleep(delay);
                }

                cancelHandle.cancel();
            });

            startBarrier.await();

            try (ResultSet<SqlRow> rs = executeLazy(sql, token, query)) {
                expectQueryCancelled(() -> {
                    while (rs.hasNext()) {
                        rs.next();
                    }
                });
            } catch (SqlException e) {
                assertEquals(Sql.EXECUTION_CANCELLED_ERR, e.code());

                continue;
            }

            await(cancelFut);

            startBarrier.reset();
        }
    }

    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, String query, Object... args) {
        return executeForRead(sql, null, query, args);
    }

    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, Statement query, Object... args) {
        return executeForRead(sql, null, query, args);
    }

    protected ResultSet<SqlRow> executeForRead(IgniteSql sql, @Nullable Transaction tx, String query, Object... args) {
        return executeForRead(sql, tx, igniteSql().createStatement(query), args);
    }

    protected abstract ResultSet<SqlRow> executeForRead(IgniteSql sql, @Nullable Transaction tx, Statement statement, Object... args);

    protected void checkSqlError(
            int code,
            String msg,
            IgniteSql sql,
            String query,
            Object... args
    ) {
        assertThrowsSqlException(code, msg, () -> execute(sql, query, args));
    }

    protected abstract long[] executeBatch(String query, BatchedArguments args);

    protected abstract long[] executeBatch(Statement statement, BatchedArguments args);

    protected ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, String query, Object... args) {
        return execute(expectedPages, tx, sql, sql.createStatement(query), args);
    }

    protected abstract ResultProcessor execute(Integer expectedPages, Transaction tx, IgniteSql sql, Statement statement, Object... args);

    protected ResultProcessor execute(int expectedPages, IgniteSql sql, String query, Object... args) {
        return execute(expectedPages, null, sql, query, args);
    }

    protected ResultProcessor execute(Transaction tx, IgniteSql sql, String query, Object... args) {
        return execute(null, tx, sql, query, args);
    }

    protected ResultProcessor execute(IgniteSql sql, String query, Object... args) {
        return execute(null, null, sql, query, args);
    }

    protected abstract void execute(IgniteSql sql, @Nullable Transaction tx, @Nullable CancellationToken token, String query);

    /** Executes query but only fetches the first page. */
    private ResultSet<SqlRow> executeLazy(IgniteSql sql, @Nullable CancellationToken token, String query, Object... args) {
        Statement statement = sql.statementBuilder().query(query).build();

        return executeLazy(sql, token, statement, args);
    }

    /** Executes statement but only fetches the first page. */
    protected abstract ResultSet<SqlRow> executeLazy(IgniteSql sql, @Nullable CancellationToken token, Statement statement, Object... args);

    protected abstract void executeScript(IgniteSql sql, String query, Object... args);

    protected abstract void executeScript(IgniteSql sql, CancellationToken token, String query, Object... args);

    protected abstract void rollback(Transaction outerTx);

    protected abstract void commit(Transaction outerTx);

    protected void checkDml(int expectedAffectedRows, Transaction tx, IgniteSql sql, String query, Object... args) {

    }

    protected void checkDml(int expectedAffectedRows, IgniteSql sql, String query, Object... args) {
        checkDml(expectedAffectedRows, null, sql, query, args);
    }

    protected void checkDdl(boolean expectedApplied, IgniteSql sql, String query) {
        checkDdl(expectedApplied, sql, query, null);
    }

    protected abstract void checkDdl(boolean expectedApplied, IgniteSql sql, String query, Transaction tx);

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
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }
}
