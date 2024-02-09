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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Various DML tests.
 */
public class ItDmlTest extends BaseSqlIntegrationTest {

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @Test
    public void pkConstraintConsistencyTest() {
        sql("CREATE TABLE my (id INT PRIMARY KEY, val INT)");
        assertQuery("INSERT INTO my VALUES (?, ?)")
                .withParams(0, 1)
                .returns(1L)
                .check();
        assertQuery("SELECT val FROM my WHERE id = 0")
                .returns(1)
                .check();

        {
            assertThrowsSqlException(
                    Sql.CONSTRAINT_VIOLATION_ERR,
                    "PK unique constraint is violated",
                    () -> sql("INSERT INTO my VALUES (?, ?)", 0, 2));
        }

        assertQuery("DELETE FROM my WHERE id=?")
                .withParams(0)
                .returns(1L)
                .check();

        assertQuery("INSERT INTO my VALUES (?, ?)")
                .withParams(0, 2)
                .returns(1L)
                .check();

        assertQuery("SELECT val FROM my WHERE id = 0")
                .returns(2)
                .check();

        {
            assertThrowsSqlException(
                    Sql.CONSTRAINT_VIOLATION_ERR,
                    "PK unique constraint is violated",
                    () -> sql("INSERT INTO my VALUES (?, ?)", 0, 3));
        }
    }

    @Test
    public void failMergeOpOnChangePrimaryKey() {
        clearAndPopulateMergeTable1();

        clearAndPopulateMergeTable2();

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = src.b, k1 = src.k1 "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        assertThrows(IgniteException.class, () -> sql(sql));
    }

    @Test
    public void batchWithConflictShouldBeRejectedEntirely() {
        sql("CREATE TABLE test (id int primary key, val int)");

        sql("INSERT INTO test values (1, 1)");

        assertQuery("SELECT count(*) FROM test")
                .returns(1L)
                .check();

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> sql("INSERT INTO test VALUES (0, 0), (1, 1), (2, 2)")
        );


        assertQuery("SELECT count(*) FROM test")
                .returns(1L)
                .check();
    }

    /**
     * Test ensures inserts are possible after read lock on a range.
     */
    @Test
    public void rangeReadAndExclusiveInsert() {
        sql("CREATE TABLE test (id INT, aff_key INT, val INT, PRIMARY KEY (id, aff_key)) COLOCATE BY (aff_key) ");
        sql("CREATE INDEX test_val_asc_idx ON test (val ASC)");
        sql("INSERT INTO test VALUES (1, 1, 1), (2, 1, 2), (3, 1, 3)");

        log.info("Data was loaded.");

        Transaction tx = CLUSTER.aliveNode().transactions().begin();

        sql(tx, "SELECT * FROM test WHERE val <= 1 ORDER BY val");

        sql("INSERT INTO test VALUES (4, 1, 4)"); // <-- this INSERT uses implicit transaction
    }

    /**
     * Test ensures that big insert although being split to several chunks will share the same implicit transaction.
     */
    @Test
    public void bigBatchSpanTheSameTransaction() {
        List<Integer> values = new ArrayList<>(AbstractNode.MODIFY_BATCH_SIZE * 2);

        // need to generate batch big enough to be split on several chunks
        for (int i = 0; i < AbstractNode.MODIFY_BATCH_SIZE * 1.5; i++) {
            values.add(i);
        }

        values.add(values.get(0)); // add conflict entry from the first chunk

        sql("CREATE TABLE test (id int primary key, val int default 1)");

        String insertStatement = "INSERT INTO test (id) VALUES " + values.stream()
                .map(Object::toString)
                .collect(Collectors.joining("), (", "(", ")"));

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> sql(insertStatement)
        );

        assertQuery("SELECT count(*) FROM test")
                .returns(0L)
                .check();
    }

    @Test
    public void testNullDefault() {
        sql("CREATE TABLE test_null_def (id INTEGER PRIMARY KEY, col INTEGER DEFAULT NULL)");

        sql("INSERT INTO test_null_def VALUES(1, DEFAULT)");
        assertQuery("SELECT col FROM test_null_def WHERE id = 1").returns(null).check();

        sql("INSERT INTO test_null_def (id, col) VALUES(2, DEFAULT)");
        assertQuery("SELECT col FROM test_null_def WHERE id = 2").returns(null).check();
    }

    /**Test full MERGE command. */
    @Test
    public void testMerge() {
        clearAndPopulateMergeTable1();

        clearAndPopulateMergeTable2();

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = 100 * src.b "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, 10 * src.a, src.b)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 10, 300, null)
                .returns(333, 333, 0, 10000, "")
                .returns(444, 444, 2, 200, null)
                .check();

        // ---- the same but ON fld _a = src.a_ append to MATCHED too
        clearAndPopulateMergeTable2();

        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = src.b, a = src.a "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, 300, null)
                .returns(333, 333, 0, 100, "")
                .returns(444, 444, 2, 200, null)
                .check();

        // ---- all flds covered on NOT MATCHED
        clearAndPopulateMergeTable2();

        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = src.b, a = src.a "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b, c) VALUES (src.k1, src.k2, src.a, src.b, src.c)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, 300, "1")
                .returns(333, 333, 0, 100, "")
                .returns(444, 444, 2, 200, null)
                .check();

        // --- only WHEN MATCHED section.
        clearAndPopulateMergeTable2();

        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = src.b, a = src.a";
        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(333, 333, 0, 100, "")
                .returns(444, 444, 2, 200, null)
                .check();

        // ---- only WHEN NOT MATCHED section.
        clearAndPopulateMergeTable2();

        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b, c) VALUES (src.k1, src.k2, src.a, src.b, src.c)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, 300, "1")
                .returns(333, 333, 0, 100, "")
                .returns(444, 444, 2, 200, null)
                .check();
    }

    private void clearAndPopulateMergeTable2() {
        sql("DROP TABLE IF EXISTS test2 ");
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b int, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, 100, '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, 200, null)");
    }

    private void clearAndPopulateMergeTable1() {
        sql("DROP TABLE IF EXISTS test1 ");
        sql("CREATE TABLE test1 (k1 int, k2 int, a int, b int, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test1 VALUES (111, 111, 0, 100, '0')");
        sql("INSERT INTO test1 VALUES (222, 222, 1, 300, '1')");
    }

    /** Test MERGE table with itself. */
    @Test
    public void testMergeTableWithItself() {
        sql("DROP TABLE IF EXISTS test1 ");
        sql("CREATE TABLE test1 (k1 int, k2 int, a int, b int, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test1 VALUES (0, 0, 0, 0, '0')");

        String sql = "MERGE INTO test1 dst USING test1 src ON dst.a = src.a + 1 "
                + "WHEN MATCHED THEN UPDATE SET b = dst.b + 1 " // dst.b just for check here
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b, c) VALUES (src.k1 + 1, src.k2 + 1, src.a + 1, 1, src.a)";

        for (int i = 0; i < 5; i++) {
            sql(sql);
        }

        assertQuery("SELECT * FROM test1")
                .returns(0, 0, 0, 0, "0")
                .returns(1, 1, 1, 5, "0")
                .returns(2, 2, 2, 4, "1")
                .returns(3, 3, 3, 3, "2")
                .returns(4, 4, 4, 2, "3")
                .returns(5, 5, 5, 1, "4")
                .check();
    }

    /** Test MERGE operator with large batch. */
    @Test
    public void testMergeBatch() {
        sql("CREATE TABLE test1 (key int PRIMARY KEY, a int)");

        sql("INSERT INTO test1 SELECT x, x FROM TABLE(SYSTEM_RANGE(0, 9999))");

        assertQuery("SELECT count(*) FROM test1").returns(10_000L).check();

        sql("CREATE TABLE test2 (key int PRIMARY KEY, a int, b int)");

        sql("INSERT INTO test2 SELECT x, x, 0 FROM TABLE(SYSTEM_RANGE(-5000, 4999))");

        assertQuery("SELECT count(*) FROM test2 WHERE b = 0").returns(10_000L).check();

        sql("MERGE INTO test2 dst USING test1 src ON dst.a = src.a"
                + " WHEN MATCHED THEN UPDATE SET b = 1 "
                + " WHEN NOT MATCHED THEN INSERT (key, a, b) VALUES (src.key, src.a, 2)");

        assertQuery("SELECT count(*) FROM test2 WHERE b = 0").returns(5_000L).check();
        assertQuery("SELECT count(*) FROM test2 WHERE b = 1").returns(5_000L).check();
        assertQuery("SELECT count(*) FROM test2 WHERE b = 2").returns(5_000L).check();
    }

    /** Test MERGE operator with aliases. */
    @Test
    public void testMergeAliases() {
        sql("DROP TABLE IF EXISTS test1 ");
        sql("DROP TABLE IF EXISTS test2 ");
        sql("CREATE TABLE test1 (k int PRIMARY KEY, a int, b int, c varchar)");
        sql("INSERT INTO test1 VALUES (0, 0, 0, '0')");

        sql("CREATE TABLE test2 (k int PRIMARY KEY, a int, d int, e varchar)");

        // Without aliases, column 'A' in insert statement is not ambiguous.
        sql("MERGE INTO test2 USING test1 ON c = e"
                + " WHEN MATCHED THEN UPDATE SET d = b + 1"
                + " WHEN NOT MATCHED THEN INSERT (k, a, d, e) VALUES (k + 1, a, b, c)");

        assertQuery("SELECT * FROM test2").returns(1, 0, 0, "0").check();

        // Target table alias duplicate source table name.
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Duplicate relation name",
                () -> sql("MERGE INTO test2 test1 USING test1 ON c = e WHEN MATCHED THEN UPDATE SET d = b + 1"));

        // Source table alias duplicate target table name.
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Duplicate relation name",
                () -> sql("MERGE INTO test2 USING test1 test2 ON c = e WHEN MATCHED THEN UPDATE SET d = b + 1"));

        // Without aliases, reference columns by table name.
        sql("MERGE INTO test2 USING test1 ON test1.a = test2.a "
                + "WHEN MATCHED THEN UPDATE SET a = test1.a + 1");

        assertQuery("SELECT * FROM test2").returns(1, 1, 0, "0").check();

        // Ambiguous column name in condition.
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Column 'A' is ambiguous",
                () -> sql("MERGE INTO test2 USING test1 ON a = test1.a WHEN MATCHED THEN UPDATE SET a = test1.a + 1"));

        // Ambiguous column name in update statement.
        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Column 'A' is ambiguous",
                () -> sql("MERGE INTO test2 USING test1 ON c = e WHEN MATCHED THEN UPDATE SET a = a + 1"));

        // With aliases, reference columns by table alias.
        sql("MERGE INTO test2 test1 USING test1 test2 ON test1.d = test2.b "
                + "WHEN MATCHED THEN UPDATE SET a = test1.a + 1 "
                + "WHEN NOT MATCHED THEN INSERT (k, a, d, e) VALUES (test2.k, test2.a, test2.b, test2.c)");

        assertQuery("SELECT * FROM test2").returns(1, 2, 0, "0").check();
    }

    /** Test MERGE operator with keys conflicts. */
    @Test
    public void testMergeKeysConflict() {
        sql("DROP TABLE IF EXISTS test1 ");
        sql("DROP TABLE IF EXISTS test2 ");
        sql("CREATE TABLE test1 (k int PRIMARY KEY, a int, b int)");
        sql("INSERT INTO test1 VALUES (0, 0, 0)");
        sql("INSERT INTO test1 VALUES (1, 1, 1)");
        sql("INSERT INTO test1 VALUES (2, 2, 2)");

        sql("CREATE TABLE test2 (k int PRIMARY KEY, a int, b int)");

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "PK unique constraint is violated",
                () -> sql(
                        "MERGE INTO test2 USING test1 ON test1.a = test2.a "
                                + "WHEN MATCHED THEN UPDATE SET b = test1.b + 1 "
                                + "WHEN NOT MATCHED THEN INSERT (k, a, b) VALUES (0, a, b)"));
    }

    /**
     * Test verifies that scan is executed within provided transaction.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15081")
    public void scanExecutedWithinGivenTransaction() {
        sql("CREATE TABLE test (id int primary key, val int)");

        Transaction tx = CLUSTER.aliveNode().transactions().begin();

        sql(tx, "INSERT INTO test VALUES (0, 0)");

        // just inserted row should be visible within the same transaction
        assertEquals(1, sql(tx, "select * from test").size());

        Transaction anotherTx = CLUSTER.aliveNode().transactions().begin();

        // just inserted row should not be visible until related transaction is committed
        assertEquals(0, sql(anotherTx, "select * from test").size());

        tx.commit();

        assertEquals(1, sql(anotherTx, "select * from test").size());

        anotherTx.commit();
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void implicitPk() {
        sql("CREATE TABLE T(VAL INT)");

        sql("INSERT INTO t VALUES (1), (2), (3)");

        assertQuery("select * from t")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

        var pkVals = sql("select \"__p_key\" from t").stream().map(row -> row.get(0)).collect(Collectors.toSet());

        assertEquals(3, pkVals.size());
    }

    private static Stream<DefaultValueArg> defaultValueArgs() {
        return Stream.of(
                new DefaultValueArg("BOOLEAN", "TRUE", Boolean.TRUE),
                new DefaultValueArg("BOOLEAN NOT NULL", "TRUE", Boolean.TRUE),

                new DefaultValueArg("BIGINT", "10", 10L),
                new DefaultValueArg("INTEGER", "10", 10),
                new DefaultValueArg("SMALLINT", "10", (short) 10),
                new DefaultValueArg("TINYINT", "10", (byte) 10),
                new DefaultValueArg("DOUBLE", "10.01", 10.01d),
                new DefaultValueArg("FLOAT", "10.01", 10.01f),
                new DefaultValueArg("DECIMAL(4, 2)", "10.01", new BigDecimal("10.01")),
                new DefaultValueArg("CHAR(2)", "'10'", "10"),
                new DefaultValueArg("VARCHAR", "'10'", "10"),
                new DefaultValueArg("VARCHAR NOT NULL", "'10'", "10"),
                new DefaultValueArg("VARCHAR(2)", "'10'", "10"),

                // TODO: IGNITE-17373
                // new DefaultValueArg("INTERVAL DAYS TO SECONDS", "INTERVAL '10' DAYS", Duration.ofDays(10)),
                // new DefaultValueArg("INTERVAL YEARS TO MONTHS", "INTERVAL '10' MONTHS", Period.ofMonths(10)),
                // new DefaultValueArg("INTERVAL MONTHS", "INTERVAL '10' YEARS", Period.ofYears(10)),

                new DefaultValueArg("DATE", "DATE '2021-01-01'", LocalDate.parse("2021-01-01")),
                new DefaultValueArg("TIME", "TIME '01:01:01'", LocalTime.parse("01:01:01")),
                new DefaultValueArg("TIMESTAMP", "TIMESTAMP '2021-01-01 01:01:01'", LocalDateTime.parse("2021-01-01T01:01:01")),

                // TODO: IGNITE-17376
                // new DefaultValueArg("TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP '2021-01-01 01:01:01'"
                //         , LocalDateTime.parse("2021-01-01T01:01:01")),

                new DefaultValueArg("BINARY(3)", "x'010203'", new byte[]{1, 2, 3})

                // TODO: IGNITE-17374
                // new DefaultValueArg("VARBINARY", "x'010203'", new byte[]{1, 2, 3})
        );
    }

    @Test
    public void testInsertDefaultValue() {
        checkDefaultValue(defaultValueArgs().collect(Collectors.toList()));

        checkWrongDefault("VARCHAR", "10");
        checkWrongDefault("INT", "'10'");
        checkWrongDefault("INT", "TRUE");
        checkWrongDefault("DATE", "10");
        checkWrongDefault("DATE", "TIME '01:01:01'");
        checkWrongDefault("TIME", "TIMESTAMP '2021-01-01 01:01:01'");
        checkWrongDefault("BOOLEAN", "1");

        // TODO: IGNITE-17373
        // checkWrongDefault("INTERVAL DAYS", "INTERVAL '10' MONTHS");
        // checkWrongDefault("INTERVAL MONTHS", "INTERVAL '10' DAYS");

        checkWrongDefault("VARBINARY", "'10'");
        checkWrongDefault("VARBINARY", "10");
    }

    @Test
    public void testInsertDefaultNullValue() {
        checkDefaultValue(defaultValueArgs()
                .filter(a -> !a.sqlType.endsWith("NOT NULL"))
                // TODO: uncomment after https://issues.apache.org/jira/browse/IGNITE-21243
                //.map(a -> new DefaultValueArg(a.sqlType, "NULL", null))
                .collect(Collectors.toList()));
    }

    private void checkDefaultValue(List<DefaultValueArg> args) {
        assert !args.isEmpty();

        try {
            StringBuilder createStatementBuilder = new StringBuilder("CREATE TABLE test (id INT PRIMARY KEY");

            var idx = 0;
            for (var arg : args) {
                createStatementBuilder.append(", col_").append(idx++).append(" ").append(arg.sqlType)
                        .append(" DEFAULT ").append(arg.sqlVal);
            }

            var createStatement = createStatementBuilder.append(")").toString();

            sql(createStatement);
            sql("INSERT INTO test (id) VALUES (0)");

            var expectedVals = args.stream()
                    .map(a -> a.expectedVal)
                    .collect(Collectors.toList());

            var columnEnumerationBuilder = new StringBuilder("col_0");

            for (int i = 1; i < args.size(); i++) {
                columnEnumerationBuilder.append(", col_").append(i);
            }

            var columnEnumeration = columnEnumerationBuilder.toString();

            checkQueryResult("SELECT " + columnEnumeration + " FROM test", expectedVals);

            sql("DELETE FROM test");
            sql("INSERT INTO test (id, " + columnEnumeration + ") VALUES (0, "
                    + ", DEFAULT".repeat(args.size()).substring(2) + ")");

            checkQueryResult("SELECT " + columnEnumeration + " FROM test", expectedVals);
        } finally {
            sql("DROP TABLE IF EXISTS test");
        }
    }

    @Test
    public void testCheckNullValueErrorMessageForColumnWithDefaultValue() {
        sql("CREATE TABLE tbl(key int DEFAULT 9 primary key, val varchar)");

        var expectedMessage = "Failed to validate query. From line 1, column 28 to line 1, column 45: Column 'KEY' does not allow NULLs";

        assertThrowsSqlException(Sql.STMT_VALIDATION_ERR, expectedMessage, () -> sql("INSERT INTO tbl (key, val) VALUES (NULL,'AA')"));
    }

    private void checkQueryResult(String sql, List<Object> expectedVals) {
        assertQuery(sql).returns(expectedVals.toArray()).check();
    }

    private void checkWrongDefault(String sqlType, String sqlVal) {
        try {
            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Unable convert literal",
                    () -> sql("CREATE TABLE test (id INT PRIMARY KEY, val " + sqlType + " DEFAULT " + sqlVal + ")")
            );
        } finally {
            sql("DROP TABLE IF EXISTS test");
        }
    }

    private static class DefaultValueArg {
        final String sqlType;
        final String sqlVal;
        final Object expectedVal;

        private DefaultValueArg(String sqlType, String sqlVal, @Nullable Object expectedVal) {
            this.sqlType = sqlType;
            this.sqlVal = sqlVal;
            this.expectedVal = expectedVal;
        }

        @Override
        public String toString() {
            return sqlType + " sql val: " + sqlVal + " java val: " + expectedVal;
        }
    }

    @Test
    public void testInsertMultipleDefaults() {
        sql("CREATE TABLE integers(i INTEGER PRIMARY KEY, col1 INTEGER DEFAULT 200, col2 INTEGER DEFAULT 300)");

        sql("INSERT INTO integers (i) VALUES (0)");
        sql("INSERT INTO integers VALUES (1, DEFAULT, DEFAULT)");
        sql("INSERT INTO integers(i, col2) VALUES (2, DEFAULT), (3, 4), (4, DEFAULT)");
        sql("INSERT INTO integers VALUES (5, DEFAULT, DEFAULT)");
        sql("INSERT INTO integers VALUES (6, 4, DEFAULT)");
        sql("INSERT INTO integers VALUES (7, 5, 5)");
        sql("INSERT INTO integers(col1, i) VALUES (DEFAULT, 8)");
        sql("INSERT INTO integers(i, col1) VALUES (9, DEFAULT)");

        assertQuery("SELECT i, col1, col2 FROM integers ORDER BY i")
                .returns(0, 200, 300)
                .returns(1, 200, 300)
                .returns(2, 200, 300)
                .returns(3, 200, 4)
                .returns(4, 200, 300)
                .returns(5, 200, 300)
                .returns(6, 4, 300)
                .returns(7, 5, 5)
                .returns(8, 200, 300)
                .returns(9, 200, 300)
                .check();
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testInsertMultipleDefaultsWithImplicitPk() {
        sql("CREATE TABLE integers(i INTEGER, j INTEGER DEFAULT 100)");

        sql("INSERT INTO integers VALUES (1, DEFAULT)");

        assertQuery("SELECT i, j FROM integers").returns(1, 100).check();

        sql("INSERT INTO integers VALUES (2, 3), (3, DEFAULT), (4, 4), (5, DEFAULT)");

        sql("INSERT INTO integers(j, i) VALUES (DEFAULT, 6)");

        assertQuery("SELECT i, j FROM integers ORDER BY i")
                .returns(1, 100)
                .returns(2, 3)
                .returns(3, 100)
                .returns(4, 4)
                .returns(5, 100)
                .returns(6, 100)
                .check();
    }

    @Test
    public void testDeleteUsingCompositePk() {
        sql("CREATE TABLE test (a INT, b VARCHAR NOT NULL, c INT NOT NULL, d INT NOT NULL, PRIMARY KEY(d, b)) COLOCATE BY (d)");
        sql("INSERT INTO test VALUES "
                + "(0, '3', 0, 1),"
                + "(0, '3', 0, 2),"
                + "(0, '4', 0, 2)");

        // Use PK index.
        sql("DELETE FROM test WHERE b = '3' and d = 2");
        assertQuery("SELECT d FROM test").returns(1).returns(2).check();

        sql("DELETE FROM test WHERE d = 1");
        assertQuery("SELECT b FROM test").returns("4").check();

        sql("DELETE FROM test WHERE a = 0");
        assertQuery("SELECT d FROM test").returnNothing().check();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("decimalLimits")
    public void testInsertValueOverflow(String type, long max, long min) {
        try {
            sql(String.format("CREATE TABLE %s (ID INT PRIMARY KEY, VAL %s);", type, type));

            sql(String.format("CREATE TABLE T_HELPER (ID INT PRIMARY KEY, VAL %s);", type));
            sql("INSERT INTO T_HELPER VALUES (1, 1);");
            sql(String.format("INSERT INTO T_HELPER VALUES (2, %d);", max));
            sql("INSERT INTO T_HELPER VALUES (3, -1);");
            sql(String.format("INSERT INTO T_HELPER VALUES (4, %d);", min));

            BigDecimal moreThanMax = new BigDecimal(max).add(BigDecimal.ONE);

            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, %s);", type, moreThanMax.toString())));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, %d + 1);", type, max)));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, %d - 1);", type, min)));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, %d + (SELECT 1));", type, max)));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, %d + (SELECT -1));", type, min)));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, (SELECT SUM(VAL) FROM T_HELPER WHERE VAL > 0));", type)));
            assertThrowsSqlException(Sql.RUNTIME_ERR, String.format("%s out of range", type),
                    () -> sql(String.format("INSERT INTO %s (ID, VAL) VALUES (1, (SELECT SUM(VAL) FROM T_HELPER WHERE VAL < 0));", type)));
        } finally {
            sql("DROP TABLE " + type);
            sql("DROP TABLE T_HELPER");
        }
    }

    private static Stream<Arguments> decimalLimits() {
        return Stream.of(
                arguments(SqlTypeName.BIGINT.getName(), Long.MAX_VALUE, Long.MIN_VALUE),
                arguments(SqlTypeName.INTEGER.getName(), Integer.MAX_VALUE, Integer.MIN_VALUE),
                arguments(SqlTypeName.SMALLINT.getName(), Short.MAX_VALUE, Short.MIN_VALUE),
                arguments(SqlTypeName.TINYINT.getName(), Byte.MAX_VALUE, Byte.MIN_VALUE)
        );
    }

    @Test
    public void test() {
        sql("CREATE TABLE my (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1))");

        Tuple key = Tuple.create()
                .set("c1", 1)
                .set("c3", 3);

        Tuple val = Tuple.create()
                .set("c2", 2);

        KeyValueView<Tuple, Tuple> embeddedKv = CLUSTER.aliveNode()
                .tables()
                .table("my")
                .keyValueView();

        embeddedKv.put(null, key, val);

        KeyValueView<Tuple, Tuple> thinKv = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
                .tables()
                .table("my")
                .keyValueView();

        assertNotNull(embeddedKv.get(null, key), "embeddedKv");
        assertNotNull(thinKv.get(null, key), "thinKv");
    }
}
