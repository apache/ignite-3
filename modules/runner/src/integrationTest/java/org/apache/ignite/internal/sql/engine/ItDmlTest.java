/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** Different DML tests. */
public class ItDmlTest extends AbstractBasicIntegrationTest {

    @Override
    protected int nodes() {
        return 3;
    }

    /**
     * Clear tables after each test.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @AfterEach
    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        dropAllTables();

        super.tearDownBase(testInfo);
    }

    @AfterAll
    public static void resetStaticState() {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16529")
    public void mergeOpChangePrimaryKey() {
        clearAndPopulateMergeTable1();

        clearAndPopulateMergeTable2();

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a "
                + "WHEN MATCHED THEN UPDATE SET b = src.b, k1 = src.k1 "
                + "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, 300, null)
                .returns(111, 333, 0, 100, "")
                .returns(444, 444, 2, 200, null)
                .check();
    }

    @Test
    public void batchWithConflictShouldBeRejectedEntirely() {
        sql("CREATE TABLE test (id int primary key, val int)");

        sql("INSERT INTO test values (1, 1)");

        assertQuery("SELECT count(*) FROM test")
                .returns(1L)
                .check();

        var sqlException = assertThrows(
                SqlException.class,
                () -> sql("INSERT INTO test VALUES (0, 0), (1, 1), (2, 2)")
        );

        assertEquals(Sql.DUPLICATE_KEYS_ERR, sqlException.code());

        assertQuery("SELECT count(*) FROM test")
                .returns(1L)
                .check();
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

        SqlException sqlException = assertThrows(
                SqlException.class,
                () -> sql(insertStatement)
        );

        assertEquals(Sql.DUPLICATE_KEYS_ERR, sqlException.code());

        assertQuery("SELECT count(*) FROM test")
                .returns(0L)
                .check();
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16679")
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
        assertThrows(CalciteContextException.class, () -> sql("MERGE INTO test2 test1 USING test1 ON c = e "
                + "WHEN MATCHED THEN UPDATE SET d = b + 1"), "Duplicate relation name");

        // Source table alias duplicate target table name.
        assertThrows(CalciteContextException.class, () -> sql("MERGE INTO test2 USING test1 test2 ON c = e "
                + "WHEN MATCHED THEN UPDATE SET d = b + 1"), "Duplicate relation name");

        // Without aliases, reference columns by table name.
        sql("MERGE INTO test2 USING test1 ON test1.a = test2.a "
                + "WHEN MATCHED THEN UPDATE SET a = test1.a + 1");

        assertQuery("SELECT * FROM test2").returns(1, 1, 0, "0").check();

        // Ambiguous column name in condition.
        assertThrows(CalciteContextException.class, () -> sql("MERGE INTO test2 USING test1 ON a = test1.a "
                + "WHEN MATCHED THEN UPDATE SET a = test1.a + 1"), "Column 'A' is ambiguous");

        // Ambiguous column name in update statement.
        assertThrows(CalciteContextException.class, () -> sql("MERGE INTO test2 USING test1 ON c = e "
                + "WHEN MATCHED THEN UPDATE SET a = a + 1"), "Column 'A' is ambiguous");

        // With aliases, reference columns by table alias.
        sql("MERGE INTO test2 test1 USING test1 test2 ON test1.d = test2.b "
                + "WHEN MATCHED THEN UPDATE SET a = test1.a + 1 "
                + "WHEN NOT MATCHED THEN INSERT (a, d, e) VALUES (test2.a, test2.b, test2.c)");

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

        IgniteException ex = assertThrows(IgniteException.class, () -> sql(
                        "MERGE INTO test2 USING test1 ON test1.a = test2.a "
                                + "WHEN MATCHED THEN UPDATE SET b = test1.b + 1 "
                                + "WHEN NOT MATCHED THEN INSERT (k, a, b) VALUES (0, a, b)"));

        assertEquals(Sql.DUPLICATE_KEYS_ERR, ex.code());
    }

    /**
     * Test verifies that scan is executed within provided transaction.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15081")
    public void scanExecutedWithinGivenTransaction() {
        sql("CREATE TABLE test (id int primary key, val int)");

        Transaction tx = CLUSTER_NODES.get(0).transactions().begin();

        sql(tx, "INSERT INTO test VALUES (0, 0)");

        // just inserted row should be visible within the same transaction
        assertEquals(1, sql(tx, "select * from test").size());

        Transaction anotherTx = CLUSTER_NODES.get(0).transactions().begin();

        // just inserted row should not be visible until related transaction is committed
        assertEquals(0, sql(anotherTx, "select * from test").size());

        tx.commit();

        assertEquals(1, sql(anotherTx, "select * from test").size());

        anotherTx.commit();
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void implicitPk() {
        IgniteTestUtils.setFieldValue(Commons.class, "implicitPkEnabled", null);

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

    @Test
    public void testInsertDefaultValue() {
        var args = List.of(
                // TODO: IGNITE-17298
                // new DefaultValueArg("BOOLEAN", "TRUE", Boolean.TRUE),
                // new DefaultValueArg("BOOLEAN NOT NULL", "TRUE", Boolean.TRUE),

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

        checkDefaultValue(args);

        checkWrongDefault("VARCHAR", "10");
        checkWrongDefault("INT", "'10'");
        checkWrongDefault("INT", "TRUE");
        checkWrongDefault("DATE", "10");
        checkWrongDefault("DATE", "TIME '01:01:01'");
        checkWrongDefault("TIME", "TIMESTAMP '2021-01-01 01:01:01'");

        // TODO: IGNITE-17298
        // checkWrongDefault("BOOLEAN", "1");

        // TODO: IGNITE-17373
        // checkWrongDefault("INTERVAL DAYS", "INTERVAL '10' MONTHS");
        // checkWrongDefault("INTERVAL MONTHS", "INTERVAL '10' DAYS");

        checkWrongDefault("VARBINARY", "'10'");
        checkWrongDefault("VARBINARY", "10");
    }

    private void checkDefaultValue(List<DefaultValueArg> args) {
        assert args.size() > 0;

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

    private void checkQueryResult(String sql, List<Object> expectedVals) {
        assertQuery(sql).returns(expectedVals.toArray()).check();
    }

    private void checkWrongDefault(String sqlType, String sqlVal) {
        try {
            assertThrows(
                    IgniteException.class,
                    () -> sql("CREATE TABLE test (val " + sqlType + " DEFAULT " + sqlVal + ")"),
                    "Cannot convert literal"
            );
        } finally {
            sql("DROP TABLE IF EXISTS test");
        }
    }

    private static class DefaultValueArg {
        final String sqlType;
        final String sqlVal;
        final Object expectedVal;

        private DefaultValueArg(String sqlType, String sqlVal, Object expectedVal) {
            this.sqlType = sqlType;
            this.sqlVal = sqlVal;
            this.expectedVal = expectedVal;
        }
    }
}
