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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration test for set op (EXCEPT, INTERSECT).
 */
public class ItSetOpTest extends BaseSqlIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createTable("EMP1");
        createTable("EMP2");

        int idx = 0;
        insertData("PUBLIC.EMP1", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, "Igor", 11d},
                {idx++, "Igor", 12d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx, "Roman", 14d}
        });

        idx = 0;
        insertData("PUBLIC.EMP2", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Roman", 10d},
                {idx++, "Roman", 11d},
                {idx++, "Roman", 12d},
                {idx++, "Roman", 13d},
                {idx++, "Igor1", 13d},
                {idx, "Igor1", 13d}
        });

        // Creating tables with different numeric types for the "val" column.
        {
            sql("CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)");
            sql("INSERT INTO t1 VALUES(1, 1)");
            sql("INSERT INTO t1 VALUES(2, 2)");
            sql("INSERT INTO t1 VALUES(3, 3)");
            sql("INSERT INTO t1 VALUES(4, 4)");

            sql("CREATE TABLE t2(id INTEGER PRIMARY KEY, val DECIMAL(4,2))");
            sql("INSERT INTO t2 VALUES(2, 2)");
            sql("INSERT INTO t2 VALUES(4, 4)");

            sql("CREATE TABLE t3(id INTEGER PRIMARY KEY, val NUMERIC(4,2))");
            sql("INSERT INTO t3 VALUES(2, 2)");
            sql("INSERT INTO t3 VALUES(3, 3)");
        }
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testExcept(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 EXCEPT SELECT name FROM emp2");

        assertEquals(1, rows.size());
        assertEquals("Igor", rows.get(0).get(0));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testExceptFromEmpty(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 WHERE salary < 0 EXCEPT SELECT name FROM emp2");

        assertEquals(0, rows.size());
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testExceptSeveralColumns(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name, salary FROM emp1 EXCEPT SELECT name, salary FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, countIf(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testExceptAll(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 EXCEPT ALL SELECT name FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, countIf(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testExceptNested(SetOpVariant setOp) {
        var rows =
                sql(setOp, "SELECT name FROM emp1 EXCEPT (SELECT name FROM emp1 EXCEPT SELECT name FROM emp2)");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18475")
    public void testSetOpBigBatch() {
        sql("CREATE TABLE big_table1(key INT PRIMARY KEY, val INT)");
        sql("CREATE TABLE big_table2(key INT PRIMARY KEY, val INT)");

        int key = 0;

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < ((i == 0) ? 1 : (1 << (i * 4 - 1))); j++) {
                // Cache1 keys count: 1 of "0", 8 of "1", 128 of "2", 2048 of "3", 32768 of "4".
                sql("INSERT INTO big_table1 VALUES (?, ?)", key++, i);

                // Cache2 keys count: 1 of "5", 128 of "3", 32768 of "1".
                if ((i & 1) == 0) {
                    sql("INSERT INTO big_table2 VALUES (?, ?)", key++, 5 - i);
                }
            }
        }

        // Check 2 partitioned caches.
        var rows = sql("SELECT val FROM BIG_TABLE1 EXCEPT SELECT val FROM BIG_TABLE2");

        assertEquals(3, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(0)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(2)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(4)));

        rows = sql("SELECT val FROM BIG_TABLE1 EXCEPT ALL SELECT val FROM BIG_TABLE2");

        assertEquals(34817, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(0)));
        assertEquals(128, countIf(rows, r -> r.get(0).equals(2)));
        assertEquals(1920, countIf(rows, r -> r.get(0).equals(3)));
        assertEquals(32768, countIf(rows, r -> r.get(0).equals(4)));

        rows = sql("SELECT val FROM BIG_TABLE1 INTERSECT SELECT val FROM BIG_TABLE2");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(1)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(3)));

        rows = sql("SELECT val FROM BIG_TABLE1 INTERSECT ALL SELECT val FROM BIG_TABLE2");

        assertEquals(136, rows.size());
        assertEquals(8, countIf(rows, r -> r.get(0).equals(1)));
        assertEquals(128, countIf(rows, r -> r.get(0).equals(3)));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testIntersect(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 INTERSECT SELECT name FROM emp2");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testIntersectAll(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 INTERSECT ALL SELECT name FROM emp2");

        assertEquals(3, rows.size());
        assertEquals(2, countIf(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testIntersectEmpty(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name FROM emp1 WHERE salary < 0 INTERSECT SELECT name FROM emp2");

        assertEquals(0, rows.size());
    }

    @ParameterizedTest
    @EnumSource(SetOpVariant.class)
    public void testIntersectSeveralColumns(SetOpVariant setOp) {
        var rows = sql(setOp, "SELECT name, salary FROM emp1 INTERSECT ALL SELECT name, salary FROM emp2");

        assertEquals(2, rows.size());
        assertEquals(2, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18426")
    @Test
    public void testSetOpColocated() {
        sql("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, PRIMARY KEY(empid, deptid)) COLOCATE BY (deptid)");
        sql("CREATE TABLE dept(deptid INTEGER, name VARCHAR, PRIMARY KEY(deptid))");

        sql("INSERT INTO emp VALUES (0, 0, 'test0'), (1, 0, 'test1'), (2, 1, 'test2')");
        sql("INSERT INTO dept VALUES (0, 'test0'), (1, 'test1'), (2, 'test2')");

        assertQuery("SELECT deptid, name FROM emp EXCEPT SELECT deptid, name FROM dept")
                .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
                .returns(0, "test1")
                .returns(1, "test2")
                .check();

        assertQuery("SELECT deptid, name FROM dept EXCEPT SELECT deptid, name FROM emp")
                .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
                .returns(1, "test1")
                .returns(2, "test2")
                .check();

        assertQuery("SELECT deptid FROM dept EXCEPT SELECT deptid FROM emp")
                .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedMinus.*"))
                .returns(2)
                .check();

        assertQuery("SELECT deptid FROM dept INTERSECT SELECT deptid FROM emp")
                .matches(QueryChecker.matches(".*IgniteExchange.*IgniteColocatedIntersect.*"))
                .returns(0)
                .returns(1)
                .check();
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({"EXCEPT,1,1.00", "INTERSECT,2,2.00"})
    public void testSetOpDifferentNumericTypes(String setOp, int expectId, String expectVal) {
        String query = "SELECT id, val FROM t1 "
                + setOp
                + " SELECT id, val FROM t2 "
                + setOp
                + " SELECT id, val FROM t3 ";

        assertQuery(query)
                .returns(expectId, new BigDecimal(expectVal))
                .columnMetadata(
                        new MetadataMatcher().nullable(false).type(ColumnType.INT32),
                        new MetadataMatcher().nullable(true).type(ColumnType.DECIMAL)
                )
                .ordered()
                .check();
    }

    @Test
    public void testUnionDifferentNumericTypes() {
        String query = ""
                + "SELECT id, val FROM t1 "
                + "UNION "
                + "SELECT id, val FROM t2";

        assertQuery(query)
                .returns(1, new BigDecimal("1.00"))
                .returns(2, new BigDecimal("2.00"))
                .returns(3, new BigDecimal("3.00"))
                .returns(4, new BigDecimal("4.00"))
                .columnMetadata(
                        new MetadataMatcher().type(ColumnType.INT32),
                        new MetadataMatcher().type(ColumnType.DECIMAL)
                )
                .check();
    }

    /**
     * Test that set op node can be rewinded.
     */
    @ParameterizedTest
    @MethodSource("rewindSetOpVariants")
    public void testSetOpRewindability(SetOpVariant setOp, int tableNum) {
        sql(format("CREATE TABLE test_{}(id int PRIMARY KEY, i INTEGER)", tableNum));
        sql(format("INSERT INTO test_{} VALUES (1, 1), (2, 2)", tableNum));

        String query = format("SELECT {} (SELECT i FROM test_{} EXCEPT SELECT test_{}.i) FROM test_{}",
                setOp.hint(), tableNum, tableNum, tableNum, tableNum
        );

        assertQuery(query)
                .returns(1)
                .returns(2)
                .check();
    }

    private static Stream<Arguments> rewindSetOpVariants() {
        List<Arguments> arguments = new ArrayList<>();

        SetOpVariant[] ops = SetOpVariant.values();
        for (int i = 0; i < ops.length; i++) {
            arguments.add(Arguments.of(ops[i], i));
        }

        return arguments.stream();
    }

    @Test
    public void testUnionAll() {
        var rows = sql("SELECT name, salary FROM emp1 "
                + "UNION ALL "
                + "SELECT name, salary FROM emp2 "
                + "UNION ALL "
                + "SELECT name, salary FROM emp1 WHERE salary > 13 ");

        assertEquals(14, rows.size());
    }

    @Test
    public void testUnion() {
        var rows = sql("SELECT name, salary FROM emp1 "
                + "UNION "
                + "SELECT name, salary FROM emp2 "
                + "UNION "
                + "SELECT name, salary FROM emp1 WHERE salary > 13 ");

        assertEquals(9, rows.size());
    }

    @Test
    public void testUnionWithDistinct() {
        var rows = sql(
                "SELECT distinct(name) FROM emp1 UNION SELECT name from emp2");

        assertEquals(3, rows.size());
    }

    private static void createTable(String tableName) {
        sql("CREATE TABLE " + tableName + "(id INT PRIMARY KEY, name VARCHAR, salary DOUBLE)");
    }

    private <T> long countIf(Iterable<T> it, Predicate<T> pred) {
        return StreamSupport.stream(it.spliterator(), false).filter(pred).count();
    }

    private static List<List<Object>> sql(SetOpVariant setOp, String sql) {
        // Wrap set query into SELECT because calcite does not allow to specify hints on set operation nodes (SqlCall nodes).
        return sql(format("SELECT {} * FROM (" + sql + ")", setOp.hint()));
    }

    /**
     * Set operation variant.
     */
    public enum SetOpVariant {
        COLOCATED("MapReduceMinusConverterRule"),
        MAP_REDUCE("ColocatedMinusConverterRule");

        final String[] disabledRules;

        SetOpVariant(String... disabledRules) {
            this.disabledRules = disabledRules;
        }

        String hint() {
            return HintUtils.toHint(IgniteHint.DISABLE_RULE, disabledRules);
        }
    }
}
