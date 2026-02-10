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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Group of tests to verify aggregation functions.
 */
public class ItAggregatesTest extends BaseSqlIntegrationTest {
    private static final String[] PERMUTATION_RULES = {
            "MapReduceHashAggregateConverterRule", "MapReduceSortAggregateConverterRule",
            "ColocatedHashAggregateConverterRule", "ColocatedSortAggregateConverterRule"
    };

    private static final int ROWS = 103;

    // most commonly used values
    private static final BigDecimal ONE_WITH_SCALE_16 = new BigDecimal("1").setScale(16, RoundingMode.UNNECESSARY);
    private static final BigDecimal ONE_AND_HALF_WITH_SCALE_16 = new BigDecimal("1.5").setScale(16, RoundingMode.UNNECESSARY);
    private static final BigDecimal TWO_WITH_SCALE_16 = new BigDecimal("2").setScale(16, RoundingMode.UNNECESSARY);

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();

        sql("CREATE ZONE test_zone (replicas 2, partitions 10) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']");
        sql("CREATE TABLE test (id INT PRIMARY KEY, grp0 INT, grp1 INT, val0 INT, val1 INT) ZONE TEST_ZONE");
        sql("CREATE TABLE test_one_col_idx (pk INT PRIMARY KEY, col0 INT)");

        for (int i = 0; i < ROWS; i++) {
            sql("INSERT INTO test (id, grp0, grp1, val0, val1) VALUES (?, ?, ?, ?, ?)", i, i / 10, i / 100, 1, 2);
            sql("INSERT INTO test_one_col_idx (pk, col0) VALUES (?, ?)", i, i);
        }

        sql("CREATE TABLE t1_colo_val1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                + "COLOCATE BY (val1)");

        sql("CREATE TABLE t2_colo_va1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                + "COLOCATE BY (val1)");

        for (int i = 0; i < 100; i++) {
            sql("INSERT INTO t1_colo_val1 VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i % 2, "val" + i);
        }

        sql("INSERT INTO t2_colo_va1 VALUES (0, 'val0', 'val0', 'val0'), (1, 'val1', 'val1', 'val1')");

        sql("CREATE TABLE test_a_b_s (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s VARCHAR);");
        sql("INSERT INTO test_a_b_s VALUES (1, 11, 1, 'hello'), (2, 12, 2, 'world'), (3, 11, 3, NULL)");
        sql("INSERT INTO test_a_b_s VALUES (4, 11, 3, 'hello'), (5, 12, 2, 'world'), (6, 10, 5, 'ahello'), (7, 13, 6, 'world')");

        sql("CREATE TABLE test_str_int_real_dec "
                + "(id INTEGER PRIMARY KEY, str_col VARCHAR, int_col INTEGER,  real_col REAL, dec_col DECIMAL)");

        sql("CREATE TABLE IF NOT EXISTS numbers ("
                + "id INTEGER PRIMARY KEY, "
                + "tinyint_col TINYINT, "
                + "smallint_col SMALLINT, "
                + "int_col INTEGER, "
                + "bigint_col BIGINT, "
                + "float_col REAL, "
                + "double_col DOUBLE, "
                + "dec2_col DECIMAL(2), "
                + "dec4_2_col DECIMAL(4,2), "
                + "dec20_18_col DECIMAL(20,18), "
                + "dec10_2_col DECIMAL(10,2) "
                + ")");

        sql("CREATE TABLE IF NOT EXISTS not_null_numbers ("
                + "id INTEGER PRIMARY KEY, "
                + "int_col INTEGER NOT NULL, "
                + "dec4_2_col DECIMAL(4,2) NOT NULL"
                + ")");

        gatherStatistics();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void aggregateWithSumAndHaving(String[] rules) {
        var res = sql(
                appendDisabledRules("SELECT SUM(val0), SUM(val1), grp0 FROM TEST GROUP BY grp0 HAVING SUM(val1) > 10", rules));

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            long s0 = (Long) r.get(0);
            long s1 = (Long) r.get(1);

            assertEquals(s0 * 2, s1);
        });
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void correctCollationsOnAgg(String[] rules) {
        var cursors = sql(
                appendDisabledRules("SELECT PK FROM TEST_ONE_COL_IDX WHERE col0 IN (SELECT col0 FROM TEST_ONE_COL_IDX)", rules));

        assertEquals(ROWS, cursors.size());
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void countOfNonNumericField(String[] rules) {
        assertQuery("select count(name) from person").disableRules(rules).returns(4L).check();
        assertQuery("select count(*) from person").disableRules(rules).returns(5L).check();
        assertQuery("select count(1) from person").disableRules(rules).returns(5L).check();
        assertQuery("select count(null) from person").disableRules(rules).returns(0L).check();

        assertQuery("select count(*) from person where salary < 0").disableRules(rules).returns(0L).check();
        assertQuery("select count(*) from person where salary < 0 and salary > 0").disableRules(rules).returns(0L).check();

        assertQuery("select count(case when name like 'R%' then 1 else null end) from person").disableRules(rules).returns(2L).check();
        assertQuery("select count(case when name not like 'I%' then 1 else null end) from person").disableRules(rules).returns(2L).check();

        assertQuery("select count(name) from person where salary > 10").disableRules(rules).returns(1L).check();
        assertQuery("select count(*) from person where salary > 10").disableRules(rules).returns(2L).check();
        assertQuery("select count(1) from person where salary > 10").disableRules(rules).returns(2L).check();
        assertQuery("select count(*) from person where name is not null").disableRules(rules).returns(4L).check();

        assertQuery("select count(name) filter (where salary > 10) from person").disableRules(rules).returns(1L).check();
        assertQuery("select count(*) filter (where salary > 10) from person").disableRules(rules).returns(2L).check();
        assertQuery("select count(1) filter (where salary > 10) from person").disableRules(rules).returns(2L).check();

        assertQuery("select salary, count(name) from person group by salary order by salary")
                .disableRules(rules)
                .returns(10d, 3L)
                .returns(15d, 1L)
                .check();

        // same query, but grouping by alias
        assertQuery("select salary as sal, count(name) from person group by sal order by sal")
                .disableRules(rules)
                .returns(10d, 3L)
                .returns(15d, 1L)
                .check();

        // same query, but grouping by ordinal
        assertQuery("select salary, count(name) from person group by 1 order by 1")
                .disableRules(rules)
                .returns(10d, 3L)
                .returns(15d, 1L)
                .check();

        assertQuery("select salary * salary / 5, count(name) from person group by (salary * salary / 5) order by (salary * salary / 5)")
                .disableRules(rules)
                .returns(20d, 3L)
                .returns(45d, 1L)
                .check();

        // same query, but grouping by alias
        assertQuery("select (salary * salary / 5) as sal, count(name) from person group by sal order by sal")
                .disableRules(rules)
                .returns(20d, 3L)
                .returns(45d, 1L)
                .check();

        // same query, but grouping by ordinal
        assertQuery("select salary * salary / 5, count(name) from person group by 1 order by 1")
                .disableRules(rules)
                .returns(20d, 3L)
                .returns(45d, 1L)
                .check();

        assertQuery("select salary, count(*) from person group by salary order by salary")
                .disableRules(rules)
                .returns(10d, 3L)
                .returns(15d, 2L)
                .check();

        assertQuery("select salary, count(1) from person group by salary order by salary")
                .disableRules(rules)
                .returns(10d, 3L)
                .returns(15d, 2L)
                .check();

        assertQuery("select salary, count(1), sum(1) from person group by salary order by salary")
                .disableRules(rules)
                .returns(10d, 3L, 3L)
                .returns(15d, 2L, 2L)
                .check();

        assertQuery("select salary, name, count(1), sum(salary) from person group by salary, name order by salary")
                .disableRules(rules)
                .returns(10d, "Igor", 1L, 10d)
                .returns(10d, "Roma", 2L, 20d)
                .returns(15d, "Ilya", 1L, 15d)
                .returns(15d, null, 1L, 15d)
                .check();

        assertQuery("select salary, count(name) from person group by salary having salary < 10 order by salary")
                .disableRules(rules)
                .check();

        assertQuery("select count(name), name from person group by name")
                .disableRules(rules)
                .returns(1L, "Igor")
                .returns(1L, "Ilya")
                .returns(2L, "Roma")
                .returns(0L, null)
                .check();

        assertQuery("select avg(salary) from person")
                .disableRules(rules)
                .returns(12.0)
                .check();

        assertQuery("select name, salary from person where person.salary > (select avg(person.salary) from person)")
                .disableRules(rules)
                .returns(null, 15d)
                .returns("Ilya", 15d)
                .check();

        assertQuery("select avg(salary) from (select avg(salary) as salary from person union all select salary from person)")
                .disableRules(rules)
                .returns(12d)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testMultipleRowsFromSingleAggr(String[] rules) {
        Assumptions.assumeTrue(
                Arrays.stream(rules).noneMatch(rule -> rule.contains("ColocatedHash"))
                        || Arrays.stream(rules).noneMatch(rule -> rule.contains("MapReduceHash")),
                "Sorted aggregates are currently disabled on correlated path because "
                        + "they may cause deadlock"
        );

        assertThrows(
                IgniteException.class,
                () -> assertQuery("SELECT (SELECT name FROM person)").disableRules(rules).check()
        );

        assertThrows(
                IgniteException.class,
                () -> assertQuery("SELECT t.id, (SELECT x FROM TABLE(system_range(1, 5))) FROM person t").disableRules(rules).check()
        );

        assertThrows(
                IgniteException.class,
                () -> assertQuery("SELECT t.id, (SELECT x FROM "
                        + "TABLE(system_range(t.id, t.id + 1))) FROM person t").disableRules(rules).check()
        );

        assertQuery("SELECT t.id, (SELECT x FROM TABLE(system_range(t.id, t.id))) FROM person t").disableRules(rules).check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAnyValAggr(String[] rules) {
        var res = sql(appendDisabledRules("select any_value(name) from person", rules));

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertTrue("Igor".equals(val) || "Roma".equals(val) || "Ilya".equals(val), "Unexpected value: " + val);

        // Test with grouping.
        res = sql(appendDisabledRules("select any_value(name), salary from person group by salary order by salary", rules));

        assertEquals(2, res.size());

        val = res.get(0).get(0);

        assertTrue("Igor".equals(val) || "Roma".equals(val), "Unexpected value: " + val);

        val = res.get(1).get(0);

        assertEquals("Ilya", val);
    }

    @Test
    public void testColocatedAggregate() {
        String sql = "SELECT val1, count(val2) FROM t1_colo_val1 GROUP BY val1";

        assertQuery(sql)
                .matches(QueryChecker.matches(".*Exchange.*Colocated.*Aggregate.*"))
                .returns("val0", 50L)
                .returns("val1", 50L)
                .check();

        sql = "SELECT t2_colo_va1.val1, agg.cnt "
                + "FROM t2_colo_va1 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1_colo_val1 GROUP BY val1) "
                + "AS agg ON t2_colo_va1.val1 = agg.val1";

        assertQuery(sql)
                .disableRules("HashJoinConverter", "MergeJoinConverter")
                .matches(QueryChecker.matches(".*Exchange.*Join.*Colocated.*Aggregate.*"))
                .returns("val0", 50L)
                .returns("val1", 50L)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testColocatedAggregate(String[] rules) {
        String sql = "SELECT val1, count(val2) FROM t1_colo_val1 GROUP BY val1";

        assertQuery(sql)
                .disableRules(rules)
                .returns("val0", 50L)
                .returns("val1", 50L)
                .check();

        sql = "SELECT t2_colo_va1.val1, agg.cnt "
                + "FROM t2_colo_va1 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1_colo_val1 GROUP BY val1) "
                + "AS agg ON t2_colo_va1.val1 = agg.val1";

        assertQuery(sql)
                .disableRules(rules)
                .returns("val0", 50L)
                .returns("val1", 50L)
                .check();
    }

    @Test
    public void testEverySomeAggregate() {
        sql("DELETE FROM test_a_b_s");

        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (1, null, 0)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (2, 0, null)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (3, null, null)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (4, 0, 1)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (5, 1, 1)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (6, 1, 2)");
        sql("INSERT INTO test_a_b_s(id, a, b) VALUES (7, 2, 2)");

        assertQuery("SELECT EVERY(a < b) FROM test_a_b_s").returns(false).check();
        assertQuery("SELECT SOME(a < b) FROM test_a_b_s").returns(true).check();
        assertQuery("SELECT EVERY(a <= b) FROM test_a_b_s").returns(true).check();
        assertQuery("SELECT SOME(a > b) FROM test_a_b_s").returns(false).check();
    }

    @Test
    public void distinctAggregateWithoutAggregateFunction() {
        var sql = "select distinct name from person";

        assertQuery(sql)
                .matches(QueryChecker.matches(".*ReduceHashAggregate.*Exchange.*MapHashAggregate.*"))
                .returns("Igor")
                .returns("Ilya")
                .returns("Roma")
                .returns(null)
                .check();

        assertQuery(sql, AggregateType.HASH)
                .matches(QueryChecker.matches(".*ReduceHashAggregate.*Exchange.*MapHashAggregate.*"))
                .returns("Igor")
                .returns("Ilya")
                .returns("Roma")
                .returns(null)
                .check();

        assertQuery(sql, AggregateType.SORT)
                .matches(QueryChecker.matches(".*ReduceSortAggregate.*Exchange.*MapSortAggregate.*"))
                .returns("Igor")
                .returns("Ilya")
                .returns("Roma")
                .returns(null)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testDifferentAgg(String[] rules) {
        assertQuery("SELECT DISTINCT(a) as a FROM test_a_b_s ORDER BY a")
                .disableRules(rules)
                .returns(10)
                .returns(11)
                .returns(12)
                .returns(13)
                .check();

        assertQuery("SELECT COUNT(*) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L)
                .check();

        assertQuery("SELECT COUNT(a), COUNT(DISTINCT(b)) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L, 5L)
                .check();

        assertQuery("SELECT COUNT(a) as a, s FROM test_a_b_s GROUP BY s ORDER BY a, s")
                .disableRules(rules)
                .returns(1L, "ahello")
                .returns(1L, null)
                .returns(2L, "hello")
                .returns(3L, "world")
                .check();

        assertQuery("SELECT COUNT(a) as a, SUBSTRING(AVG(a)::VARCHAR, 1, 6) as b, MIN(a), MIN(b), s FROM test_a_b_s "
                + "GROUP BY s ORDER BY a, b")
                .disableRules(rules)
                .returns(1L, "10.000", 10, 5, "ahello")
                .returns(1L, "11.000", 11, 3, null)
                .returns(2L, "11.000", 11, 1, "hello")
                .returns(3L, "12.333", 12, 2, "world")
                .check();

        assertQuery("SELECT COUNT(a) as a, SUBSTRING(AVG(a)::VARCHAR, 1, 6) as bb, MIN(a), MIN(b), s FROM test_a_b_s "
                + "GROUP BY s, b ORDER BY a, s")
                .disableRules(rules)
                .returns(1L, "10.000", 10, 5, "ahello")
                .returns(1L, "11.000", 11, 1, "hello")
                .returns(1L, "11.000", 11, 3, "hello")
                .returns(1L, "13.000", 13, 6, "world")
                .returns(1L, "11.000", 11, 3, null)
                .returns(2L, "12.000", 12, 2, "world")
                .check();

        assertQuery("SELECT COUNT(a) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L)
                .check();

        assertQuery("SELECT COUNT(DISTINCT(a)) FROM test_a_b_s")
                .disableRules(rules)
                .returns(4L)
                .check();

        assertQuery("SELECT COUNT(a), COUNT(s), COUNT(*) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L, 6L, 7L)
                .check();

        assertQuery("SELECT SUBSTRING(AVG(a)::VARCHAR, 1, 6) FROM test_a_b_s")
                .disableRules(rules)
                .returns("11.428")
                .check();

        assertQuery("SELECT MIN(a) FROM test_a_b_s")
                .disableRules(rules)
                .returns(10)
                .check();

        assertQuery("SELECT COUNT(a), COUNT(DISTINCT(a)) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L, 4L)
                .check();

        assertQuery("SELECT COUNT(a), COUNT(DISTINCT a), SUM(a), SUM(DISTINCT a) FROM test_a_b_s")
                .disableRules(rules)
                .returns(7L, 4L, 80L, 46L)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void checkEmptyTable(String[] rules) {
        sql("DELETE FROM test_a_b_s");

        assertQuery("SELECT min(b) FROM test_a_b_s GROUP BY a")
                .disableRules(rules)
                .returnNothing().check();
    }

    @ParameterizedTest
    @MethodSource("rulesForGroupingSets")
    public void testGroupingSets(String[] rules) {
        sql("DELETE FROM test_str_int_real_dec");

        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (1, 's1', 10)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (2, 's1', 20)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (3, 's2', 10)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (4, 's3', 40)");

        assertQuery("SELECT str_col, SUM(int_col), COUNT(str_col) FROM test_str_int_real_dec GROUP BY GROUPING SETS "
                + "( (str_col, int_col), (str_col), (int_col), () ) HAVING SUM(int_col) > 0")
                .disableRules(rules)
                // empty group
                .returns(null, 80L, 4L)
                // group (str_col, int_col)
                .returns("s1", 10L, 1L)
                .returns("s1", 20L, 1L)
                .returns("s2", 10L, 1L)
                .returns("s3", 40L, 1L)
                // group (str_col)
                .returns("s1", 30L, 2L)
                .returns("s2", 10L, 1L)
                .returns("s3", 40L, 1L)
                // group (int_col)
                .returns(null, 40L, 1L)
                .returns(null, 20L, 2L)
                .returns(null, 20L, 1L)
                .check();
    }

    @ParameterizedTest
    @MethodSource("rulesForGroupingSets")
    public void testGroupingFunction(String[] rules) {
        sql("DELETE FROM test_str_int_real_dec");

        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (1, 's1', 10)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (2, 's1', 20)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (3, 's2', 10)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (4, 's3', 40)");

        assertQuery("SELECT GROUPING(str_col), str_col FROM test_str_int_real_dec GROUP BY GROUPING SETS ((str_col))")
                .disableRules(rules)
                .returns(1L, "s1")
                .returns(1L, "s2")
                .returns(1L, "s3")
                .check();

        assertQuery("SELECT GROUPING(str_col), str_col FROM test_str_int_real_dec GROUP BY GROUPING SETS ((str_col), (str_col))")
                .disableRules(rules)
                .returns(1L, "s1")
                .returns(1L, "s1")
                .returns(1L, "s2")
                .returns(1L, "s2")
                .returns(1L, "s3")
                .returns(1L, "s3")
                .check();

        assertQuery("SELECT GROUPING(int_col, str_col), GROUPING(int_col),"
                + "str_col, SUM(int_col), COUNT(str_col) FROM test_str_int_real_dec GROUP BY GROUPING SETS "
                + "( (str_col, int_col), (str_col), (int_col), () ) HAVING SUM(int_col) > 0")
                .disableRules(rules)
                // group (str_col, int_col)
                .returns(3L, 1L, "s1", 10L, 1L)
                .returns(3L, 1L, "s1", 20L, 1L)
                .returns(3L, 1L, "s2", 10L, 1L)
                .returns(3L, 1L, "s3", 40L, 1L)
                // group (str_col)
                .returns(1L, 0L, "s1", 30L, 2L)
                .returns(1L, 0L, "s2", 10L, 1L)
                .returns(1L, 0L, "s3", 40L, 1L)
                // group (int_col)
                .returns(2L, 1L, null, 20L, 2L)
                .returns(2L, 1L, null, 20L, 1L)
                .returns(2L, 1L, null, 40L, 1L)
                // empty group
                .returns(0L, 0L, null, 80L, 4L)
                .check();

        String invalidQuery = IgniteStringFormatter.format(
                "SELECT GROUPING({}), str_col FROM test_str_int_real_dec GROUP BY GROUPING SETS ((str_col))",
                IntStream.rangeClosed(1, 64).mapToObj(i -> "str_col").collect(Collectors.joining(",")));
        assertThrows(SqlException.class, () -> sql(invalidQuery),
                "Invalid number of arguments to function ''GROUPING''. Was expecting number of agruments in range [1, 63]");

    }

    @ParameterizedTest
    @MethodSource("rulesForGroupingSets")
    public void testDuplicateGroupingSets(String[] rules) {
        sql("DELETE FROM test_str_int_real_dec");

        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (1, 's1', 10)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (2, 's1', 20)");
        sql("INSERT INTO test_str_int_real_dec(id, str_col, int_col) VALUES (3, 's2', 10)");

        assertQuery("SELECT str_col  FROM test_str_int_real_dec GROUP BY GROUPING SETS ((str_col), (), (str_col), ()) ORDER BY str_col")
                .disableRules(rules)
                .returns("s1")
                .returns("s2")
                .returns(null)
                .returns("s1")
                .returns("s2")
                .returns(null)
                .check();
    }

    @SuppressWarnings("BigDecimalMethodWithoutRoundingCalled")
    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAvg(String[] rules) {
        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)");

        BigDecimal avgDec = ONE_AND_HALF_WITH_SCALE_16;
        BigDecimal avgDecBigScale = new BigDecimal("1.5").setScale(18, RoundingMode.UNNECESSARY);
        Double avgDouble = 1.5d;

        assertQuery("SELECT "
                + "AVG(tinyint_col), AVG(smallint_col), AVG(int_col), AVG(bigint_col), "
                + "AVG(float_col), AVG(double_col), AVG(dec2_col), AVG(dec4_2_col), AVG(dec20_18_col) "
                + "FROM numbers")
                .disableRules(rules)
                .returns(avgDec, avgDec, avgDec, avgDec, avgDouble, avgDouble, avgDec, avgDec, avgDecBigScale)
                .check();

        assertQuery("SELECT AVG(int_col) FILTER (WHERE smallint_col % 2 = 0)" 
                + "           , AVG(int_col) FILTER (WHERE smallint_col % 2 = 1)" 
                + "        FROM numbers")
                .disableRules(rules)
                .returns(new BigDecimal(2).setScale(16), new BigDecimal(1).setScale(16))
                .check();

        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers (id, dec4_2_col) VALUES (1, 1), (2, 2)");

        assertQuery("SELECT AVG(dec4_2_col) FROM numbers")
                .disableRules(rules)
                .returns(avgDec)
                .check();

        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers (id, dec4_2_col) VALUES (1, 1), (2, 2.3333)");

        assertQuery("SELECT AVG(dec4_2_col) FROM numbers")
                .disableRules(rules)
                .returns(new BigDecimal("1.665").setScale(16, RoundingMode.UNNECESSARY))
                .check();

        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers (id, int_col, dec4_2_col) VALUES (1, null, null)");

        assertQuery("SELECT AVG(int_col), AVG(dec4_2_col) FROM numbers")
                .disableRules(rules)
                .returns(null, null)
                .check();

        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers (id, int_col, dec4_2_col) VALUES (1, 1, 1), (2, null, null)");

        assertQuery("SELECT AVG(int_col), AVG(dec4_2_col) FROM numbers")
                .disableRules(rules)
                .returns(ONE_WITH_SCALE_16, ONE_WITH_SCALE_16)
                .check();
    }

    @Test
    public void testAvgRandom() {
        long seed = System.nanoTime();
        Random random = new Random(seed);

        sql("DELETE FROM numbers");

        List<BigDecimal> numbers = new ArrayList<>();
        log.info("Seed: {}", seed);

        for (int i = 1; i < 20; i++) {
            int val = random.nextInt(100) + 1;
            BigDecimal num = BigDecimal.valueOf(val);
            numbers.add(num);

            String query = "INSERT INTO numbers (id, int_col, dec10_2_col) VALUES(?, ?, ?)";
            sql(query, i, num.setScale(0, RoundingMode.HALF_UP).intValue(), num);
        }

        BigDecimal avg = numbers.stream()
                .reduce(new BigDecimal("0.00"), BigDecimal::add)
                .divide(BigDecimal.valueOf(numbers.size()), 16, IgniteTypeSystem.INSTANCE.roundingMode());

        for (String[] rules : makePermutations(PERMUTATION_RULES)) {
            assertQuery("SELECT AVG(int_col), AVG(dec10_2_col) FROM numbers")
                    .disableRules(rules)
                    .returns(avg, avg)
                    .check();
        }
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAvgNullNotNull(String[] rules) {
        sql("DELETE FROM not_null_numbers");
        sql("INSERT INTO not_null_numbers (id, int_col, dec4_2_col) VALUES (1, 1, 1), (2, 2, 2)");

        assertQuery("SELECT AVG(int_col), AVG(dec4_2_col) FROM not_null_numbers")
                .disableRules(rules)
                .returns(ONE_AND_HALF_WITH_SCALE_16, ONE_AND_HALF_WITH_SCALE_16)
                .check();

        // Return type of an AVG aggregate can never be null.
        assertQuery("SELECT AVG(int_col) FROM not_null_numbers GROUP BY int_col")
                .disableRules(rules)
                .returns(ONE_WITH_SCALE_16)
                .returns(TWO_WITH_SCALE_16)
                .check();

        assertQuery("SELECT AVG(dec4_2_col) FROM not_null_numbers GROUP BY dec4_2_col")
                .disableRules(rules)
                .returns(ONE_WITH_SCALE_16)
                .returns(TWO_WITH_SCALE_16)
                .check();

        sql("DELETE FROM numbers");
        sql("INSERT INTO numbers (id, int_col, dec4_2_col) VALUES (1, 1, 1), (2, 2, 2)");

        assertQuery("SELECT AVG(int_col) FROM numbers GROUP BY int_col")
                .disableRules(rules)
                .returns(ONE_WITH_SCALE_16)
                .returns(TWO_WITH_SCALE_16)
                .check();

        assertQuery("SELECT AVG(dec4_2_col) FROM numbers GROUP BY dec4_2_col")
                .disableRules(rules)
                .returns(ONE_WITH_SCALE_16)
                .returns(TWO_WITH_SCALE_16)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAvgOnEmptyGroup(String[] rules) {
        sql("DELETE FROM numbers");

        assertQuery("SELECT "
                + "AVG(tinyint_col), AVG(smallint_col), AVG(int_col), AVG(bigint_col), "
                + "AVG(float_col), AVG(double_col), AVG(dec2_col), AVG(dec4_2_col), AVG(dec20_18_col) "
                + "FROM numbers")
                .disableRules(rules)
                .returns(null, null, null, null, null, null, null, null, null)
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAvgFromLiterals(String[] rules) {
        BigDecimal avgDec = ONE_AND_HALF_WITH_SCALE_16;
        BigDecimal avgDecBigScale = new BigDecimal("1.5").setScale(18, RoundingMode.UNNECESSARY);
        Double avgDouble = 1.5d;

        assertQuery("SELECT "
                + "AVG(tinyint_col), AVG(smallint_col), AVG(int_col), AVG(bigint_col), AVG(float_col), " 
                + "AVG(double_col), AVG(dec2_col), AVG(dec4_2_col), AVG(dec20_18_col) "
                + "FROM (VALUES "
                + "(1::TINYINT, 1::SMALLINT, 1::INTEGER, 1::BIGINT, 1::REAL," 
                + " 1::DOUBLE, 1::DECIMAL(2), 1.00::DECIMAL(4,2), 1.00::DECIMAL(20,18)), "
                + "(2::TINYINT, 2::SMALLINT, 2::INTEGER, 2::BIGINT, 2::REAL," 
                + " 2::DOUBLE, 2::DECIMAL(2), 2.00::DECIMAL(4,2), 2.00::DECIMAL(20,18)) "
                + ") "
                + "t(tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, dec2_col, dec4_2_col, dec20_18_col)")
                .disableRules(rules)
                .returns(avgDec, avgDec, avgDec, avgDec, avgDouble, avgDouble, avgDec, avgDec, avgDecBigScale)
                .check();

        assertQuery("SELECT "
                + "AVG(1::TINYINT), AVG(2::SMALLINT), AVG(3::INTEGER), AVG(4::BIGINT), AVG(5::REAL), " 
                + "AVG(6::DOUBLE), AVG(7::DECIMAL(2)), AVG(8.00::DECIMAL(4,2)), AVG(9.00::DECIMAL(20,18))")
                .disableRules(rules)
                .returns(
                        ONE_WITH_SCALE_16,
                        TWO_WITH_SCALE_16,
                        new BigDecimal("3").setScale(16, RoundingMode.UNNECESSARY),
                        new BigDecimal("4").setScale(16, RoundingMode.UNNECESSARY),
                        5.0d,
                        6.0d,
                        new BigDecimal("7").setScale(16, RoundingMode.UNNECESSARY),
                        new BigDecimal("8").setScale(16, RoundingMode.UNNECESSARY),
                        new BigDecimal("9").setScale(18, RoundingMode.UNNECESSARY)
                )
                .check();

        assertQuery("SELECT AVG(dec2_col), AVG(dec4_2_col) FROM\n"
                + "(SELECT \n"
                + "  1::DECIMAL(2) as dec2_col, 2.00::DECIMAL(4, 2) as dec4_2_col\n"
                + "  UNION\n"
                + "  SELECT 2::DECIMAL(2) as dec2_col, 3.00::DECIMAL(4,2) as dec4_2_col\n"
                + ") as t")
                .returns(ONE_AND_HALF_WITH_SCALE_16, new BigDecimal("2.5").setScale(16, RoundingMode.UNNECESSARY))
                .check();
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAggDistinctGroupSet(String[] rules) {
        sql("DELETE FROM test_a_b_s");

        sql("INSERT INTO test_a_b_s (id, a, b) VALUES (1, 11, 2), (2, 12, 2), (3, 12, 3)");

        assertQuery("SELECT COUNT(a), COUNT(DISTINCT(b)) FROM test_a_b_s")
                .disableRules(rules)
                .returns(3L, 2L)
                .check();
    }

    @Test
    void testDivisionOfSumsOfDecimals() {
        assertQuery("SELECT sum(c1) / sum(c2) FROM (SELECT decimal '10.2382' AS c1, decimal '2.06' AS c2)")
                .returns(new BigDecimal("4.97000000000"))
                .check();
    }

    private static Stream<Arguments> rulesForGroupingSets() {
        List<Object[]> rules = Arrays.asList(
                // Use map/reduce aggregates for grouping sets
                new String[]{"ColocatedHashAggregateConverterRule", "ColocatedSortAggregateConverterRule"},

                // Use colocated aggregates grouping sets
                new String[]{"MapReduceHashAggregateConverterRule", "MapReduceSortAggregateConverterRule"}
        );

        return rules.stream().map(Object.class::cast).map(Arguments::of);
    }

    static String[][] makePermutations(String[] rules) {
        String[][] out = new String[rules.length][rules.length - 1];

        for (int i = 0; i < rules.length; ++i) {
            int pos = 0;
            for (int ruleIdx = 0; ruleIdx < rules.length; ++ruleIdx) {
                if (ruleIdx == i) {
                    continue;
                }
                out[i][pos++] = rules[ruleIdx];
            }
        }

        return out;
    }

    private static Stream<Arguments> provideRules() {
        return Arrays.stream(makePermutations(PERMUTATION_RULES)).map(Object.class::cast).map(Arguments::of);
    }

    private String appendDisabledRules(String sql, String[] rules) {
        sql = sql.toLowerCase(Locale.ENGLISH);
        int pos = sql.indexOf("select");

        assert pos >= 0;

        String newSql = sql.substring(0, pos + "select".length() + 1);
        newSql += HintUtils.toHint(IgniteHint.DISABLE_RULE, rules);
        newSql += sql.substring(pos + "select".length() + 1);
        return newSql;
    }
}
