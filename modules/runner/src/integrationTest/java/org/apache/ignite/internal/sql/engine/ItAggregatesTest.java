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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.util.HintUtils;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Group of tests to verify aggregation functions.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-20114")
public class ItAggregatesTest extends ClusterPerClassIntegrationTest {
    private static final String[] DISABLED_RULES = {"MapReduceHashAggregateConverterRule", "MapReduceSortAggregateConverterRule",
            "ColocatedHashAggregateConverterRule", "ColocatedSortAggregateConverterRule"};

    private static final List<String> MAP_REDUCE_RULES = List.of("MapReduceHashAggregateConverterRule",
            "MapReduceSortAggregateConverterRule");

    private static final List<String> COLO_RULES = Arrays.stream(DISABLED_RULES).filter(r -> !MAP_REDUCE_RULES.contains(r))
            .collect(Collectors.toList());

    private static final int ROWS = 103;

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() throws InterruptedException {
        createAndPopulateTable();

        sql("CREATE ZONE test_zone with replicas=2, partitions=10");
        sql("CREATE TABLE test (id INT PRIMARY KEY, grp0 INT, grp1 INT, val0 INT, val1 INT) WITH PRIMARY_ZONE='TEST_ZONE'");
        sql("CREATE TABLE test_one_col_idx (pk INT PRIMARY KEY, col0 INT)");

        sql("CREATE INDEX test_idx ON test(grp0, grp1)");
        sql("CREATE INDEX test_one_col_idx_idx ON test_one_col_idx(col0)");

        for (int i = 0; i < ROWS; i++) {
            sql("INSERT INTO test (id, grp0, grp1, val0, val1) VALUES (?, ?, ?, ?, ?)", i, i / 10, i / 100, 1, 2);
            sql("INSERT INTO test_one_col_idx (pk, col0) VALUES (?, ?)", i, i);
        }
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

        if (Arrays.stream(rules).noneMatch(MAP_REDUCE_RULES::contains)) {
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
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testMultipleRowsFromSingleAggr(String[] rules) {
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
        try {
            sql("CREATE TABLE t1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                    + "COLOCATE BY (val1)");

            sql("CREATE TABLE t2(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                    + "COLOCATE BY (val1)");

            for (int i = 0; i < 100; i++) {
                sql("INSERT INTO t1 VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i % 2, "val" + i);
            }

            sql("INSERT INTO t2 VALUES (0, 'val0', 'val0', 'val0'), (1, 'val1', 'val1', 'val1')");

            String sql = "SELECT val1, count(val2) FROM t1 GROUP BY val1";

            assertQuery(sql)
                    .matches(QueryChecker.matches(".*Exchange.*Colocated.*Aggregate.*"))
                    .returns("val0", 50L)
                    .returns("val1", 50L)
                    .check();

            sql = "SELECT t2.val1, agg.cnt "
                    + "FROM t2 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1 GROUP BY val1) AS agg ON t2.val1 = agg.val1";

            assertQuery(sql)
                    .matches(QueryChecker.matches(".*Exchange.*Join.*Colocated.*Aggregate.*"))
                    .returns("val0", 50L)
                    .returns("val1", 50L)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t1");
            sql("DROP TABLE IF EXISTS t2");
        }
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testColocatedAggregate(String[] rules) {
        try {
            sql("CREATE TABLE t1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                    + "COLOCATE BY (val1)");

            sql("CREATE TABLE t2(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) "
                    + "COLOCATE BY (val1)");

            for (int i = 0; i < 100; i++) {
                sql("INSERT INTO t1 VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i % 2, "val" + i);
            }

            sql("INSERT INTO t2 VALUES (0, 'val0', 'val0', 'val0'), (1, 'val1', 'val1', 'val1')");

            String sql = "SELECT val1, count(val2) FROM t1 GROUP BY val1";

            assertQuery(sql)
                    .disableRules(rules)
                    .returns("val0", 50L)
                    .returns("val1", 50L)
                    .check();

            sql = "SELECT t2.val1, agg.cnt "
                    + "FROM t2 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1 GROUP BY val1) AS agg ON t2.val1 = agg.val1";

            assertQuery(sql)
                    .disableRules(rules)
                    .returns("val0", 50L)
                    .returns("val1", 50L)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS t1");
            sql("DROP TABLE IF EXISTS t2");
        }
    }

    @Test
    public void testEverySomeAggregate() {
        sql("CREATE TABLE t(c0 INT PRIMARY KEY, c1 INT, c2 INT)");
        sql("INSERT INTO t VALUES (1, null, 0)");
        sql("INSERT INTO t VALUES (2, 0, null)");
        sql("INSERT INTO t VALUES (3, null, null)");
        sql("INSERT INTO t VALUES (4, 0, 1)");
        sql("INSERT INTO t VALUES (5, 1, 1)");
        sql("INSERT INTO t VALUES (6, 1, 2)");
        sql("INSERT INTO t VALUES (7, 2, 2)");

        assertQuery("SELECT EVERY(c1 < c2) FROM t").returns(false).check();
        assertQuery("SELECT SOME(c1 < c2) FROM t").returns(true).check();
        assertQuery("SELECT EVERY(c1 <= c2) FROM t").returns(true).check();
        assertQuery("SELECT SOME(c1 > c2) FROM t").returns(false).check();
    }

    @Test
    public void distinctAggregateWithoutAggregateFunction() {
        var sql = "select distinct name from person";

        assertQuery(sql)
                .matches(QueryChecker.matches(".*Colocated.*Aggregate.*Exchange.*"))
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
        try {
            sql("CREATE TABLE testMe (a INTEGER, b INTEGER, s VARCHAR);");
            sql("INSERT INTO testMe VALUES (11, 1, 'hello'), (12, 2, 'world'), (11, 3, NULL)");
            sql("INSERT INTO testMe VALUES (11, 3, 'hello'), (12, 2, 'world'), (10, 5, 'ahello'), (13, 6, 'world')");

            assertQuery("SELECT DISTINCT(a) as a FROM testMe ORDER BY a")
                    .disableRules(rules)
                    .returns(10)
                    .returns(11)
                    .returns(12)
                    .returns(13)
                    .check();

            assertQuery("SELECT COUNT(*) FROM testMe")
                    .disableRules(rules)
                    .returns(7L)
                    .check();

            // Such kind of queries can`t be processed with
            if (Arrays.stream(rules).anyMatch(r -> r.contains("MapReduceSortAggregateConverterRule"))) {
                assertQuery("SELECT COUNT(a), COUNT(DISTINCT(b)) FROM testMe")
                        .disableRules(rules)
                        .returns(7L, 5L)
                        .check();
            }

            assertQuery("SELECT COUNT(a) as a, s FROM testMe GROUP BY s ORDER BY a, s")
                    .disableRules(rules)
                    .returns(1L, "ahello")
                    .returns(1L, null)
                    .returns(2L, "hello")
                    .returns(3L, "world")
                    .check();

            if (Arrays.stream(rules).noneMatch(MAP_REDUCE_RULES::contains)) {
                assertQuery("SELECT COUNT(a) as a, AVG(a) as b, MIN(a), MIN(b), s FROM testMe GROUP BY s ORDER BY a, b")
                        .disableRules(rules)
                        .returns(1L, 10, 10, 5, "ahello")
                        .returns(1L, 11, 11, 3, null)
                        .returns(2L, 11, 11, 1, "hello")
                        .returns(3L, 12, 12, 2, "world")
                        .check();

                assertQuery("SELECT COUNT(a) as a, AVG(a) as bb, MIN(a), MIN(b), s FROM testMe GROUP BY s, b ORDER BY a, s")
                        .disableRules(rules)
                        .returns(1L, 10, 10, 5, "ahello")
                        .returns(1L, 11, 11, 1, "hello")
                        .returns(1L, 11, 11, 3, "hello")
                        .returns(1L, 13, 13, 6, "world")
                        .returns(1L, 11, 11, 3, null)
                        .returns(2L, 12, 12, 2, "world")
                        .check();
            }


            assertQuery("SELECT COUNT(a) FROM testMe")
                    .disableRules(rules)
                    .returns(7L)
                    .check();

            assertQuery("SELECT COUNT(DISTINCT(a)) FROM testMe")
                    .disableRules(rules)
                    .returns(4L)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(s), COUNT(*) FROM testMe")
                    .disableRules(rules)
                    .returns(7L, 6L, 7L)
                    .check();

            if (Arrays.stream(rules).noneMatch(MAP_REDUCE_RULES::contains)) {
                assertQuery("SELECT AVG(a) FROM testMe")
                        .disableRules(rules)
                        .returns(11)
                        .check();
            }

            assertQuery("SELECT MIN(a) FROM testMe")
                    .disableRules(rules)
                    .returns(10)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(DISTINCT(a)) FROM testMe")
                    .disableRules(rules)
                    .returns(7L, 4L)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(DISTINCT a), SUM(a), SUM(DISTINCT a) FROM testMe")
                    .disableRules(rules)
                    .returns(7L, 4L, 80L, 46L)
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS testMe");
        }
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void checkEmptyTable(String[] rules) {
        sql("CREATE TABLE t (a INTEGER, b INTEGER)");

        try {
            assertQuery("SELECT min(b) FROM t GROUP BY a")
                    .disableRules(rules)
                    .returnNothing().check();
        } finally {
            sql("DROP TABLE t");
        }
    }

    @ParameterizedTest
    @MethodSource("rulesForGroupingSets")
    public void testGroupingSets(String[] rules) {
        try {
            sql("CREATE TABLE test1 (id INTEGER PRIMARY KEY, str_col VARCHAR, int_col INTEGER);");
            sql("INSERT INTO test1 VALUES (1, 's1', 10)");
            sql("INSERT INTO test1 VALUES (2, 's1', 20)");
            sql("INSERT INTO test1 VALUES (3, 's2', 10)");
            sql("INSERT INTO test1 VALUES (4, 's3', 40)");

            assertQuery("SELECT str_col, SUM(int_col), COUNT(str_col) FROM test1 GROUP BY GROUPING SETS "
                    + "( (str_col, int_col), (str_col), (int_col), () ) HAVING SUM(int_col) > 0")
                    .disableRules(rules)
                    .returns(null, 80L, 4L)
                    .returns("s1", 10L, 1L)
                    .returns("s3", 40L, 1L)
                    .returns("s1", 20L, 1L)
                    .returns("s2", 10L, 1L)
                    .returns("s2", 10L, 1L)
                    .returns("s3", 40L, 1L)
                    .returns("s1", 30L, 2L)
                    .returns(null, 40L, 1L)
                    .returns(null, 20L, 2L)
                    .returns(null, 20L, 1L)
                    .check();

        } finally {
            sql("DROP TABLE test1");
        }
    }

    @ParameterizedTest
    @MethodSource("rulesForGroupingSets")
    public void testDuplicateGroupingSets(String[] rules) {
        try {
            sql("CREATE TABLE test1 (id INTEGER PRIMARY KEY, str_col VARCHAR, int_col INTEGER);");
            sql("INSERT INTO test1 VALUES (1, 's1', 10)");
            sql("INSERT INTO test1 VALUES (2, 's1', 20)");
            sql("INSERT INTO test1 VALUES (3, 's2', 10)");

            assertQuery("SELECT str_col  FROM test1 GROUP BY GROUPING SETS ((str_col), (), (str_col), ()) ORDER BY str_col")
                    .disableRules(rules)
                    .returns("s1")
                    .returns("s2")
                    .returns(null)
                    .returns("s1")
                    .returns("s2")
                    .returns(null)
                    .check();
        } finally {
            sql("DROP TABLE test1");
        }
    }

    @ParameterizedTest
    @MethodSource("provideRules")
    public void testAvgOnEmptyGroup(String[] rules) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-20009
        //  Remove after is fixed.
        Assumptions.assumeFalse(Arrays.stream(rules)
                .filter(COLO_RULES::contains).count() == COLO_RULES.size(), "AVG is disabled for MAP/REDUCE");

        try {
            sql("CREATE TABLE test1 (id INTEGER PRIMARY KEY, str_col VARCHAR, int_col INTEGER, real_col REAL, dec_col DECIMAL);");

            assertQuery("SELECT AVG(int_col) FROM test1")
                    .disableRules(rules)
                    .returns(new Object[]{null})
                    .check();

            assertQuery("SELECT AVG(real_col) FROM test1")
                    .disableRules(rules)
                    .returns(new Object[]{null})
                    .check();

            assertQuery("SELECT AVG(dec_col) FROM test1")
                    .disableRules(rules)
                    .returns(new Object[]{null})
                    .check();
        } finally {
            sql("DROP TABLE test1");
        }
    }

    private static Stream<Arguments> rulesForGroupingSets() {
        List<Object[]> rules = Arrays.asList(
                // Use map/reduce aggregates for grouping sets
                new String[]{"ColocatedHashAggregateConverterRule", "ColocatedSortAggregateConverterRule"},

                // Use colocated aggregates grouping sets
                new String[]{"MapReduceHashAggregateConverterRule", "ColocatedSortAggregateConverterRule"}
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
        return Arrays.stream(makePermutations(DISABLED_RULES)).map(Object.class::cast).map(Arguments::of);
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
