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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Group of tests that still has not been sorted out. It’s better to avoid extending this class with new tests.
 */
public class ItMixedQueriesTest extends BaseSqlIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createTable("EMP1");
        createTable("EMP2");

        int idx = 0;
        insertData("EMP1", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, "Igor", 11d},
                {idx++, "Igor", 12d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx, "Roman", 14d}
        });

        idx = 0;
        insertData("EMP2", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Roman", 10d},
                {idx++, "Roman", 11d},
                {idx++, "Roman", 12d},
                {idx++, "Roman", 13d},
                {idx++, "Igor1", 13d},
                {idx, "Igor1", 13d}
        });

        /*
        select * from emp1;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Igor  |   10  |
        |  2 | Igor  |   11  |
        |  3 | Igor  |   12  |
        |  4 | Igor1 |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        |  7 | Roman |   14  |
        +----+-------+-------+

        select * from emp2;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Roman |   10  |
        |  2 | Roman |   11  |
        |  3 | Roman |   12  |
        |  4 | Roman |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        +----+-------+-------+
         */
    }

    /** Tests varchar min\max aggregates. */
    @Test
    public void testVarCharMinMax() {
        sql("CREATE TABLE TEST(val VARCHAR primary key, val1 integer);");
        sql("INSERT INTO test VALUES ('б', 1), ('бб', 2), ('щ', 3), ('щщ', 4), ('Б', 4), ('ББ', 4), ('Я', 4);");
        var rows = sql("SELECT MAX(val), MIN(val) FROM TEST");

        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("щщ", "Б"), first(rows));

        sql("DROP TABLE IF EXISTS TEST");
    }

    @Test
    public void testOrderingByColumnOutsideSelectList() {
        assertQuery("select salary from emp2 order by id desc")
                .returns(13d)
                .returns(13d)
                .returns(13d)
                .returns(12d)
                .returns(11d)
                .returns(10d)
                .check();

        assertQuery("select name, sum(salary) from emp2 group by name order by count(salary)")
                .returns("Roman", 46d)
                .returns("Igor1", 26d)
                .check();
    }

    @Test
    public void testEqConditionWithDistinctSubquery() {
        var rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT DISTINCT(salary) FROM emp2 WHERE name='Igor1')");

        assertEquals(3, rows.size());
    }

    @Test
    public void testEqConditionWithAggregateSubqueryMax() {
        var rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT MAX(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(3, rows.size());
    }

    @Test
    public void testEqConditionWithAggregateSubqueryMin() {
        var rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT MIN(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(1, rows.size());
    }

    @Test
    public void testInConditionWithSubquery() {
        var rows = sql(
                "SELECT name FROM emp1 WHERE name IN (SELECT name FROM emp2)");

        assertEquals(4, rows.size());
    }

    @Test
    public void testDistinctQueryWithInConditionWithSubquery() {
        var rows = sql("SELECT distinct(name) FROM emp1 o WHERE name IN ("
                + "   SELECT name"
                + "   FROM emp2)");

        assertEquals(2, rows.size());
    }

    @Test
    public void testNotInConditionWithSubquery() {
        var rows = sql(
                "SELECT name FROM emp1 WHERE name NOT IN (SELECT name FROM emp2)");

        assertEquals(3, rows.size());
    }

    @Test
    public void testExistsConditionWithSubquery() {
        var rows = sql("SELECT name FROM emp1 o WHERE EXISTS ("
                + "   SELECT 1"
                + "   FROM emp2 a"
                + "   WHERE o.name = a.name)");

        assertEquals(4, rows.size());
    }

    @Test
    public void testNotExistsConditionWithSubquery() {
        var rows = sql("SELECT name FROM emp1 o WHERE NOT EXISTS ("
                + "   SELECT 1"
                + "   FROM emp2 a"
                + "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql("SELECT name FROM emp1 o WHERE NOT EXISTS ("
                + "   SELECT name"
                + "   FROM emp2 a"
                + "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql("SELECT distinct(name) FROM emp1 o WHERE NOT EXISTS ("
                + "   SELECT name"
                + "   FROM emp2 a"
                + "   WHERE o.name = a.name)");

        assertEquals(1, rows.size());
    }

    /**
     * Verifies that table modification events are passed to a calcite schema modification listener.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    public void testIgniteSchemaAwaresAlterTableCommand(int nodeToExecuteSelectsIndex) {
        Ignite nodeToExecuteSelects = CLUSTER.node(nodeToExecuteSelectsIndex);

        String selectAllQry = "select * from test_tbl";

        sql(0, "drop table if exists test_tbl");
        sql(0, "create table test_tbl(id int primary key, val varchar)");

        assertQuery(nodeToExecuteSelects, selectAllQry).columnNames("ID", "VAL").check();

        sql(0, "alter table test_tbl add column new_col int");

        assertQuery(nodeToExecuteSelects, selectAllQry).columnNames("ID", "VAL", "NEW_COL").check();

        // column with such name already exists
        assertThrows(Exception.class, () -> sql(0, "alter table test_tbl add column new_col int"));

        assertQuery(nodeToExecuteSelects, selectAllQry).columnNames("ID", "VAL", "NEW_COL").check();

        sql(0, "alter table test_tbl drop column new_col");

        assertQuery(nodeToExecuteSelects, selectAllQry).columnNames("ID", "VAL").check();

        // column with such name is not exists
        assertThrows(Exception.class, () -> sql(0, "alter table test_tbl drop column new_col"));

        assertQuery(nodeToExecuteSelects, selectAllQry).columnNames("ID", "VAL").check();
    }

    /** Quantified predicates test. */
    @Test
    public void quantifiedCompTest() {
        assertQuery("select salary from emp2 where salary > SOME (10, 11) ORDER BY salary")
                .returns(11d)
                .returns(12d)
                .returns(13d)
                .returns(13d)
                .returns(13d)
                .check();

        assertQuery("select salary from emp2 where salary < SOME (12, 12) ORDER BY salary")
                .returns(10d)
                .returns(11d)
                .check();

        assertQuery("select salary from emp2 where salary < ANY (11, 12) ORDER BY salary")
                .returns(10d)
                .returns(11d)
                .check();

        assertQuery("select salary from emp2 where salary > ANY (12, 13) ORDER BY salary")
                .returns(13d)
                .returns(13d)
                .returns(13d)
                .check();

        assertQuery("select salary from emp2 where salary <> ALL (12, 13) ORDER BY salary")
                .returns(10d)
                .returns(11d)
                .check();
    }

    /**
     * Checks bang equal is allowed and works.
     */
    @Test
    public void testBangEqual() {
        assertEquals(4, sql("SELECT * FROM EMP1 WHERE name != ?", "Igor").size());
    }

    /**
     * Test verifies that 1) proper indexes will be chosen for queries with different kinds of ordering, and 2) result set returned will be
     * sorted as expected.
     */
    @Test
    public void testSelectWithOrdering() throws InterruptedException {
        sql("drop table if exists test_tbl");
        sql("create table test_tbl (k1 int primary key, c1 int)");

        sql("create index idx_asc on test_tbl (c1)");
        sql("create index idx_desc on test_tbl (c1 desc)");

        sql("insert into test_tbl values (1, 1), (2, 2), (3, 3), (4, null)");

        assertQuery("select c1 from test_tbl ORDER BY c1")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
                .matches(not(containsSubPlan("Sort")))
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .returns(null)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 asc nulls first")
                .matches(containsSubPlan("Sort"))
                .ordered()
                .returns(null)
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 asc nulls last")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
                .matches(not(containsSubPlan("Sort")))
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .returns(null)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
                .matches(not(containsSubPlan("Sort")))
                .ordered()
                .returns(null)
                .returns(3)
                .returns(2)
                .returns(1)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc nulls first")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
                .matches(not(containsSubPlan("Sort")))
                .ordered()
                .returns(null)
                .returns(3)
                .returns(2)
                .returns(1)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc nulls last")
                .matches(containsSubPlan("Sort"))
                .ordered()
                .returns(3)
                .returns(2)
                .returns(1)
                .returns(null)
                .check();
    }

    private static void createTable(String tableName) {
        sql("CREATE TABLE " + tableName + "(id INT PRIMARY KEY, name VARCHAR, salary DOUBLE)");
    }
}
