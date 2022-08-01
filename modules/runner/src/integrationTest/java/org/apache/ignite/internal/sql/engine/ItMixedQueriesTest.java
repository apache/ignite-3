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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Group of tests that still has not been sorted out. It’s better to avoid extending this class with new tests.
 */
public class ItMixedQueriesTest extends AbstractBasicIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        Table emp1 = createTable("EMP1");
        Table emp2 = createTable("EMP2");

        int idx = 0;
        insertData(emp1, new String[]{"ID", "NAME", "SALARY"}, new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, "Igor", 11d},
                {idx++, "Igor", 12d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx++, "Igor1", 13d},
                {idx, "Roman", 14d}
        });

        idx = 0;
        insertData(emp2, new String[]{"ID", "NAME", "SALARY"}, new Object[][]{
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
    //@Test
    public void testVarCharMinMax() {
        sql("CREATE TABLE TEST(val VARCHAR primary key, val1 integer);");
        sql("INSERT INTO test VALUES ('б', 1), ('бб', 2), ('щ', 3), ('щщ', 4), ('Б', 4), ('ББ', 4), ('Я', 4);");
        List<List<?>> rows = sql("SELECT MAX(val), MIN(val) FROM TEST");

        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("щщ", "Б"), first(rows));
    }

    //@Test
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

    //@Test
    public void testEqConditionWithDistinctSubquery() {
        List<List<?>> rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT DISTINCT(salary) FROM emp2 WHERE name='Igor1')");

        assertEquals(3, rows.size());
    }

    //@Test
    public void testEqConditionWithAggregateSubqueryMax() {
        List<List<?>> rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT MAX(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(3, rows.size());
    }

    //@Test
    public void testEqConditionWithAggregateSubqueryMin() {
        List<List<?>> rows = sql(
                "SELECT name FROM emp1 WHERE salary = (SELECT MIN(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(1, rows.size());
    }

    //@Test
    public void testInConditionWithSubquery() {
        List<List<?>> rows = sql(
                "SELECT name FROM emp1 WHERE name IN (SELECT name FROM emp2)");

        assertEquals(4, rows.size());
    }

    //@Test
    public void testDistinctQueryWithInConditionWithSubquery() {
        List<List<?>> rows = sql("SELECT distinct(name) FROM emp1 o WHERE name IN ("
                + "   SELECT name"
                + "   FROM emp2)");

        assertEquals(2, rows.size());
    }

    //@Test
    public void testNotInConditionWithSubquery() {
        List<List<?>> rows = sql(
                "SELECT name FROM emp1 WHERE name NOT IN (SELECT name FROM emp2)");

        assertEquals(3, rows.size());
    }

    //@Test
    public void testExistsConditionWithSubquery() {
        List<List<?>> rows = sql("SELECT name FROM emp1 o WHERE EXISTS ("
                + "   SELECT 1"
                + "   FROM emp2 a"
                + "   WHERE o.name = a.name)");

        assertEquals(4, rows.size());
    }

    //@Test
    public void testNotExistsConditionWithSubquery() {
        List<List<?>> rows = sql("SELECT name FROM emp1 o WHERE NOT EXISTS ("
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

    //@Test
    public void testSequentialInserts() {
        sql("CREATE TABLE t(x INTEGER PRIMARY KEY, y int)");

        for (int i = 0; i < 10_000; i++) {
            sql("INSERT INTO t VALUES (?,?)", i, i);
        }

        assertEquals(10_000L, sql("SELECT count(*) FROM t").get(0).get(0));
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand1() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand2() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand3() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand4() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand5() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand6() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand7() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand8() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand9() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand10() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand11() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand12() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand13() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand14() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand15() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand16() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand17() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand18() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand19() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand20() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand21() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand22() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand23() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand24() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand25() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand26() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand27() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand28() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand29() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand30() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand31() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand32() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand33() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand34() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand35() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand36() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand37() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand38() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand39() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand40() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand41() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand42() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand43() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand44() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand45() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand46() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand47() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand48() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand49() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand50() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand51() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand52() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand53() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand54() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand55() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand56() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand57() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand58() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand59() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand60() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand61() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand62() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand63() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand64() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand65() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand66() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand67() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand68() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand69() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand70() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand71() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand72() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand73() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand74() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand75() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand76() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand77() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand78() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand79() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand80() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand81() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand82() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand83() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand84() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand85() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand86() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand87() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand88() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand89() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand90() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand91() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand92() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand93() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand94() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand95() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand96() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand97() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand98() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand99() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    @Test
    public void testIgniteSchemaAwaresAlterTableCommand100() {
        testIgniteSchemaAwaresAlterTableCommand();
    }

    /**
     * Verifies that table modification events are passed to a calcite schema modification listener.
     */
//    @Test
    public void testIgniteSchemaAwaresAlterTableCommand() {//qqq
        String selectAllQry = "select * from test_tbl";

        sql("drop table if exists test_tbl");
        sql("create table test_tbl(id int primary key, val varchar)");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        sql("alter table test_tbl add column new_col int");

        assertQuery(selectAllQry).columnNames("ID", "VAL", "NEW_COL").check();

        // column with such name already exists
        assertThrows(Exception.class, () -> sql("alter table test_tbl add column new_col int"));

        assertQuery(selectAllQry).columnNames("ID", "VAL", "NEW_COL").check();

        sql("alter table test_tbl add column if not exists new_col int");

        assertQuery(selectAllQry).columnNames("ID", "VAL", "NEW_COL").check();

        sql("alter table test_tbl drop column new_col");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        // column with such name is not exists
        assertThrows(Exception.class, () -> sql("alter table test_tbl drop column new_col"));

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        sql("alter table test_tbl drop column if exists new_col");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();
    }

    /** Quantified predicates test. */
    //@Test
    public void quantifiedCompTest() throws InterruptedException {
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
    //@Test
    public void testBangEqual() {
        assertEquals(4, sql("SELECT * FROM EMP1 WHERE name != ?", "Igor").size());
    }

    /**
     * Test verifies that
     * 1) proper indexes will be chosen for queries with different kinds of ordering, and
     * 2) result set returned will be sorted as expected.
     */
    //@Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-14925")
    public void testSelectWithOrdering() {
        // sql("drop table if exists test_tbl", true);
        // sql("create table test_tbl (c1 int)", true);
        // sql("insert into test_tbl values (1), (2), (3), (null)", true);

        // sql("create index idx_asc on test_tbl (c1)", true);
        // sql("create index idx_desc on test_tbl (c1 desc)", true);

        assertQuery("select c1 from test_tbl ORDER BY c1")
                // .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
                // .matches(not(containsSubPlan("IgniteSort")))
                .ordered()
                .returns(new Object[]{null})
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 asc nulls first")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_ASC"))
                .matches(not(containsSubPlan("IgniteSort")))
                .ordered()
                .returns(new Object[]{null})
                .returns(1)
                .returns(2)
                .returns(3)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 asc nulls last")
                .matches(containsSubPlan("IgniteSort"))
                .ordered()
                .returns(1)
                .returns(2)
                .returns(3)
                .returns(new Object[]{null})
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
                .matches(not(containsSubPlan("IgniteSort")))
                .ordered()
                .returns(3)
                .returns(2)
                .returns(1)
                .returns(new Object[]{null})
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc nulls first")
                .matches(containsSubPlan("IgniteSort"))
                .ordered()
                .returns(new Object[]{null})
                .returns(3)
                .returns(2)
                .returns(1)
                .check();

        assertQuery("select c1 from test_tbl ORDER BY c1 desc nulls last")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL", "IDX_DESC"))
                .matches(not(containsSubPlan("IgniteSort")))
                .ordered()
                .returns(3)
                .returns(2)
                .returns(1)
                .returns(new Object[]{null})
                .check();
    }

    private static Table createTable(String tableName) {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", tableName)
                .columns(
                        SchemaBuilders.column("ID", ColumnType.INT32).build(),
                        SchemaBuilders.column("NAME", ColumnType.string()).asNullable(true).build(),
                        SchemaBuilders.column("SALARY", ColumnType.DOUBLE).asNullable(true).build()
                )
                .withPrimaryKey("ID")
                .build();

        return CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schTbl1, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        );
    }
}
