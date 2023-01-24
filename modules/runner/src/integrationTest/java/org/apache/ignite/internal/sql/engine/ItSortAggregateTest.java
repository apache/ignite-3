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

import java.util.Locale;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Sort aggregate integration test.
 */
public class ItSortAggregateTest extends AbstractBasicIntegrationTest {
    private static final int ROWS = 103;

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        sql("CREATE TABLE test (id INT PRIMARY KEY, grp0 INT, grp1 INT, val0 INT, val1 INT) WITH replicas=2,partitions=10");
        sql("CREATE TABLE test_one_col_idx (pk INT PRIMARY KEY, col0 INT)");

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17304 uncomment this
        // sql("CREATE INDEX test_idx ON test(grp0, grp1)");
        // sql("CREATE INDEX test_one_col_idx_idx ON test_one_col_idx(col0)");

        for (int i = 0; i < ROWS; i++) {
            sql("INSERT INTO test (id, grp0, grp1, val0, val1) VALUES (?, ?, ?, ?, ?)", i, i / 10, i / 100, 1, 2);
            sql("INSERT INTO test_one_col_idx (pk, col0) VALUES (?, ?)", i, i);
        }
    }

    @Test
    public void mapReduceAggregate() {
        String disabledRules = " /*+ DISABLE_RULE('MapReduceHashAggregateConverterRule', 'ColocatedHashAggregateConverterRule', "
                + "'ColocatedSortAggregateConverterRule') */ ";

        var res = sql(
                appendDisabledRules("SELECT SUM(val0), SUM(val1), grp0 FROM TEST GROUP BY grp0 HAVING SUM(val1) > 10", disabledRules));

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            long s0 = (Long) r.get(0);
            long s1 = (Long) r.get(1);

            assertEquals(s0 * 2, s1);
        });
    }

    @Test
    public void correctCollationsOnMapReduceSortAgg() {
        String disabledRules = " /*+ DISABLE_RULE('MapReduceHashAggregateConverterRule', 'ColocatedHashAggregateConverterRule', "
                + "'ColocatedSortAggregateConverterRule') */ ";

        var cursors = sql(
                appendDisabledRules("SELECT PK FROM TEST_ONE_COL_IDX WHERE col0 IN (SELECT col0 FROM TEST_ONE_COL_IDX)", disabledRules));

        assertEquals(ROWS, cursors.size());
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testDifferentCollocatedSortAgg() {
        try {
            sql("CREATE TABLE testMe (a INTEGER, b INTEGER, s VARCHAR);");
            sql("INSERT INTO testMe VALUES (11, 1, 'hello'), (12, 2, 'world'), (11, 3, NULL)");
            sql("INSERT INTO testMe VALUES (11, 3, 'hello'), (12, 2, 'world'), (10, 5, 'ahello'), (13, 6, 'world')");

            String[] disabledRules = {"MapReduceHashAggregateConverterRule", "MapReduceSortAggregateConverterRule",
                    "ColocatedHashAggregateConverterRule"};

            assertQuery("SELECT DISTINCT(a) as a FROM testMe ORDER BY a")
                    .disableRules(disabledRules)
                    .returns(10)
                    .returns(11)
                    .returns(12)
                    .returns(13)
                    .check();

            assertQuery("SELECT COUNT(*) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(7L)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(DISTINCT(b)) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(7L, 5L)
                    .check();

            assertQuery("SELECT COUNT(a) as a, s FROM testMe GROUP BY s ORDER BY a, s")
                    .disableRules(disabledRules)
                    .returns(1L, "ahello")
                    .returns(1L, null)
                    .returns(2L, "hello")
                    .returns(3L, "world")
                    .check();

            assertQuery("SELECT COUNT(a) as a, AVG(a) as b, MIN(a), MIN(b), s FROM testMe GROUP BY s ORDER BY a, b")
                    .disableRules(disabledRules)
                    .returns(1L, 10, 10, 5, "ahello")
                    .returns(1L, 11, 11, 3, null)
                    .returns(2L, 11, 11, 1, "hello")
                    .returns(3L, 12, 12, 2, "world")
                    .check();

            assertQuery("SELECT COUNT(a) as a, AVG(a) as bb, MIN(a), MIN(b), s FROM testMe GROUP BY s, b ORDER BY a, s")
                    .disableRules(disabledRules)
                    .returns(1L, 10, 10, 5, "ahello")
                    .returns(1L, 11, 11, 1, "hello")
                    .returns(1L, 11, 11, 3, "hello")
                    .returns(1L, 13, 13, 6, "world")
                    .returns(1L, 11, 11, 3, null)
                    .returns(2L, 12, 12, 2, "world")
                    .check();

            assertQuery("SELECT COUNT(a) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(7L)
                    .check();

            assertQuery("SELECT COUNT(DISTINCT(a)) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(4L)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(s), COUNT(*) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(7L, 6L, 7L)
                    .check();

            assertQuery("SELECT AVG(a) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(11)
                    .check();

            assertQuery("SELECT MIN(a) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(10)
                    .check();

            assertQuery("SELECT COUNT(a), COUNT(DISTINCT(a)) FROM testMe")
                    .disableRules(disabledRules)
                    .returns(7L, 4L)
                    .check();
        } finally {
            sql("DROP TABLE testMe");
        }
    }

    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    @Test
    public void checkEmptyTable() {
        sql("CREATE TABLE t (a INTEGER, b INTEGER)");

        // Check ColocatedSortAggregate
        String disabledRules1 = " /*+ DISABLE_RULE('MapReduceHashAggregateConverterRule', 'MapReduceSortAggregateConverterRule', "
                + "'ColocatedHashAggregateConverterRule') */ ";

        // Check MapReduceSortAggregate
        String disabledRules2 = " /*+ DISABLE_RULE('MapReduceHashAggregateConverterRule', 'ColocatedSortAggregateConverterRule', "
                + "'ColocatedHashAggregateConverterRule') */ ";

        try {
            for (String disabledRules : List.of(disabledRules1, disabledRules2)) {
                assertQuery(appendDisabledRules("SELECT min(b) FROM t GROUP BY a", disabledRules))
                        .returnNothing().check();
            }
        } finally {
            sql("DROP TABLE t");
        }
    }

    private String appendDisabledRules(String sql, String rules) {
        sql = sql.toLowerCase(Locale.ENGLISH);
        int pos = sql.indexOf("select");

        assert pos >= 0;

        String newSql = sql.substring(0, pos + "select".length() + 1);
        newSql += rules;
        newSql += sql.substring(pos + "select".length() + 1);
        return newSql;
    }
}
