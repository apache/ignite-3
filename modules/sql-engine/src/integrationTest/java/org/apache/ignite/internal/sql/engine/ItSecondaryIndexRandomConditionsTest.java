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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests random conditions.
 */
public class ItSecondaryIndexRandomConditionsTest extends BaseSqlIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    @BeforeAll
    static void initTestData() {
        sql("CREATE ZONE IN_MEM_ZONE ENGINE aimem");
    }

    @Test
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testRandomConditions() {
        sql("create table test0(a int, b int, c int) WITH PRIMARY_ZONE='IN_MEM_ZONE'");
        sql("create index idx_1 on test0(a)");
        sql("create index idx_2 on test0(b, a)");

        sql("create table test1(a int, b int, c int) WITH PRIMARY_ZONE='IN_MEM_ZONE'");

        sql("insert into test0 select x / 100, "
                + "mod(x / 10, 10), mod(x, 10) from table(system_range(0, 999))");

        sql("update test0 set a = null where a = 9");
        sql("update test0 set b = null where b = 9");
        sql("update test0 set c = null where c = 9");
        sql("insert into test1 select * from test0");

        doRandomQueries(500);
    }

    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    @ParameterizedTest
    @ValueSource(strings = {"a,b,c", "a,c,b", "b,a,c", "b,c,a", "c,a,b", "c,b,a"})
    public void testRandomQueriesWithIndexFieldsPermutations(String idxFields) {
        sql("create table test0(a int, b int, c int, d int) WITH PRIMARY_ZONE='IN_MEM_ZONE'");
        sql("create index idx_2 on test0(" + idxFields + ")");
        sql("create table test1(a int, b int, c int, d int) WITH PRIMARY_ZONE='IN_MEM_ZONE'");
        sql("insert into test0 select x / 100, "
                + "mod(x / 10, 10), mod(x, 10), x from table(system_range(0, 999))");

        sql("update test0 set a = null where a = 9");
        sql("update test0 set b = null where b = 9");
        sql("update test0 set c = null where c = 9");
        sql("insert into test1 select * from test0");

        String[] predefinedConditions = {
                "a >=1 and c =2 and a in(1,0,0)",
                "a >=1 and c =2 and a in(1,0,0) and c in (1,3)",
                "a = 1 and b in(10, 2)",
                "b =1 and a in(0,3,4,7,0) and c =2",
                "b >0 and a in(0,7,3,4) and c >0",
                "b in (1,2) and c >=2",
                "b in (select b from test1 where c >=1 and a =0) and c >=2",
                "b in (select b from test1 where c >=1 and a =0) and c =2",
                "a in (1,2) and b in (select b from test1 where c >=1 and a =0) and c >=0",
                // IN(query).
                "b =1 and a in(select b from test1 where c >=1 and a =0) and c =2",
                "b >0 and a in(select b from test1 where c >=1 and a =0) and c >0",
                // IN(const) + IN(query).
                "b in (1,2) and a in(select b from test1 where c >=1 and a =0) and c =2",
                "b in (1,2) and a in(select b from test1 where c >=1 and a =0) and c >0",
                // Multiple IN(const).
                "b in (1,2) and a in(2,3) and c >0",
                "b in (1,2) and a in(2,3) and c =0",
                "b in (1,2) and a in(2,3) and c in (1,2,3)",
                // Multiple IN(const) + single IN(query).
                "b in (1,2) and a in(select b from test1 where c >=1 and a =0) and c in(2,3)",
                "b in (1,2) and a in(select b from test1 where c >=1 and a =0) and "
                        + "c in(select b from test1 where c > 0 and b =0)"
        };

        for (String cond : predefinedConditions) {
            executeAndCompare(cond, Collections.emptyList());
        }

        doRandomQueries(200);

        sql("drop table test0");
        sql("drop table test1");
    }

    private void doRandomQueries(int batchSize) {
        Random seedGenerator = new Random();
        String[] columns = { "a", "b", "c" };
        Object[] values = { null, 0, 0, 1, 2, 10, "a", "?" };
        // TODO Issue hangs "in(select", "not in(select".
        String[] compares = { "in(", "not in(", "=", "=", ">", "<", ">=", "<=", "<>"};

        for (int i = 0; i < batchSize; i++) {
            long seed = seedGenerator.nextLong();
            Random random = new Random(seed);
            List<Object> params = new ArrayList<>();
            String condition = getRandomCondition(random, params, columns,
                    compares, values);

            log.info("Seed=" + seed + ", condition=" + condition + ", params=" + params);

            executeAndCompare(condition, params);

            if (!params.isEmpty()) {
                params.replaceAll(ignored -> values[random.nextInt(values.length - 2)]);
                executeAndCompare(condition, params);
            }
        }
    }

    private static void executeAndCompare(String condition, List<Object> params) {
        Object[] parameters = params.toArray(new Object[0]);

        String sql = "select * from test1 where " + condition + " order by 1, 2, 3";
        String sqlIdx = "select * from test0 where " + condition + " order by 1, 2, 3";

        List<List<Object>> expected = sql(sql, parameters);
        List<List<Object>> result = sql(sqlIdx, parameters);

        assertEquals(expected.size(), result.size(), "Expected: " + expected + "\nActual: " + result);

        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), result.get(i), "Row number: " + i);
        }
    }

    private static String getRandomCondition(Random random, List<Object> params,
            String[] columns, String[] compares, Object[] values) {
        int comp = 1 + random.nextInt(4);
        StringBuilder buff = new StringBuilder();
        for (int j = 0; j < comp; j++) {
            if (j > 0) {
                buff.append(random.nextBoolean() ? " and " : " or ");
            }
            String column = columns[random.nextInt(columns.length)];
            String compare = compares[random.nextInt(compares.length)];
            buff.append(column).append(' ').append(compare);
            if (compare.endsWith("in(")) {
                int len = 1 + random.nextInt(3);
                for (int k = 0; k < len; k++) {
                    if (k > 0) {
                        buff.append(", ");
                    }
                    Object value = values[random.nextInt(values.length)];
                    buff.append(value);
                    if ("?".equals(value)) {
                        value = values[random.nextInt(values.length - 2)];
                        params.add(value);
                    }
                }
                buff.append(")");
            } else if (compare.endsWith("(select")) {
                String col = columns[random.nextInt(columns.length)];
                buff.append(" ").append(col).append(" from test1 where ");
                String condition = getRandomCondition(
                        random, params, columns, compares, values);
                buff.append(condition);
                buff.append(")");
            } else {
                Object value = values[random.nextInt(values.length)];
                buff.append(value);
                if ("?".equals(value)) {
                    value = values[random.nextInt(values.length - 2)];
                    params.add(value);
                }
            }
        }
        return buff.toString();
    }
}
