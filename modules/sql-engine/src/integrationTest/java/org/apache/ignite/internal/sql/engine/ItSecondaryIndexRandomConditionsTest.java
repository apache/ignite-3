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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests queries with random index conditions and checks the results against a table that has no indexes.
 */
public class ItSecondaryIndexRandomConditionsTest extends BaseSqlIntegrationTest {
    private static final long[] seeds = new long[200];

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

        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < seeds.length; i++) {
            seeds[i] = rnd.nextLong();
        }
    }

    @ParameterizedTest(name = "order direction = {0}")
    @ValueSource(strings = {"ASC", "DESC"})
    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    public void testRandomConditions(String direction) {
        sql("create table test0(a int, b int, c int) with primary_zone='IN_MEM_ZONE'");
        sql(format("create index idx_1 on test0(a {})", direction));
        sql(format("create index idx_2 on test0(b, a {})", direction));
        sql("create table test1(a int, b int, c int) with primary_zone='IN_MEM_ZONE'");

        sql("insert into test0 select x / 100, "
                + "mod(x / 10, 10), mod(x, 10) from table(system_range(0, 999))");

        sql("update test0 set a = null where a = 9");
        sql("update test0 set b = null where b = 9");
        sql("update test0 set c = null where c = 9");
        sql("insert into test1 select * from test0");

        doRandomQueries(seeds);
    }

    @WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
    @ParameterizedTest
    @ValueSource(strings = {
            "a,b,c ASC", "a,b,c DESC",
            "a,c,b ASC", "a,c,b DESC",
            "b,a,c ASC", "b,a,c DESC",
            "b,c,a ASC", "b,c,a DESC",
            "c,a,b ASC", "c,a,b DESC",
            "c,b,a ASC", "c,b,a DESC"
    })
    public void testRandomConditionsWithIndexFieldsPermutations(String idxFields) {
        sql("create table test0(a int, b int, c int, d int) with primary_zone='IN_MEM_ZONE'");
        sql("create index idx_2 on test0(" + idxFields + ")");

        sql("create table test1(a int, b int, c int, d int) with primary_zone='IN_MEM_ZONE'");

        sql("insert into test0 select x / 100, "
                + "mod(x / 10, 10), mod(x, 10), x from table(system_range(0, 999))");

        sql("update test0 set a = null where a = 9");
        sql("update test0 set b = null where b = 9");
        sql("update test0 set c = null where c = 9");
        sql("insert into test1 select * from test0");

        doRandomQueries(seeds);

        sql("drop table test0");
        sql("drop table test1");
    }

    private static void doRandomQueries(long ... seeds) {
        String[] columns = { "a", "b", "c" };
        Object[] values = { null, 0, 0, 1, 2, 10, "a", "?" };
        // TODO Issue hangs "in(select", "not in(select".
        String[] compares = { "in(", "not in(", "=", "=", ">", "<", ">=", "<=", "<>"};

        for (long seed : seeds) {
            Random random = new Random(seed);
            List<Object> params = new ArrayList<>();
            String condition = getRandomCondition(random, params, columns,
                    compares, values);

            executeAndCompare(seed, condition, params);

            if (!params.isEmpty()) {
                params.replaceAll(ignored -> values[random.nextInt(values.length - 2)]);

                executeAndCompare(seed, condition, params);
            }
        }
    }

    private static void executeAndCompare(long seed, String condition, List<Object> params) {
        Object[] parameters = params.toArray(new Object[0]);

        String sql = "select * from test1 where " + condition + " order by 1, 2, 3";
        String sqlIdx = "select * from test0 where " + condition + " order by 1, 2, 3";

        String testInfo = "Seed: " + seed + ". Params: " + params + ". Condition: " + condition + "\n";

        List<List<Object>> expected;
        List<List<Object>> result;

        try {
            expected = sql(sql, parameters);
            result = sql(sqlIdx, parameters);
        } catch (Throwable t) {
            throw new RuntimeException(testInfo, t);
        }

        assertEquals(expected.size(), result.size(), testInfo + "Expected: " + expected + "\nActual: " + result);

        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), result.get(i), testInfo + "Row number: " + i);
        }
    }

    private static String getRandomCondition(Random random, List<Object> params,
            String[] columns, String[] compares, Object[] values) {
        int comp = 1 + random.nextInt(4);
        IgniteStringBuilder buff = new IgniteStringBuilder();
        for (int j = 0; j < comp; j++) {
            if (j > 0) {
                buff.app(random.nextBoolean() ? " and " : " or ");
            }
            String column = columns[random.nextInt(columns.length)];
            String compare = compares[random.nextInt(compares.length)];
            buff.app(column).app(' ').app(compare);
            if (compare.endsWith("in(")) {
                int len = 1 + random.nextInt(3);
                for (int k = 0; k < len; k++) {
                    if (k > 0) {
                        buff.app(", ");
                    }
                    Object value = values[random.nextInt(values.length)];
                    buff.app(value);
                    if ("?".equals(value)) {
                        value = values[random.nextInt(values.length - 2)];
                        params.add(value);
                    }
                }
                buff.app(")");
            } else if (compare.endsWith("(select")) {
                String col = columns[random.nextInt(columns.length)];
                buff.app(" ").app(col).app(" from test1 where ");
                String condition = getRandomCondition(
                        random, params, columns, compares, values);
                buff.app(condition);
                buff.app(")");
            } else {
                Object value = values[random.nextInt(values.length)];
                buff.app(value);
                if ("?".equals(value)) {
                    value = values[random.nextInt(values.length - 2)];
                    params.add(value);
                }
            }
        }
        return buff.toString();
    }
}
