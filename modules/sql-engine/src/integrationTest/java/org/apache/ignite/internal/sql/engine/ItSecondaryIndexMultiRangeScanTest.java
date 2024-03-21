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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests index multi-range scans (with SEARCH/SARG operator or with dynamic parameters).
 */
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
public class ItSecondaryIndexMultiRangeScanTest extends BaseSqlIntegrationTest {
    private static final String TEST_DISPLAY_NAME = "dynamicParams={0}, direction={1}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    /** {@inheritDoc} */
    @BeforeAll
    protected void prepare() {
        sql("CREATE TABLE test_asc (c1 INTEGER, c2 VARCHAR, c3 INTEGER)");
        sql("CREATE INDEX c1c2c3_asc ON test_asc(c1 ASC, c2 ASC, c3 ASC)");
        sql("CREATE TABLE test_desc (c1 INTEGER, c2 VARCHAR, c3 INTEGER)");
        sql("CREATE INDEX c1c2c3_desc ON test_desc(c1 DESC, c2 DESC, c3 DESC)");

        for (String tbl : List.of("test_asc", "test_desc")) {
            sql("INSERT INTO " + tbl + "(c1, c2, c3) VALUES (0, null, 0)");

            for (int i = 0; i <= 5; i++) {
                for (int j = 1; j <= 5; j++) {
                    sql("INSERT INTO " + tbl + "(c1, c2, c3) VALUES (?, ?, ?)", i == 0 ? null : i, Integer.toString(j), i * j);
                }
            }
        }
    }

    @ParameterizedTest(name = TEST_DISPLAY_NAME)
    @MethodSource("testParameters")
    public void testIn(boolean useDynamicParameters, String direction) {
        assertCondition(direction, useDynamicParameters,
                "c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 IN (%s, %s)",
                2, 3, "2", "3", 6, 9)
                .returns(2, "3", 6)
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .check();

        assertCondition(direction, useDynamicParameters,
                "(c1 = %s OR c1 IS NULL) AND c2 IN (%s, %s) AND c3 IN (%s, %s)",
                2, "2", "3", 0, 6)
                .returns(null, "2", 0)
                .returns(null, "3", 0)
                .returns(2, "3", 6)
                .check();
    }

    @ParameterizedTest(name = TEST_DISPLAY_NAME)
    @MethodSource("testParameters")
    public void testRange(boolean useDynamicParameters, String direction) {
        assertCondition(direction, useDynamicParameters,
                "((c1 > %s AND c1 < %s) OR (c1 > %s AND c1 < %s)) AND c2 > %s AND c2 < %s",
                1, 3, 3, 5, "2", "5")
                .returns(2, "3", 6)
                .returns(2, "4", 8)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

        assertCondition(direction, useDynamicParameters,
                "c1 IN (%s, %s) AND ((c2 >= %s AND c2 < %s) OR (c2 > %s AND c1 <= %s))",
                1, 2, "2", "3", "4", 5)
                .returns(1, "2", 2)
                .returns(1, "5", 5)
                .returns(2, "2", 4)
                .returns(2, "5", 10)
                .check();

        assertCondition(direction, useDynamicParameters,
                        "c1 IN (%s, %s) AND (c2 < %s OR (c2 >= %s AND c2 < %s) OR (c2 > %s AND c2 < %s) OR c2 > %s)",
                1, 2, "1", "2", "3", "2", "4", "5")
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(2, "2", 4)
                .returns(2, "3", 6)
                .check();

        assertCondition(direction, useDynamicParameters,
                "c1 = %s AND c2 > %s AND c3 IN (%s, %s)",
                4, "3", 16, 20)
                .returns(4, "4", 16)
                .returns(4, "5", 20)
                .check();

        assertCondition(direction, useDynamicParameters,
                "(c1 < %s OR c1 >= %s) AND c2 = c1",
                2, 4)
                .returns(1, "1", 1)
                .returns(4, "4", 16)
                .returns(5, "5", 25)
                .check();
    }

    @ParameterizedTest(name = TEST_DISPLAY_NAME)
    @MethodSource("testParameters")
    public void testNulls(boolean useDynamicParameters, String direction) {
        assertCondition(direction, useDynamicParameters, "c1 IS NULL AND c2 <= %s", "1")
                .returns(null, "1", 0)
                .check();

        assertCondition(direction, useDynamicParameters, "(c1 IS NULL OR c1 = %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "1", 0, 3)
                .returns(null, "1", 0)
                .returns(3, "1", 3)
                .check();

        assertCondition(direction, useDynamicParameters, "(c1 IS NULL OR c1 < %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "1", 0, 1)
                .returns(null, "1", 0)
                .returns(1, "1", 1)
                .check();

        assertCondition(direction, useDynamicParameters, "(c1 IS NULL OR c1 > %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "5", 0, 25)
                .returns(null, "5", 0)
                .returns(5, "5", 25)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IS NOT NULL AND c2 IS NULL")
                .returns(0, null, 0)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IN(NULL, %s) AND c2 = %s", 1, "1")
                .returns(1, "1", 1)
                .check();
    }

    /** Tests not supported range index scan conditions. */
    @ParameterizedTest(name = "dynamicParams={0}")
    @ValueSource(booleans = {true, false})
    public void testNot(boolean useDynamicParameters) {
        assertQuery(useDynamicParameters, "SELECT * FROM test_asc WHERE c1 <> %s AND c3 = %s", 1, 6)
                .matches(QueryChecker.containsTableScan("PUBLIC", "TEST_ASC")) // Can't use index scan.
                .returns(2, "3", 6)
                .returns(3, "2", 6)
                .check();

        assertQuery(useDynamicParameters, "SELECT * FROM test_asc WHERE c1 NOT IN (%s, %s, %s) AND c2 NOT IN (%s, %s, %s)",
                1, 2, 5, 1, 2, 5)
                .matches(QueryChecker.containsTableScan("PUBLIC", "TEST_ASC")) // Can't use index scan.
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();
    }

    /** Test correct index ordering without additional sorting. */
    @ParameterizedTest(name = "dynamicParams={0}")
    @ValueSource(booleans = {true, false})
    public void testOrdering(boolean useDynamicParameters) {
        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_ASC) */ * FROM test_asc WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) ORDER BY c1, c2, c3",
                3, 2, "3", "2")
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(2, "2", 4)
                .returns(2, "3", 6)
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .check();

        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_ASC) */ * FROM test_asc WHERE c1 IN (%s, %s) AND c2 < %s ORDER BY c1, c2, c3", 2, 3, "3")
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(2, "1", 2)
                .returns(2, "2", 4)
                .returns(3, "1", 3)
                .returns(3, "2", 6)
                .check();

        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_ASC) */ * FROM test_asc"
                        + " WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 BETWEEN %s AND %s ORDER BY c1, c2, c3",
                2, 3, "2", "3", 5, 7)
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(2, "3", 6)
                .returns(3, "2", 6)
                .check();

        // Check order for table with DESC ordering.
        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_DESC) */ * FROM test_desc"
                        + " WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) ORDER BY c1 DESC, c2 DESC, c3 DESC",
                3, 2, "3", "2")
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(3, "3", 9)
                .returns(3, "2", 6)
                .returns(2, "3", 6)
                .returns(2, "2", 4)
                .check();

        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_DESC) */ * FROM test_desc"
                        + " WHERE c1 IN (%s, %s) AND c2 < %s ORDER BY c1 DESC, c2 DESC, c3 DESC",
                2, 3, "3")
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(3, "2", 6)
                .returns(3, "1", 3)
                .returns(2, "2", 4)
                .returns(2, "1", 2)
                .check();

        assertQuery(useDynamicParameters,
                "SELECT /*+ FORCE_INDEX(c1c2c3_DESC) */ * FROM test_desc"
                        + " WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 BETWEEN %s AND %s"
                        + " ORDER BY c1 DESC, c2 DESC, c3 DESC",
                2, 3, "2", "3", 5, 7)
                .matches(CoreMatchers.not(QueryChecker.containsSubPlan("Sort"))) // Don't require additional sorting.
                .ordered()
                .returns(3, "2", 6)
                .returns(2, "3", 6)
                .check();
    }

    /** Tests not supported range index scan conditions. */
    @ParameterizedTest(name = TEST_DISPLAY_NAME)
    @MethodSource("testParameters")
    public void testRangeIntersection(boolean useDynamicParameters, String direction) {
        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s, %s, %s) and c3 in (%s, %s, %s, %s)",
                3, 4, 4, 3, 12, 9, 16, 12)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s) "
                        + "AND ((c2 > %s AND c2 < %s) OR (c2 > %s AND c2 < %s))",
                3, 4, "1", "4", "3", "5")
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .returns(4, "2", 8)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

        // Different combinations of LESS_THAN and LESS_THAN_OR_EQUAL.
        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s) AND (c2 < %s OR c2 < %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(4, "1", 4)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s) AND (c2 <= %s OR c2 < %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(4, "1", 4)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s) AND (c2 <= %s OR c2 <= %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(3, "2", 6)
                .returns(4, "1", 4)
                .returns(4, "2", 8)
                .check();

        // Different combinations of LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL.
        assertCondition(direction, useDynamicParameters,
                "c1 IN (%s, %s) AND ((c2 > %s AND c2 <= %s) OR (c2 > %s AND c2 <= %s) OR (c2 >= %s AND c2 < %s))",
                1, 2, "1", "2", "2", "3", "3", "4")
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(2, "2", 4)
                .returns(2, "3", 6)
                .check();

        assertCondition(direction, useDynamicParameters, "c1 IN (%s, %s) AND c2 BETWEEN %s AND %s",
                3, 3, "2", "4")
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .check();
    }

    @ParameterizedTest(name = TEST_DISPLAY_NAME)
    @MethodSource("testParameters")
    public void testInvalidRange(boolean useDynamicParameters, String direction) {
        assertCondition(direction, useDynamicParameters, "c1 BETWEEN %s AND %s", 4, 3)
                .returnNothing()
                .check();

        assertCondition(direction, useDynamicParameters, "c1 = %s AND c2 > %s AND c2 < %s", 1, "2", "2")
                .returnNothing()
                .check();

        assertCondition(direction, useDynamicParameters, "c1 = %s OR c1 BETWEEN %s AND %s", 1, 3, 2)
                .returns(1, "1", 1)
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(1, "4", 4)
                .returns(1, "5", 5)
                .check();
    }

    private static QueryChecker assertCondition(String direction, boolean dynamicParams, String condition, Object... params) {
        String sqlPattern = format("SELECT /*+ FORCE_INDEX(c1c2c3_{}) */ * FROM test_{} WHERE {}", direction, direction, condition);

        return assertQuery(dynamicParams, sqlPattern, params);
    }

    private static QueryChecker assertQuery(boolean dynamicParams, String sqlPattern, Object... params) {
        Object[] args = new Object[params.length];

        if (dynamicParams) {
            Arrays.fill(args, "?");

            return assertQuery(String.format(sqlPattern, args)).withParams(params);
        }

        for (int i = 0; i < params.length; i++) {
            args[i] = params[i] instanceof String ? '\'' + params[i].toString() + '\'' : params[i].toString();
        }

        return assertQuery(String.format(sqlPattern, args));
    }

    private static List<Arguments> testParameters() {
        return List.of(
                Arguments.of(true, "ASC"),
                Arguments.of(true, "DESC"),
                Arguments.of(false, "ASC"),
                Arguments.of(false, "DESC")
        );
    }
}
