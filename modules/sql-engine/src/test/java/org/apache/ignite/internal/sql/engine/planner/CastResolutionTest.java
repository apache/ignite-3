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

package org.apache.ignite.internal.sql.engine.planner;

import static java.lang.String.format;
import static org.apache.calcite.sql.type.SqlTypeName.ALL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.EXACT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.FRACTIONAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.IgniteCustomAssignmentsRules;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Test CAST type to type possibilities. */
public class CastResolutionTest extends AbstractPlannerTest {
    private static final String CAST_ERROR_MESSAGE = "Cast function cannot convert value of type";

    private static final Set<String> NUMERIC_NAMES = new HashSet<>();

    private static final Set<String> FRACTIONAL_NAMES = FRACTIONAL_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> EXACT_NUMERIC = EXACT_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> CHAR_NAMES = CHAR_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> BINARY_NAMES = BINARY_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> DT_NAMES = DATETIME_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> YM_INTERVAL = Set.of(INTERVAL_YEAR.getName(), INTERVAL_MONTH.getName());

    private static final Set<String> DAY_INTERVAL = Set.of(INTERVAL_HOUR.getName(), INTERVAL_MINUTE.getName(), INTERVAL_SECOND.getName());

    private static final Set<String> ALL_BESIDES_BINARY_NAMES =
            BOOLEAN_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> CHAR_NUMERIC_AND_INTERVAL_NAMES = new HashSet<>();

    private static final Set<String> CHAR_AND_NUMERIC_NAMES = new HashSet<>();

    private static final Set<String> CHAR_AND_TS = new HashSet<>();

    private static final Set<String> CHAR_AND_DT = new HashSet<>();

    private static final Set<String> CHAR_EXACT_AND_YM_INTERVAL = new HashSet<>();

    private static final Set<String> CHAR_EXACT_AND_DAY_INTERVAL = new HashSet<>();

    private static final String COMMON_TEMPLATE = "SELECT CAST('1'::%s AS %s)";

    private static final String INTERVAL_TEMPLATE = "SELECT CAST(%s AS %s)";

    private static final String BINARY_TEMPLATE = "SELECT CAST(X'01'::%s AS %s)";

    static {
        NUMERIC_NAMES.add("NUMERIC");
        NUMERIC_NAMES.addAll(EXACT_NUMERIC);
        NUMERIC_NAMES.addAll(FRACTIONAL_NAMES);

        ALL_BESIDES_BINARY_NAMES.addAll(NUMERIC_NAMES);
        ALL_BESIDES_BINARY_NAMES.addAll(FRACTIONAL_NAMES);
        ALL_BESIDES_BINARY_NAMES.addAll(CHAR_NAMES);
        ALL_BESIDES_BINARY_NAMES.addAll(DT_NAMES);
        ALL_BESIDES_BINARY_NAMES.addAll(YM_INTERVAL);
        ALL_BESIDES_BINARY_NAMES.addAll(DAY_INTERVAL);
        ALL_BESIDES_BINARY_NAMES.add("NUMERIC");
        ALL_BESIDES_BINARY_NAMES.add(UuidType.NAME);

        CHAR_NUMERIC_AND_INTERVAL_NAMES.addAll(NUMERIC_NAMES);
        CHAR_NUMERIC_AND_INTERVAL_NAMES.addAll(CHAR_NAMES);
        CHAR_NUMERIC_AND_INTERVAL_NAMES.addAll(YM_INTERVAL);
        CHAR_NUMERIC_AND_INTERVAL_NAMES.addAll(DAY_INTERVAL);

        CHAR_AND_NUMERIC_NAMES.addAll(NUMERIC_NAMES);
        CHAR_AND_NUMERIC_NAMES.addAll(CHAR_NAMES);

        CHAR_AND_TS.addAll(CHAR_NAMES);
        CHAR_AND_TS.add(SqlTypeName.TIMESTAMP.getName());
        CHAR_AND_TS.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName());

        CHAR_AND_DT.addAll(DT_NAMES);
        CHAR_AND_DT.addAll(CHAR_NAMES);

        CHAR_EXACT_AND_YM_INTERVAL.addAll(List.of(INTERVAL_YEAR.getName(), INTERVAL_MONTH.getName()));
        CHAR_EXACT_AND_YM_INTERVAL.addAll(CHAR_NAMES);
        CHAR_EXACT_AND_YM_INTERVAL.addAll(EXACT_NUMERIC);
        CHAR_EXACT_AND_YM_INTERVAL.add("NUMERIC");

        CHAR_EXACT_AND_DAY_INTERVAL.addAll(List.of(INTERVAL_HOUR.getName(), INTERVAL_MINUTE.getName()));
        CHAR_EXACT_AND_DAY_INTERVAL.addAll(CHAR_NAMES);
        CHAR_EXACT_AND_DAY_INTERVAL.addAll(EXACT_NUMERIC);
        CHAR_EXACT_AND_DAY_INTERVAL.add("NUMERIC");
    }

    /** Test CAST possibility for different supported types. */
    @TestFactory
    public Stream<DynamicTest> allowedCasts() {
        List<DynamicTest> testItems = new ArrayList<>();

        Set<String> allTypes = Arrays.stream(CastMatrix.values()).map(v -> v.from).collect(Collectors.toSet());

        for (CastMatrix types : CastMatrix.values()) {
            String fromInitial = types.from;
            Set<String> toTypes = types.toTypes;

            String template = template(fromInitial);
            String from = makeUsableIntervalFromType(fromInitial);

            for (String toType : toTypes) {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-21555
                if (toType.equals(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE.getName())
                        || toType.equals(SqlTypeName.TIME_TZ.getName())
                        || toType.equals(SqlTypeName.TIMESTAMP_TZ.getName())) {
                    continue;
                }

                toType = makeSpaceName(toType);

                testItems.add(checkStatement().sql(format(template, from, toType)).ok(false));
            }

            testItems.add(checkStatement().sql(format(template, from, makeSpaceName(fromInitial))).ok());

            testItems.add(checkStatement().sql(format(INTERVAL_TEMPLATE, "NULL", makeSpaceName(fromInitial))).ok());

            String finalFrom = from;
            Set<String> deprecatedCastTypes = allTypes.stream().filter(t -> !toTypes.contains(t) && !t.equals(finalFrom))
                    .collect(Collectors.toSet());

            for (String toType : deprecatedCastTypes) {
                testItems.add(
                        checkStatement().sql(format(template, from, makeSpaceName(toType))).fails(CAST_ERROR_MESSAGE)
                );
            }
        }

        return testItems.stream();
    }

    /* Check that casts between intervals and exact numerics can involve only intervals with a single datetime field. */
    @TestFactory
    public Stream<DynamicTest> testMultiIntervals() {
        List<DynamicTest> testItems = new ArrayList<>();

        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval year to month)").fails(CAST_ERROR_MESSAGE));
        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval day to hour)").fails(CAST_ERROR_MESSAGE));
        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval day to minute)").fails(CAST_ERROR_MESSAGE));
        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval day to second)").fails(CAST_ERROR_MESSAGE));
        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval hour to minute)").fails(CAST_ERROR_MESSAGE));
        testItems.add(checkStatement().sql("SELECT CAST(1 AS interval hour to second)").fails(CAST_ERROR_MESSAGE));

        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval hour to second)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval day to hour)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval day to minute)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval day to second)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval hour to minute)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1' AS interval hour to second)").ok());

        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval hour to second)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval day to hour)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval day to minute)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval day to second)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval hour to minute)").ok());
        testItems.add(checkStatement().sql("SELECT CAST('1'::VARCHAR AS interval hour to second)").ok());

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> allowedCastsFromNull() {
        List<DynamicTest> testItems = new ArrayList<>();

        SqlTypeMappingRule rules = SqlTypeCoercionRule.instance(IgniteCustomAssignmentsRules.instance().getTypeMapping());

        for (SqlTypeName type : ALL_TYPES) {
            if (type == SqlTypeName.NULL) {
                continue;
            }

            testItems.add(DynamicTest.dynamicTest(format("ALLOW: from: %s to: %s", SqlTypeName.NULL.getName(), type),
                    () -> assertTrue(rules.canApplyFrom(type, SqlTypeName.NULL))));
        }

        return testItems.stream();
    }

    @TestFactory
    public Stream<DynamicTest> testRulesContainsIntervalWithExactTypesRules() {
        List<DynamicTest> testItems = new ArrayList<>();

        List<SqlTypeName> singleIntervals = List.of(INTERVAL_YEAR, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE,
                INTERVAL_SECOND);

        List<SqlTypeName> nonSingleIntervals = List.of(INTERVAL_YEAR_MONTH, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND,
                INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE_SECOND);

        List<SqlTypeName> singleYearIntervals = List.of(INTERVAL_YEAR, INTERVAL_MONTH);

        List<SqlTypeName> singleDayIntervals = List.of(INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE,
                INTERVAL_SECOND);

        SqlTypeMappingRule rules = SqlTypeCoercionRule.instance(IgniteCustomAssignmentsRules.instance().getTypeMapping());

        for (SqlTypeName toType : singleIntervals) {
            for (SqlTypeName fromType : EXACT_TYPES) {
                testItems.add(DynamicTest.dynamicTest(format("ALLOW: from: %s to: %s", fromType, toType),
                        () -> assertTrue(rules.canApplyFrom(toType, fromType))));
            }
        }

        for (SqlTypeName toType : nonSingleIntervals) {
            for (SqlTypeName fromType : EXACT_TYPES) {
                testItems.add(DynamicTest.dynamicTest(format("ALLOW: from: %s to: %s", fromType, toType),
                        () -> assertFalse(rules.canApplyFrom(toType, fromType))));
            }
        }

        for (SqlTypeName toType : singleYearIntervals) {
            for (SqlTypeName fromType : singleYearIntervals) {
                testItems.add(DynamicTest.dynamicTest(format("ALLOW: from: %s to: %s", fromType, toType),
                        () -> assertTrue(rules.canApplyFrom(toType, fromType))));
            }
        }

        for (SqlTypeName toType : singleDayIntervals) {
            for (SqlTypeName fromType : singleDayIntervals) {
                testItems.add(DynamicTest.dynamicTest(format("ALLOW: from: %s to: %s", fromType, toType),
                        () -> assertTrue(rules.canApplyFrom(toType, fromType))));
            }
        }

        for (SqlTypeName toType : singleYearIntervals) {
            for (SqlTypeName fromType : singleDayIntervals) {
                testItems.add(DynamicTest.dynamicTest(format("FORBID: from: %s to: %s", fromType, toType),
                        () -> assertFalse(rules.canApplyFrom(toType, fromType))));
            }
        }

        for (SqlTypeName toType : singleDayIntervals) {
            for (SqlTypeName fromType : singleYearIntervals) {
                testItems.add(DynamicTest.dynamicTest(format("FORBID: from: %s to: %s", fromType, toType),
                        () -> assertFalse(rules.canApplyFrom(toType, fromType))));
            }
        }

        return testItems.stream();
    }

    private static String template(String typeName) {
        if (typeName.toLowerCase().contains("interval")) {
            return INTERVAL_TEMPLATE;
        } else if (typeName.toLowerCase().contains("binary")) {
            return BINARY_TEMPLATE;
        } else {
            return COMMON_TEMPLATE;
        }
    }

    private static String makeSpaceName(String typeName) {
        return typeName.replace("_", " ");
    }

    private static String makeUsableIntervalFromType(String typeName) {
        if (typeName.toLowerCase().contains("interval")) {
            return typeName.replace("_", " 1 ");
        }

        return makeSpaceName(typeName);
    }

    private enum CastMatrix {
        BOOLEAN(SqlTypeName.BOOLEAN.getName(), CHAR_NAMES),

        INT8(SqlTypeName.TINYINT.getName(), CHAR_NUMERIC_AND_INTERVAL_NAMES),

        INT16(SqlTypeName.SMALLINT.getName(), CHAR_NUMERIC_AND_INTERVAL_NAMES),

        INT32(SqlTypeName.INTEGER.getName(), CHAR_NUMERIC_AND_INTERVAL_NAMES),

        INT64(SqlTypeName.BIGINT.getName(), CHAR_NUMERIC_AND_INTERVAL_NAMES),

        DECIMAL(SqlTypeName.DECIMAL.getName(), CHAR_NUMERIC_AND_INTERVAL_NAMES),

        REAL(SqlTypeName.REAL.getName(), CHAR_AND_NUMERIC_NAMES),

        DOUBLE(SqlTypeName.DOUBLE.getName(), CHAR_AND_NUMERIC_NAMES),

        FLOAT(SqlTypeName.FLOAT.getName(), CHAR_AND_NUMERIC_NAMES),

        NUMERIC("NUMERIC", CHAR_NUMERIC_AND_INTERVAL_NAMES),

        UUID(UuidType.NAME, new HashSet<>(CHAR_NAMES)),

        VARCHAR(SqlTypeName.VARCHAR.getName(), ALL_BESIDES_BINARY_NAMES),

        CHAR(SqlTypeName.CHAR.getName(), ALL_BESIDES_BINARY_NAMES),

        VARBINARY(SqlTypeName.VARBINARY.getName(), BINARY_NAMES),

        BINARY(SqlTypeName.BINARY.getName(), BINARY_NAMES),

        DATE(SqlTypeName.DATE.getName(), CHAR_AND_TS),

        TIME(SqlTypeName.TIME.getName(), CHAR_AND_TS),

        // TODO: https://issues.apache.org/jira/browse/IGNITE-21555
        // TIME_TZ(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE.getName(), CHAR_AND_DT);

        TIMESTAMP(SqlTypeName.TIMESTAMP.getName(), CHAR_AND_DT),

        TIMESTAMP_TZ(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName(), CHAR_AND_DT),

        INTERVAL_YEAR(SqlTypeName.INTERVAL_YEAR.getName(), CHAR_EXACT_AND_YM_INTERVAL),

        INTERVAL_HOUR(SqlTypeName.INTERVAL_HOUR.getName(), CHAR_EXACT_AND_DAY_INTERVAL);

        private String from;
        private Set<String> toTypes;

        CastMatrix(String from, Set<String> toTypes) {
            this.from = from;
            this.toTypes = toTypes;
        }
    }
}
