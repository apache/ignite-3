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

import static org.apache.calcite.sql.type.SqlTypeName.BINARY_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR;
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Test CAST type to type possibilities. */
public class CastResolutionTest extends AbstractPlannerTest {
    private static final String castErrorMessage = "Cast function cannot convert value of type";

    private static final Set<String> numericNames = NUMERIC_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> charNames = CHAR_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> binaryNames = BINARY_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> dtNames = DATETIME_TYPES.stream().map(SqlTypeName::getName).collect(Collectors.toSet());

    private static final Set<String> charAndNumericNames = new HashSet<>();

    private static final Set<String> charAndBinaryNames = new HashSet<>();

    private static final Set<String> charAndTs = new HashSet<>();

    private static final Set<String> charAndDt = new HashSet<>();

    private static final Set<String> charAndYInterval = new HashSet<>();

    private static final Set<String> charAndDInterval = new HashSet<>();

    private static final String commonTemplate = "SELECT CAST('1'::%s AS %s)";

    private static final String intervalTemplate = "SELECT CAST(INTERVAL 1 %s AS %s)";

    static {
        numericNames.add("NUMERIC");

        charAndNumericNames.addAll(numericNames);
        charAndNumericNames.addAll(charNames);

        charAndBinaryNames.addAll(charNames);
        charAndBinaryNames.addAll(binaryNames);

        charAndTs.addAll(charNames);
        charAndTs.add(SqlTypeName.TIMESTAMP.getName());

        charAndDt.addAll(dtNames);
        charAndDt.addAll(charNames);

        charAndYInterval.addAll(List.of(INTERVAL_YEAR.getName(), INTERVAL_MONTH.getName()));
        charAndYInterval.addAll(charNames);

        charAndDInterval.addAll(List.of(INTERVAL_HOUR.getName(), INTERVAL_MINUTE.getName()));
        charAndDInterval.addAll(charNames);
    }

    /** Test CAST possibility for different supported types. */
    @TestFactory
    public Stream<DynamicTest> allowedCasts() {
        List<DynamicTest> testItems = new ArrayList<>();

        Set<String> allTypes = Arrays.stream(CastMatrix.values()).map(v -> v.from).collect(Collectors.toSet());

        for (CastMatrix types : CastMatrix.values()) {
            String from = types.from;
            Set<String> toTypes = types.toTypes;
            boolean allCastsPossible = false;

            boolean interval = isInterval(from);
            String template = interval ? intervalTemplate : commonTemplate;
            from = interval ? from.substring("interval_".length()) : from;

            for (String toType : toTypes) {
                toType = isInterval(toType) ? makeUsableIntervalType(toType) : toType;

                if (toType.equals("ALL")) {
                    allCastsPossible = true;

                    for (String type : allTypes) {
                        type = isInterval(type) ? makeUsableIntervalType(type) : type;

                        testItems.add(checkStatement().sql(String.format(template, from, type)).ok());
                    }

                    break;
                }

                // TODO: https://issues.apache.org/jira/browse/IGNITE-19274
                if (toType.contains("LOCAL_TIME")) {
                    continue;
                }

                testItems.add(checkStatement().sql(String.format(template, from, toType)).ok(false));
            }

            if (!interval) {
                testItems.add(checkStatement().sql(String.format("SELECT '1'::%s", from)).ok(false));
            }

            // all types are allowed.
            if (allCastsPossible) {
                continue;
            }

            if (!interval) {
                testItems.add(checkStatement().sql(String.format(template, from, from)).ok());
            }

            String finalFrom = from;
            Set<String> deprecatedCastTypes = allTypes.stream().filter(t -> !toTypes.contains(t) && !t.equals(finalFrom))
                    .collect(Collectors.toSet());

            for (String toType : deprecatedCastTypes) {
                toType = isInterval(toType) ? makeUsableIntervalType(toType) : toType;

                testItems.add(checkStatement().sql(String.format(template, from, toType)).fails(castErrorMessage));
            }
        }

        return testItems.stream();
    }

    private static boolean isInterval(String typeName) {
        return typeName.toLowerCase().contains("interval");
    }

    private static String makeUsableIntervalType(String typeName) {
        return typeName.replace("_", " ");
    }

    private enum CastMatrix {
        BOOLEAN(SqlTypeName.BOOLEAN.getName(), charNames),

        INT8(SqlTypeName.TINYINT.getName(), charAndNumericNames),

        INT16(SqlTypeName.SMALLINT.getName(), charAndNumericNames),

        INT32(SqlTypeName.INTEGER.getName(), charAndNumericNames),

        INT64(SqlTypeName.BIGINT.getName(), charAndNumericNames),

        DECIMAL(SqlTypeName.DECIMAL.getName(), charAndNumericNames),

        REAL(SqlTypeName.REAL.getName(), charAndNumericNames),

        DOUBLE(SqlTypeName.DOUBLE.getName(), charAndNumericNames),

        FLOAT(SqlTypeName.FLOAT.getName(), charAndNumericNames),

        NUMERIC("NUMERIC", charAndNumericNames),

        UUID(UuidType.NAME, new HashSet<>(charNames)),

        VARCHAR(SqlTypeName.VARCHAR.getName(), Set.of("ALL")),

        CHAR(SqlTypeName.CHAR.getName(), Set.of("ALL")),

        VARBINARY(SqlTypeName.VARBINARY.getName(), charAndBinaryNames),

        DATE(SqlTypeName.DATE.getName(), charAndTs),

        TIME(SqlTypeName.TIME.getName(), charAndTs),

        TIMESTAMP(SqlTypeName.TIMESTAMP.getName(), charAndDt),

        INTERVAL_YEAR(SqlTypeName.INTERVAL_YEAR.getName(), charAndYInterval),

        INTERVAL_HOUR(SqlTypeName.INTERVAL_HOUR.getName(), charAndDInterval);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19274
        //TIMESTAMP_TS(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getName(), charAndDt);

        private String from;
        private Set<String> toTypes;

        CastMatrix(String from, Set<String> toTypes) {
            this.from = from;
            this.toTypes = toTypes;
        }
    }
}
