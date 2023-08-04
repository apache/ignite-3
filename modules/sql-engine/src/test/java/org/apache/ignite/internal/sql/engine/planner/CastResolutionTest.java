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
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.StatementChecker;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Test CAST type to type possibilities. */
public class CastResolutionTest extends AbstractPlannerTest {
    private final String castErrorMessage = "Cast function cannot convert value of type";

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

            boolean interval = from.toLowerCase().contains("interval");
            String template = interval ? intervalTemplate : commonTemplate;
            from = interval ? from.substring("interval_".length()) : from;

            for (String toType : toTypes) {
                toType = interval || toType.toLowerCase().contains("interval") ? toType.replace("_", " ") : toType;

                if (toType.equals("ALL")) {
                    allCastsPossible = true;

                    for (String type : allTypes) {
                        type = interval || type.toLowerCase().contains("interval") ? type.replace("_", " ") : type;

                        testItems.add(checkStatement().sql(String.format(template, from, type)).ok());
                    }

                    break;
                }

                // TODO: https://issues.apache.org/jira/browse/IGNITE-19274
                if (toType.contains("LOCAL_TIME")) {
                    continue;
                }

                testItems.add(checkStatement().sql(String.format(template, from, toType)).ok());
            }

            if (!interval) {
                testItems.add(checkStatement().sql(String.format("SELECT '1'::%s", from)).ok());
            }

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
                toType = toType.toLowerCase().contains("interval") ? toType.replace("_", " ") : toType;

                testItems.add(checkStatement().sql(String.format(template, from, toType)).fails(castErrorMessage));
            }
        }

        return testItems.stream();
    }

    @Override public StatementChecker checkStatement() {
        return new PlanChecker();
    }

    /**
     * An implementation of {@link AbstractPlannerTest.PlanChecker} with initialized {@link SqlPrepare} to test plans.
     */
    public class PlanChecker extends StatementChecker {
        PlanChecker() {
            super((schema, sql, params) -> physicalPlan(sql, List.of(schema), HintStrategyTable.EMPTY,
                    params, new NoopRelOptListener()));
        }

        /** {@inheritDoc} */
        @Override
        protected void checkRel(IgniteRel igniteRel, IgniteSchema schema) {
        }
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
