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
import static org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES;

import com.google.common.collect.ImmutableSet;
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

            for (String toType : toTypes) {
                if (toType.equals("ALL")) {
                    allCastsPossible = true;

                    for (String type : allTypes) {
                        testItems.add(checkStatement().sql(String.format("SELECT CAST('1'::%s AS %s)", from, type)).ok());
                    }

                    break;
                }

                // TODO: https://issues.apache.org/jira/browse/IGNITE-19274
                if (toType.contains("LOCAL_TIME")) {
                    continue;
                }

                testItems.add(checkStatement().sql(String.format("SELECT CAST('1'::%s AS %s)", from, toType)).ok());
            }

            testItems.add(checkStatement().sql(String.format("SELECT '1'::%s", from)).ok());

            if (allCastsPossible) {
                continue;
            }

            testItems.add(checkStatement().sql(String.format("SELECT CAST('1'::%s AS %s)", from, from)).ok());

            Set<String> deprecatedCastTypes = allTypes.stream().filter(t -> !toTypes.contains(t) && !t.equals(from))
                    .collect(Collectors.toSet());

            for (String toType : deprecatedCastTypes) {
                testItems.add(checkStatement().sql(String.format("SELECT CAST('1'::%s AS %s)", from, toType)).fails(castErrorMessage));
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

        UUID(UuidType.NAME, ImmutableSet.copyOf(charNames)),

        VARCHAR(SqlTypeName.VARCHAR.getName(), Set.of("ALL")),

        CHAR(SqlTypeName.CHAR.getName(), Set.of("ALL")),

        VARBINARY(SqlTypeName.VARBINARY.getName(), charAndBinaryNames),

        DATE(SqlTypeName.DATE.getName(), charAndTs),

        TIME(SqlTypeName.TIME.getName(), charAndTs),

        TIMESTAMP(SqlTypeName.TIMESTAMP.getName(), charAndDt);

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
