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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.rel.explain.ExplainUtils;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test to verify that special floating point values are correctly serialized and deserialized in the planner.
 */
public class SpecialFloatingPointValuesSerializationTest extends AbstractPlannerTest {
    @ParameterizedTest
    @MethodSource("specialValuesArgs")
    void testSpecialValues(SqlTypeName typeName, String condition, String expectedCompiledCondition) throws Exception {
        String sql = "SELECT c1 FROM test WHERE " + condition;
        RelDataType sqlType = TYPE_FACTORY.createSqlType(typeName);

        IgniteSchema schema = createSchemaFrom(
                tableBuilder ->
                        tableBuilder.name("TEST")
                                .addColumn("C1", IgniteTypeFactory.relDataTypeToNative(sqlType))
                                .size(400)
                                .distribution(IgniteDistributions.single())
        );

        IgniteRel plan = physicalPlan(sql, List.of(schema), null, List.of(), null);

        checkSplitAndSerialization(plan, List.of(schema));

        log.info("statement: {}\n{}", sql, ExplainUtils.toString(plan));

        var tableScan = (ProjectableFilterableTableScan) plan;
        RexNode rexNode = tableScan.condition();

        assertThat(rexNode, notNullValue());
        assertThat(rexNode.toString(), equalTo(expectedCompiledCondition));
    }

    private static Stream<Arguments> specialValuesArgs() {
        return Stream.of(
                Arguments.of(SqlTypeName.REAL, "c1 = -0.0::REAL", "=($t0, -0.0E0)"),
                Arguments.of(SqlTypeName.REAL, "c1 = '-0.0'::REAL", "=($t0, -0.0E0)"),
                Arguments.of(SqlTypeName.REAL, "c1 = 0.0::REAL", "=($t0, 0.0E0)"),
                Arguments.of(SqlTypeName.REAL, "c1 = '0.0'::REAL", "=($t0, 0.0E0)"),
                Arguments.of(SqlTypeName.REAL, "c1 = 'NaN'::REAL", "=($t0, NaN)"),
                Arguments.of(SqlTypeName.REAL, "c1 = 'Infinity'::REAL", "=($t0, Infinity)"),
                Arguments.of(SqlTypeName.REAL, "c1 = '-Infinity'::REAL", "=($t0, -Infinity)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = -0.0::FLOAT", "=(CAST($t0):FLOAT, -0.0E0)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = '-0.0'::FLOAT", "=(CAST($t0):FLOAT, -0.0E0)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = 0.0::FLOAT", "=(CAST($t0):FLOAT, 0.0E0)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = '0.0'::FLOAT", "=(CAST($t0):FLOAT, 0.0E0)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = 'NaN'::FLOAT", "=(CAST($t0):FLOAT, NaN)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = 'Infinity'::FLOAT", "=(CAST($t0):FLOAT, Infinity)"),
                Arguments.of(SqlTypeName.FLOAT, "c1 = '-Infinity'::FLOAT", "=(CAST($t0):FLOAT, -Infinity)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = -0.0::DOUBLE", "=($t0, -0.0E0)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = '-0.0'::DOUBLE", "=($t0, -0.0E0)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = 0.0::DOUBLE", "=($t0, 0.0E0)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = '0.0'::DOUBLE", "=($t0, 0.0E0)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = 'NaN'::DOUBLE", "=($t0, NaN)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = 'Infinity'::DOUBLE", "=($t0, Infinity)"),
                Arguments.of(SqlTypeName.DOUBLE, "c1 = '-Infinity'::DOUBLE", "=($t0, -Infinity)")
        );
    }
}
