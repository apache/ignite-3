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

package org.apache.ignite.internal.sql.engine.planner.datatypes;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for aggregates, when operand belongs to the NUMERIC type family.
 *
 * <p>Note: technically tests cases from this class doesn't involve {@link TypeCoercion}, yet this approach is convenient
 * to highlight how exactly types are deducted for particular functions.
 */
public class NumericAggregateTypeCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("avgArgs")
    public void avg(NativeType argument, NativeType expected) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(argument);

        assertPlan("SELECT avg(c1) FROM t", schema, aggregateReturnTypeMatcher(expected)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("sumArgs")
    public void sum(NativeType argument, NativeType expected) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(argument);

        assertPlan("SELECT sum(c1) FROM t", schema, aggregateReturnTypeMatcher(expected)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("sameTypeArgs")
    public void min(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT min(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("sameTypeArgs")
    public void max(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT max(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("sameTypeArgs")
    public void any(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT any_value(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    private static Stream<Arguments> avgArgs() {
        return Stream.of(
                forArgumentOfType(NativeTypes.INT8).resultWillBe(Types.DECIMAL_19_16),
                forArgumentOfType(NativeTypes.INT16).resultWillBe(Types.DECIMAL_21_16),
                forArgumentOfType(NativeTypes.INT32).resultWillBe(Types.DECIMAL_26_16),
                forArgumentOfType(NativeTypes.INT64).resultWillBe(Types.DECIMAL_35_16),
                forArgumentOfType(Types.DECIMAL_4_0).resultWillBe(Types.DECIMAL_20_16),
                forArgumentOfType(Types.DECIMAL_4_2).resultWillBe(Types.DECIMAL_18_16),
                forArgumentOfType(Types.DECIMAL_20_18).resultWillBe(Types.DECIMAL_20_18),
                forArgumentOfType(NativeTypes.FLOAT).resultWillBe(NativeTypes.DOUBLE),
                forArgumentOfType(NativeTypes.DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> sumArgs() {
        return Stream.of(
                forArgumentOfType(NativeTypes.INT8).resultWillBe(NativeTypes.INT64),
                forArgumentOfType(NativeTypes.INT16).resultWillBe(NativeTypes.INT64),
                forArgumentOfType(NativeTypes.INT32).resultWillBe(NativeTypes.INT64),
                forArgumentOfType(NativeTypes.INT64).resultWillBe(Types.DECIMAL_38_0),
                forArgumentOfType(Types.DECIMAL_4_0).resultWillBe(Types.DECIMAL_8_0),
                forArgumentOfType(Types.DECIMAL_4_2).resultWillBe(Types.DECIMAL_8_2),
                forArgumentOfType(Types.DECIMAL_20_18).resultWillBe(Types.DECIMAL_40_18),
                forArgumentOfType(NativeTypes.FLOAT).resultWillBe(NativeTypes.DOUBLE),
                forArgumentOfType(NativeTypes.DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> sameTypeArgs() {
        return Stream.of(
                Arguments.of(NativeTypes.INT8),
                Arguments.of(NativeTypes.INT16),
                Arguments.of(NativeTypes.INT32),
                Arguments.of(NativeTypes.INT64),
                Arguments.of(Types.DECIMAL_4_0),
                Arguments.of(Types.DECIMAL_4_2),
                Arguments.of(Types.DECIMAL_20_18),
                Arguments.of(NativeTypes.FLOAT),
                Arguments.of(NativeTypes.DOUBLE)
        );
    }
}
