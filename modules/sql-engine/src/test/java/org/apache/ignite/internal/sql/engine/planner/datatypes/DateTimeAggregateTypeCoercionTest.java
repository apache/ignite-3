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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for aggregates, when operand belongs DATETIME types.
 *
 * <p>Note: technically tests cases from this class doesn't involve {@link TypeCoercion}, yet this approach is convenient
 * to highlight how exactly types are deducted for particular functions.
 */
public class DateTimeAggregateTypeCoercionTest extends BaseTypeCoercionTest {

    @ParameterizedTest
    @MethodSource("args")
    public void min(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT min(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void max(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT max(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void any(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        assertPlan("SELECT any_value(c1) FROM t", schema, aggregateReturnTypeMatcher(type)::matches, List.of());
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                Arguments.of(Types.DATE),
                Arguments.of(Types.TIME_0),
                Arguments.of(Types.TIME_3),
                Arguments.of(Types.TIME_6),
                Arguments.of(Types.TIME_9),
                Arguments.of(Types.TIMESTAMP_0),
                Arguments.of(Types.TIMESTAMP_3),
                Arguments.of(Types.TIMESTAMP_6),
                Arguments.of(Types.TIMESTAMP_9),
                Arguments.of(Types.TIMESTAMP_WLTZ_0),
                Arguments.of(Types.TIMESTAMP_WLTZ_3),
                Arguments.of(Types.TIMESTAMP_WLTZ_6),
                Arguments.of(Types.TIMESTAMP_WLTZ_9)
        );
    }
}
