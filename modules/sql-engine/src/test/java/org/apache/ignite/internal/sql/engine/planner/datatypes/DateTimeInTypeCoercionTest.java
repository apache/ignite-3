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
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for IN operator, when operands belongs to belong DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit casts are added to operands of the IN operator.
 */
public class DateTimeInTypeCoercionTest extends BaseTypeCoercionTest {

    private static final NativeType DYNAMIC_PARAM_TIMESTAMP_TYPE = NativeTypes.datetime(6);
    ;
    private static final NativeType DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE = NativeTypes.timestamp(6);

    private static Stream<Arguments> inOperandsAllColumns() {

        return Stream.of(
                Arguments.of(DatetimePair.DATE_DATE,
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_1,
                        castTo(Types.TIME_1),
                        castTo(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_1,
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_0,
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_0,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_1,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        castTo(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        castTo(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsAllColumns")
    public void columns(DatetimePair typePair,
            Matcher<RexNode> first,
            Matcher<RexNode> second,
            Matcher<RexNode> third,
            Matcher<RexNode> forth
    ) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", typePair.first())
                        .addColumn("C2", typePair.first())
                        .addColumn("C3", typePair.second())
                        .build()
        );

        Predicate<IgniteTableScan> matcher = checkPlan(first, second, third, forth);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN (c2, c3)", schema, matcher);
    }

    private static Stream<Arguments> inOperandsDynamicParamLhs() {

        return Stream.of(

                Arguments.of(DatetimePair.DATE_DATE,
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_1,
                        castTo(Types.TIME_1),
                        castTo(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_1,
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_0,
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1),
                        castTo(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_0,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_1,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9,
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsDynamicParamLhs")
    public void dynamicParamsLhs(
            DatetimePair typePair,
            Matcher<RexNode> first,
            Matcher<RexNode> second,
            Matcher<RexNode> third,
            Matcher<RexNode> forth
    ) throws Exception {

        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", typePair.first())
                        .addColumn("C2", typePair.first())
                        .addColumn("C3", typePair.second())
                        .build()
        );

        List<Object> params = List.of(
                SqlTestUtils.generateValueByType(typePair.first())
        );

        Predicate<IgniteTableScan> matcher = checkPlan(first, second, third, forth);
        assertPlan("SELECT c1 FROM T1 WHERE ? IN (c2, c3)", schema, matcher, params);
    }

    private static Stream<Arguments> inOperandsDynamicParamsRhs() {
        return Stream.of(

                Arguments.of(DatetimePair.DATE_DATE,
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_1,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_1,
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_0,
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_9,
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1),
                        castTo(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_0,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_1,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9,
                        castTo(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_LTZ_TYPE),
                        castTo(DYNAMIC_PARAM_TIMESTAMP_TYPE),
                        ofTypeWithoutCast(DYNAMIC_PARAM_TIMESTAMP_TYPE)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsDynamicParamsRhs")
    public void dynamicParamsRhs(
            DatetimePair typePair,
            Matcher<RexNode> first,
            Matcher<RexNode> second,
            Matcher<RexNode> third,
            Matcher<RexNode> forth
    ) throws Exception {

        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", typePair.first())
                        .addColumn("C2", typePair.first())
                        .addColumn("C3", typePair.second())
                        .build()
        );

        List<Object> params = List.of(
                SqlTestUtils.generateValueByType(typePair.first()),
                SqlTestUtils.generateValueByType(typePair.second())
        );

        Predicate<IgniteTableScan> matcher = checkPlan(first, second, third, forth);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN (?, ?)", schema, matcher, params);
    }


    private static Stream<Arguments> inOperandsLiterals() {
        return Stream.of(

                Arguments.of(DatetimePair.DATE_DATE,
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_1,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_1,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_0,
                        ofTypeWithoutCast(Types.TIME_1),
                        ofTypeWithoutCast(Types.TIME_1)
                ),

                Arguments.of(DatetimePair.TIME_1_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_0,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_9_TIME_1,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        castTo(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_1),
                        castTo(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_1,
                        castTo(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_1),
                        ofTypeWithoutCast(Types.TIMESTAMP_1)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_1_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_1,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_1,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsLiterals")
    public void literals(
            DatetimePair typePair,
            Matcher<RexNode> first,
            Matcher<RexNode> second
    ) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", typePair.first())
                        .addColumn("C2", typePair.second())
                        .build()
        );

        String value = "(" + generateLiteral(typePair.second(), false) + ")";

        Predicate<IgniteTableScan> matcher = checkPlan(first, second);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN " + value, schema, matcher);
    }

    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(inOperandsAllColumns(), DatetimePair.class);
        checkIncludesAllTypePairs(inOperandsDynamicParamLhs(), DatetimePair.class);
        checkIncludesAllTypePairs(inOperandsDynamicParamsRhs(), DatetimePair.class);
        checkIncludesAllTypePairs(inOperandsLiterals(), DatetimePair.class);
    }

    private Predicate<IgniteTableScan> checkPlan(
            Matcher<RexNode> call1op1, Matcher<RexNode> call1op2,
            Matcher<RexNode> call2op1, Matcher<RexNode> call2op2
    ) {
        return isInstanceOf(IgniteTableScan.class).and(t -> {
            RexNode condition = t.condition();

            if (condition.getKind() != SqlKind.OR) {
                return false;
            }

            RexCall or = (RexCall) condition;
            List<RexNode> operands = or.getOperands();
            RexCall call1 = (RexCall) operands.get(0);
            RexCall call2 = (RexCall) operands.get(1);

            boolean firstOp = matchCall(call1, call1op1, call1op2);
            boolean secondOp = matchCall(call2, call2op1, call2op2);

            return firstOp && secondOp;
        });
    }

    private Predicate<IgniteTableScan> checkPlan(Matcher<RexNode> call1op1, Matcher<RexNode> call1op2) {
        return isInstanceOf(IgniteTableScan.class).and(t -> {
            RexNode condition = t.condition();

            return matchCall((RexCall) condition, call1op1, call1op2);
        });
    }

    private static boolean matchCall(RexCall call, Matcher<RexNode> first, Matcher<RexNode> second) {
        List<RexNode> operands = call.getOperands();
        RexNode op1 = operands.get(0);
        RexNode op2 = operands.get(1);

        boolean op1Matches = first.matches(op1);
        boolean op2Matches = second.matches(op2);

        return op1Matches && op2Matches;
    }

}
