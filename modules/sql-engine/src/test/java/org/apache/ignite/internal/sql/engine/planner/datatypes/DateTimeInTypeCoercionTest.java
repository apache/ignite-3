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
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
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

    private static Stream<Arguments> inOperandsAllColumns() {

        return Stream.of(
                Arguments.of(DatetimePair.DATE_DATE,
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE),
                        ofTypeWithoutCast(Types.DATE)
                ),

                // TIME 0

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_3,
                        castTo(Types.TIME_3),
                        castTo(Types.TIME_3),
                        castTo(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_6,
                        castTo(Types.TIME_6),
                        castTo(Types.TIME_6),
                        castTo(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 3

                Arguments.of(DatetimePair.TIME_3_TIME_3,
                        ofTypeWithoutCast(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_6,
                        castTo(Types.TIME_6),
                        castTo(Types.TIME_6),
                        castTo(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 6

                Arguments.of(DatetimePair.TIME_6_TIME_6,
                        ofTypeWithoutCast(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_6_TIME_9,
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 9

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIMESTAMP 0

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
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
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 3

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 6

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 9

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 0

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_WLTZ_3),
                        castTo(Types.TIMESTAMP_WLTZ_3),
                        castTo(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_0),
                        castTo(Types.TIMESTAMP_0),
                        castTo(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 3

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 6

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 9

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
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

                // TIME 0

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_3,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_6,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 3

                Arguments.of(DatetimePair.TIME_3_TIME_3,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_6,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_6_TIME_6,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_6_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 9

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIMESTAMP 0

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 3

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 6

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 9

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                // TIMESTAMP LTZ 0

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 3

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 6

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 9

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
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

                // TIME 0

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_3,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_6,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 3

                Arguments.of(DatetimePair.TIME_3_TIME_3,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_6,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 6

                Arguments.of(DatetimePair.TIME_6_TIME_6,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),
                Arguments.of(DatetimePair.TIME_6_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 9

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIMESTAMP 0

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP 3

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP 6

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP 9

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 0

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP LTZ 3

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP LTZ 6

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        castTo(Types.TIMESTAMP_WLTZ_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        castTo(Types.TIMESTAMP_DYN_PARAM),
                        ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)
                ),

                // TIMESTAMP LTZ 9

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
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

                // TIME 0

                Arguments.of(DatetimePair.TIME_0_TIME_0,
                        ofTypeWithoutCast(Types.TIME_0),
                        ofTypeWithoutCast(Types.TIME_0)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_3,
                        castTo(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_6,
                        castTo(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_0_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                Arguments.of(DatetimePair.TIME_3_TIME_3,
                        ofTypeWithoutCast(Types.TIME_3),
                        ofTypeWithoutCast(Types.TIME_3)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_6,
                        castTo(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_3_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 6

                Arguments.of(DatetimePair.TIME_6_TIME_6,
                        ofTypeWithoutCast(Types.TIME_6),
                        ofTypeWithoutCast(Types.TIME_6)
                ),
                Arguments.of(DatetimePair.TIME_6_TIME_9,
                        castTo(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIME 9

                Arguments.of(DatetimePair.TIME_9_TIME_9,
                        ofTypeWithoutCast(Types.TIME_9),
                        ofTypeWithoutCast(Types.TIME_9)
                ),

                // TIMESTAMP 0

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_0),
                        castTo(Types.TIMESTAMP_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 3

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_3),
                        castTo(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 6

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_6),
                        castTo(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 9

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_9),
                        castTo(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 0

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3,
                        castTo(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0,
                        castTo(Types.TIMESTAMP_0),
                        ofTypeWithoutCast(Types.TIMESTAMP_0)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 3

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6,
                        castTo(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3,
                        castTo(Types.TIMESTAMP_3),
                        ofTypeWithoutCast(Types.TIMESTAMP_3)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP LTZ 6

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9,
                        castTo(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
                ),

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6,
                        castTo(Types.TIMESTAMP_6),
                        ofTypeWithoutCast(Types.TIMESTAMP_6)
                ),
                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9,
                        castTo(Types.TIMESTAMP_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_9)
                ),

                // TIMESTAMP 9

                Arguments.of(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9,
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9),
                        ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)
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

        String value = "(" + generateLiteralWithNoRepetition(typePair.second()) + ")";

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
}
