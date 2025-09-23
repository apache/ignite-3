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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.DatetimePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for CASE operator, when operands belong to DATETIME types.
 *
 * <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class DateTimeCaseTypeCoercionTest extends BaseTypeCoercionTest {

    private static final IgniteSchema SCHEMA = createSchemaWithTwoColumnTable(NativeTypes.STRING, NativeTypes.STRING);

    /** CASE operands from columns. */
    @ParameterizedTest
    @MethodSource("caseArgs")
    public void caseColumnsTypeCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN c1 ELSE c2 END FROM t", schema,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> caseArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIME 0

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .firstOpMatches(castTo(Types.TIME_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .firstOpMatches(castTo(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 3

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .firstOpMatches(castTo(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 6

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .firstOpMatches(castTo(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 9

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_0))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame()
        );
    }

    /** CASE operands from dynamic params. */
    @ParameterizedTest
    @MethodSource("dynamicLiteralArgs")
    public void caseWithDynamicParamsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                SqlTestUtils.generateValueByType(typePair.first()),
                SqlTestUtils.generateValueByType(typePair.second())
        );

        assertPlan("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN ? ELSE ? END FROM t", SCHEMA,
                operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, params);
    }

    private static Stream<Arguments> dynamicLiteralArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIME 0

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),

                // TIME 3

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),

                // TIME 6

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM)),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_DYN_PARAM))
                        .secondOpBeSame(),

                // TIME 9

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM)),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_DYN_PARAM)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_DYN_PARAM))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_DYN_PARAM))
        );
    }

    /** CASE operands from literals. */
    @ParameterizedTest
    @MethodSource("literalArgs")
    public void caseWithLiteralsCoercion(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        List<Object> params = List.of(
                generateLiteralWithNoRepetition(typePair.first()), generateLiteralWithNoRepetition(typePair.second())
        );

        assertPlan(format("SELECT CASE WHEN RAND_UUID() != RAND_UUID() THEN {} ELSE {} END FROM t", params.get(0), params.get(1)),
                SCHEMA, operandCaseMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
    }

    private static Stream<Arguments> literalArgs() {
        return Stream.of(

                forTypePair(DatetimePair.DATE_DATE)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                // TIME 0

                forTypePair(DatetimePair.TIME_0_TIME_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_0_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 3

                forTypePair(DatetimePair.TIME_3_TIME_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_3_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 6

                forTypePair(DatetimePair.TIME_6_TIME_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIME_6_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpBeSame(),

                // TIME 9

                forTypePair(DatetimePair.TIME_9_TIME_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIME_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIME_9)),

                // TIMESTAMP 0

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_0)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_3))
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 3

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_3)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_3)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_6))
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 6

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9)),

                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_6)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_6)),
                forTypePair(DatetimePair.TIMESTAMP_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_9))
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP 9

                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_9)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_9_TIMESTAMP_WLTZ_9)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.TIMESTAMP_9)),

                // TIMESTAMP LTZ 0

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_0)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_0)
                        .firstOpMatches(castTo(Types.TIMESTAMP_0))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_0_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 3

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_3)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_6)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_3)
                        .firstOpMatches(castTo(Types.TIMESTAMP_3))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_3_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 6

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_6)
                        .firstOpBeSame()
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpBeSame(),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_6)
                        .firstOpMatches(castTo(Types.TIMESTAMP_6))
                        .secondOpBeSame(),
                forTypePair(DatetimePair.TIMESTAMP_WLTZ_6_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame(),

                // TIMESTAMP LTZ 9

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_WLTZ_9)
                        .firstOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9))
                        .secondOpMatches(ofTypeWithoutCast(Types.TIMESTAMP_WLTZ_9)),

                forTypePair(DatetimePair.TIMESTAMP_WLTZ_9_TIMESTAMP_9)
                        .firstOpMatches(castTo(Types.TIMESTAMP_9))
                        .secondOpBeSame()
        );
    }

    /**
     * This test ensures that all test cases don't miss any type pair from {@link DatetimePair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllTypePairs(caseArgs(), DatetimePair.class);
        checkIncludesAllTypePairs(dynamicLiteralArgs(), DatetimePair.class);
        checkIncludesAllTypePairs(literalArgs(), DatetimePair.class);
    }
}
