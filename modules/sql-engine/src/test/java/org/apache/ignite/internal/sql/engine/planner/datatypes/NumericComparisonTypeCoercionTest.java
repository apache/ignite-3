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

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *  A set of test to verify behavior of type coercion for binary comparison, when operands belongs
 *  to the NUMERIC type family.
 *
 *  <p>This tests aim to help to understand in which cases implicit cast will be added to which operand.
 */
public class NumericComparisonTypeCoercionTest extends AbstractPlannerTest {
    private static Stream<Arguments> args() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT8))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT8)),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpMatches(castTo(NativeTypes.INT16))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT16)),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.TINYINT_NUMBER_1)
                        .firstOpMatches(castTo(NativeTypes.numberOf(3)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(3))),

                forTypePair(NumericPair.TINYINT_NUMBER_2)
                        .firstOpMatches(castTo(NativeTypes.numberOf(3)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(3))),

                forTypePair(NumericPair.TINYINT_NUMBER_5)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_5)),

                forTypePair(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(3, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(3, 0))),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(4, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(4, 1))),

                forTypePair(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(6, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(6, 3))),

                forTypePair(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(3, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(3, 0))),

                forTypePair(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(4, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(4, 1))),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(6, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(6, 3))),

                forTypePair(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.TINYINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.TINYINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT16))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT16)),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpMatches(castTo(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.SMALLINT_NUMBER_1)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(castTo(Types.NUMBER_5)),

                forTypePair(NumericPair.SMALLINT_NUMBER_2)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(castTo(Types.NUMBER_5)),

                forTypePair(NumericPair.SMALLINT_NUMBER_5)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_5)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.SMALLINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.SMALLINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.INT_INT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT32))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT32)),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpMatches(castTo(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.INT_NUMBER_1)
                        .firstOpMatches(castTo(NativeTypes.numberOf(10)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(10))),

                forTypePair(NumericPair.INT_NUMBER_2)
                        .firstOpMatches(castTo(NativeTypes.numberOf(10)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(10))),

                forTypePair(NumericPair.INT_NUMBER_5)
                        .firstOpMatches(castTo(NativeTypes.numberOf(10)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(10))),

                forTypePair(NumericPair.INT_DECIMAL_1_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(10, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(10, 0))),

                forTypePair(NumericPair.INT_DECIMAL_2_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(11, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(11, 1))),

                forTypePair(NumericPair.INT_DECIMAL_4_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(13, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(13, 3))),

                forTypePair(NumericPair.INT_DECIMAL_2_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(10, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(10, 0))),

                forTypePair(NumericPair.INT_DECIMAL_3_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(11, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(11, 1))),

                forTypePair(NumericPair.INT_DECIMAL_5_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(13, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(13, 3))),

                forTypePair(NumericPair.INT_DECIMAL_5_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(10, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(10, 0))),

                forTypePair(NumericPair.INT_DECIMAL_6_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(11, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(11, 1))),

                forTypePair(NumericPair.INT_DECIMAL_8_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(13, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(13, 3))),

                forTypePair(NumericPair.INT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.INT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.INT64))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.INT64)),

                forTypePair(NumericPair.BIGINT_NUMBER_1)
                        .firstOpMatches(castTo(NativeTypes.numberOf(19)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(19))),

                forTypePair(NumericPair.BIGINT_NUMBER_2)
                        .firstOpMatches(castTo(NativeTypes.numberOf(19)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(19))),

                forTypePair(NumericPair.BIGINT_NUMBER_5)
                        .firstOpMatches(castTo(NativeTypes.numberOf(19)))
                        .secondOpMatches(castTo(NativeTypes.numberOf(19))),

                forTypePair(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(19, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(19, 0))),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(20, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(20, 1))),

                forTypePair(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(22, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(22, 3))),

                forTypePair(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(19, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(19, 0))),

                forTypePair(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(20, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(20, 1))),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(22, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(22, 3))),

                forTypePair(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(19, 0)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(19, 0))),

                forTypePair(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(20, 1)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(20, 1))),

                forTypePair(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpMatches(castTo(NativeTypes.decimalOf(22, 3)))
                        .secondOpMatches(castTo(NativeTypes.decimalOf(22, 3))),

                forTypePair(NumericPair.BIGINT_REAL)
                        .firstOpMatches(castTo(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.BIGINT_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.NUMBER_1_NUMBER_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_1)),

                forTypePair(NumericPair.NUMBER_1_NUMBER_2)
                        .firstOpMatches(castTo(Types.NUMBER_2))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_2)),

                forTypePair(NumericPair.NUMBER_1_NUMBER_5)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_5)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_2_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_2_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.NUMBER_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.NUMBER_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.NUMBER_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.NUMBER_2_NUMBER_2)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_2))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_2)),

                forTypePair(NumericPair.NUMBER_2_NUMBER_5)
                        .firstOpMatches(castTo(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_5)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_2))
                        .secondOpMatches(castTo(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_2))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.NUMBER_2_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.NUMBER_2_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.NUMBER_2_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.NUMBER_5_NUMBER_5)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.NUMBER_5)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_5))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_5))
                        .secondOpMatches(castTo(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.NUMBER_5))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.NUMBER_5_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.NUMBER_5_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.NUMBER_5_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_1_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_1_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpMatches(castTo(Types.DECIMAL_2_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_2_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpMatches(castTo(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(castTo(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_4_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(castTo(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_2_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpMatches(castTo(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_3_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpMatches(castTo(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(castTo(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(castTo(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_5_0)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpMatches(castTo(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_6_1)),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpMatches(castTo(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3))
                        .secondOpMatches(ofTypeWithoutCast(Types.DECIMAL_8_3)),

                forTypePair(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(castTo(NativeTypes.DOUBLE)),

                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.REAL_REAL)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.FLOAT)),

                forTypePair(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE)),


                forTypePair(NumericPair.DOUBLE_DOUBLE)
                        .firstOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
                        .secondOpMatches(ofTypeWithoutCast(NativeTypes.DOUBLE))
        );
    }

    @ParameterizedTest
    @MethodSource("args")
    public void equalsTo(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 = c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 = c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void lessThan(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 < c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 > c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void lessThanOrEqual(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 <= c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 >= c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void greaterThan(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 > c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 < c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("args")
    public void greaterThanOrEqual(
            TypePair typePair,
            Matcher<RexNode> firstOperandMatcher,
            Matcher<RexNode> secondOperandMatcher
    ) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(typePair.first(), typePair.second());

        assertPlan("SELECT c1 <= c2 FROM t", schema, operandMatcher(firstOperandMatcher, secondOperandMatcher)::matches, List.of());
        assertPlan("SELECT c2 >= c1 FROM t", schema, operandMatcher(secondOperandMatcher, firstOperandMatcher)::matches, List.of());
    }

    /**
     * This test ensures that {@link #args()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        EnumSet<NumericPair> remainingPairs = EnumSet.allOf(NumericPair.class);

        args().map(Arguments::get).map(arg -> (NumericPair) arg[0]).forEach(remainingPairs::remove);

        assertThat(remainingPairs, Matchers.empty());
    }

    private static IgniteSchema createSchemaWithTwoColumnTable(NativeType c1, NativeType c2) {
        return createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", c1)
                        .addColumn("C2", c2)
                        .build()
        );
    }

    private static Matcher<IgniteRel> operandMatcher(Matcher<RexNode> first, Matcher<RexNode> second) {
        return new BaseMatcher<IgniteRel>() {
            @Override
            public boolean matches(Object actual) {
                RexNode comparison = ((ProjectableFilterableTableScan) actual).projects().get(0);

                assertThat(comparison, instanceOf(RexCall.class));

                RexCall comparisonCall = (RexCall) comparison;

                RexNode leftOperand = comparisonCall.getOperands().get(0);
                RexNode rightOperand = comparisonCall.getOperands().get(1);

                assertThat(leftOperand, first);
                assertThat(rightOperand, second);

                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }

    /**
     * Creates a matcher to verify that given expression has expected return type, but it is not CAST operator.
     *
     * @param type Expected return type.
     * @return A matcher.
     */
    private static Matcher<RexNode> ofTypeWithoutCast(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = TypeUtils.native2relationalType(typeFactory, type);

        return new BaseMatcher<RexNode>() {
            @Override
            public boolean matches(Object actual) {
                return SqlTypeUtil.equalSansNullability(typeFactory, ((RexNode) actual).getType(), sqlType)
                        && !((RexNode) actual).isA(SqlKind.CAST);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Operand of type {} that is not CAST", sqlType));
            }
        };
    }

    /**
     * Creates a matcher to verify that given expression is CAST operator with expected return type.
     *
     * @param type Expected return type.
     * @return A matcher.
     */
    private static Matcher<RexNode> castTo(NativeType type) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();
        RelDataType sqlType = TypeUtils.native2relationalType(typeFactory, type);

        return new BaseMatcher<RexNode>() {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof RexCall
                        && ((RexNode) actual).isA(SqlKind.CAST)
                        && SqlTypeUtil.equalSansNullability(typeFactory, ((RexNode) actual).getType(), sqlType);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Operand that is CAST(..):" + sqlType);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendValue(item).appendText(" of type " + ((RexNode) item).getType());
            }
        };
    }

    private static TestCaseBuilder forTypePair(TypePair typePair) {
        return new TestCaseBuilder(typePair);
    }

    /**
     * Not really a builder, but provides DSL-like API to describe test case.
     */
    static class TestCaseBuilder {
        private final TypePair pair;
        private Matcher<RexNode> firstOpMatcher;

        private TestCaseBuilder(TypePair pair) {
            this.pair = pair;
        }

        TestCaseBuilder firstOpMatches(Matcher<RexNode> operandMatcher) {
            firstOpMatcher = operandMatcher;

            return this;
        }

        Arguments secondOpMatches(Matcher<RexNode> operandMatcher) {
            return Arguments.of(pair, firstOpMatcher, operandMatcher);
        }
    }
}
