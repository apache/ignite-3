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

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for numeric functions, when operand belongs to the NUMERIC type family.
 */
public class NumericFunctionsTypeCoercionTest extends BaseTypeCoercionTest {

    private static final List<NativeType> INT_TYPES = List.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64);

    @ParameterizedTest
    @MethodSource("modArgs")
    public void mod(
            TypePair typePair,
            Matcher<RexNode> arg1,
            Matcher<RexNode> arg2,
            Matcher<RexCall> returnType
    ) throws Exception {

        NativeType type1 = typePair.first();
        NativeType type2 = typePair.second();

        IgniteSchema schema = createSchemaWithTwoColumnTable(type1, type2);

        List<Matcher<RexNode>> args = List.of(arg1, arg2);
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT MOD(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> modArgs() {
        return Stream.of(
                forTypePairEx(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT8),

                forTypePairEx(NumericPair.TINYINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT16),

                forTypePairEx(NumericPair.TINYINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.TINYINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_1),

                forTypePairEx(NumericPair.TINYINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_3),

                forTypePairEx(NumericPair.TINYINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_10_7),

                forTypePairEx(NumericPair.TINYINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_18_15),

                forTypePairEx(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT16),

                forTypePairEx(NumericPair.SMALLINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.SMALLINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.SMALLINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.SMALLINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.SMALLINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT32),

                forTypePairEx(NumericPair.INT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.INT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.INT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.INT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.INT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.INT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.INT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.INT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.INT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.INT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.INT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.INT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_25_15),

                forTypePairEx(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(NativeTypes.INT64),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.BIGINT_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.BIGINT_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.BIGINT_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_30_15),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_1_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_1_0),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),
                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_1_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_1_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_1_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_1),

                forTypePairEx(NumericPair.DECIMAL_2_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_2_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_2_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_4_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_4_3),

                forTypePairEx(NumericPair.DECIMAL_4_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_8_7),

                forTypePairEx(NumericPair.DECIMAL_4_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_16_15),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_2_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_2_0),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_2_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_2_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_2_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_3_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_3_1),

                forTypePairEx(NumericPair.DECIMAL_3_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_3_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_3_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_3),

                forTypePairEx(NumericPair.DECIMAL_5_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_9_7),

                forTypePairEx(NumericPair.DECIMAL_5_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_17_15),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_5_0)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_5_0),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.DECIMAL_5_0_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_5_0_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_5_0_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_6_1)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_6_1),

                forTypePairEx(NumericPair.DECIMAL_6_1_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_6_1_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_6_1_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.DECIMAL_8_3_DECIMAL_8_3)
                        .firstOpBeSame()
                        .secondOpBeSame()
                        .resultWillBe(Types.DECIMAL_8_3),

                forTypePairEx(NumericPair.DECIMAL_8_3_REAL)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_12_7),

                forTypePairEx(NumericPair.DECIMAL_8_3_DOUBLE)
                        .firstOpBeSame()
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_20_15),

                forTypePairEx(NumericPair.REAL_REAL)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo(Types.DECIMAL_14_7))
                        .resultWillBe(Types.DECIMAL_14_7),

                forTypePairEx(NumericPair.REAL_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_14_7))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_22_15),

                forTypePairEx(NumericPair.DOUBLE_DOUBLE)
                        .firstOpMatches(castTo(Types.DECIMAL_30_15))
                        .secondOpMatches(castTo(Types.DECIMAL_30_15))
                        .resultWillBe(Types.DECIMAL_30_15)
        );
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void exp(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        Matcher<RelNode> matcher = new FunctionCallMatcher(List.of(ofTypeWithoutCast(type))).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT EXP(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void power(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT POWER(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void ln(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT LN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void log10(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT LOG10(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void abs(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(type);

        assertPlan("SELECT ABS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void rand(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT RAND(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void randInteger1(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.INT32);

        assertPlan("SELECT RAND_INTEGER(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void randInteger2(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args)
                .returnTypeNullability(false)
                .resultWillBe(NativeTypes.INT32);

        assertPlan("SELECT RAND_INTEGER(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void acos(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ACOS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void asin(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ASIN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void atan(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ATAN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void atan2(TypePair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT ATAN2(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sqrt(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        // SQRT is rewritten to POWER($t0, 0.5:DECIMAL(2, 1))
        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type), ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SQRT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cbrt(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT CBRT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cos(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cosh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COSH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void cot(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT COT(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void degrees(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT DEGREES(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void radians(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT RADIANS(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void round(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT ROUND(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("round2Args")
    public void round2(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(pair.first());

        assertPlan("SELECT ROUND(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<NumericPair> round2Args() {
        return Arrays.stream(NumericPair.values()).filter(p -> INT_TYPES.contains(p.second()));
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void ceil(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT CEIL(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void floor(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT FLOOR(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sign(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(type);

        assertPlan("SELECT SIGN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sin(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SIN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void sinh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT SINH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void tan(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT TAN(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void tanh(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DOUBLE);

        assertPlan("SELECT TANH(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("numeric")
    public void truncate(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        NativeType returnType;
        if (type instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type);
        } else {
            returnType = type;
        }

        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT TRUNCATE(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("truncate2Args")
    public void truncate2(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(pair.first()), ofTypeWithoutCast(pair.second()));
        NativeType type1 = pair.first();

        NativeType returnType;
        if (type1 instanceof DecimalNativeType) {
            returnType = decimalWithZeroScale((DecimalNativeType) type1);
        } else {
            returnType = type1;
        }

        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT TRUNCATE(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    private static NativeType decimalWithZeroScale(DecimalNativeType decimalType) {
        return NativeTypes.decimalOf(decimalType.precision(), 0);
    }

    private static Stream<NumericPair> truncate2Args() {
        return Arrays.stream(NumericPair.values()).filter(p -> INT_TYPES.contains(p.second()));
    }

    // FUNCTIONS WITH NUMERIC ARGUMENTS

    @ParameterizedTest
    @MethodSource("integerInteger")
    public void substring(NumericPair pair, Matcher<RexNode> arg1, Matcher<RexNode> arg2) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", NativeTypes.STRING)
                        .addColumn("C2", pair.first())
                        .addColumn("C3", pair.second())
                        .build()
        );

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1
            );
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);
            assertPlan("SELECT SUBSTRING(C1 FROM C2) FROM T", schema, matcher::matches, List.of());
        }

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1
            );
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);
            assertPlan("SELECT SUBSTRING(C1 FROM C2) FROM T", schema, matcher::matches, List.of());
        }

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1,
                    arg2
            );
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);
            assertPlan("SELECT SUBSTRING(C1 FROM C2 FOR C3) FROM T", schema, matcher::matches, List.of());
        }
    }

    @ParameterizedTest
    @MethodSource("integer")
    public void left(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(NativeTypes.STRING, type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(NativeTypes.STRING), ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);

        assertPlan("SELECT LEFT(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("integer")
    public void right(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(NativeTypes.STRING, type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(NativeTypes.STRING), ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);

        assertPlan("SELECT RIGHT(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("integer")
    public void chr(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);
        RelDataType char1 = Commons.typeFactory().createSqlType(SqlTypeName.CHAR, 1);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(char1);

        assertPlan("SELECT CHR(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("integerInteger")
    public void overlay(NumericPair pair, Matcher<RexNode> arg1, Matcher<RexNode> arg2) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", NativeTypes.stringOf(32))
                        .addColumn("C2", pair.first())
                        .addColumn("C3", pair.second())
                        .build()
        );

        RelDataType varchar = Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR, 64);

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.stringOf(32)),
                    ofTypeWithoutCast(NativeTypes.stringOf(32)),
                    arg1
            );

            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);
            assertPlan("SELECT OVERLAY(C1 PLACING C1 FROM C2) FROM T", schema, matcher::matches, List.of());
        }

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.stringOf(32)),
                    ofTypeWithoutCast(NativeTypes.stringOf(32)),
                    arg1,
                    arg2
            );

            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);
            assertPlan("SELECT OVERLAY(C1 PLACING C1 FROM C2 FOR C3) FROM T", schema, matcher::matches, List.of());
        }
    }

    @ParameterizedTest
    @MethodSource("integer")
    public void position(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(NativeTypes.STRING, type);

        List<Matcher<RexNode>> args = List.of(
                ofTypeWithoutCast(NativeTypes.STRING),
                ofTypeWithoutCast(NativeTypes.STRING),
                ofTypeWithoutCast(type)
        );
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.INT32);

        assertPlan("SELECT POSITION(C1 IN C1 FROM C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("integer")
    public void repeat(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(NativeTypes.STRING, type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(NativeTypes.STRING), ofTypeWithoutCast(type));
        RelDataType varchar = Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR);

        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);

        assertPlan("SELECT REPEAT(C1, C2) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("integerInteger")
    public void substr(NumericPair pair, Matcher<RexNode> arg1, Matcher<RexNode> arg2) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", NativeTypes.STRING)
                        .addColumn("C2", pair.first())
                        .addColumn("C3", pair.second())
                        .build()
        );

        List<Matcher<RexNode>> args = List.of(
                ofTypeWithoutCast(NativeTypes.STRING),
                arg1,
                arg2
        );
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.STRING);

        assertPlan("SELECT SUBSTR(C1, C2, C3) FROM T", schema, matcher::matches, List.of());
    }

    // DATE

    @ParameterizedTest
    @MethodSource("integer")
    public void dateFromUnixDate(NativeType type) throws Exception {
        IgniteSchema schema = createSchemaWithSingleColumnTable(type);

        List<Matcher<RexNode>> args = List.of(ofTypeWithoutCast(type));
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(NativeTypes.DATE);

        assertPlan("SELECT DATE_FROM_UNIX_DATE(C1) FROM T", schema, matcher::matches, List.of());
    }

    @ParameterizedTest
    @MethodSource("dateYearMonthDayArgs")
    public void dateYearMonthDay(NativeType type, NativeType returnType) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", type)
                        .addColumn("C2", type)
                        .addColumn("C3", type)
                        .build()
        );

        List<Matcher<RexNode>> args = List.of(
                ofTypeWithoutCast(type),
                ofTypeWithoutCast(type),
                ofTypeWithoutCast(type)
        );
        Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(returnType);

        assertPlan("SELECT DATE(C1, C2, C3) FROM T", schema, matcher::matches, List.of());
    }

    private static Stream<Arguments> dateYearMonthDayArgs() {
        return Stream.of(
                forArgumentOfType(NativeTypes.INT8)
                        .resultWillBe(NativeTypes.DATE),

                forArgumentOfType(NativeTypes.INT16)
                        .resultWillBe(NativeTypes.DATE),

                forArgumentOfType(NativeTypes.INT32)
                        .resultWillBe(NativeTypes.DATE),

                forArgumentOfType(NativeTypes.INT64)
                        .resultWillBe(NativeTypes.DATE)
        );
    }

    @ParameterizedTest
    @MethodSource("integerInteger")
    public void regexReplace(NumericPair pair, Matcher<RexNode> arg1, Matcher<RexNode> arg2) throws Exception {
        RelDataType varchar = Commons.typeFactory().createSqlType(SqlTypeName.VARCHAR);

        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", NativeTypes.STRING)
                        .addColumn("C2", pair.first())
                        .addColumn("C3", pair.second())
                        .build()
        );

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1
            );
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);
            assertPlan("SELECT REGEXP_REPLACE(C1, C1, C1, C2) FROM T", schema, matcher::matches, List.of());
        }

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1,
                    arg2
            );
            // REGEXP_REPLACE(<character>, <character>, <character>, <integer>, <integer>) ***
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);
            assertPlan("SELECT REGEXP_REPLACE(C1, C1, C1, C2, C3) FROM T", schema, matcher::matches, List.of());
        }

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    ofTypeWithoutCast(NativeTypes.STRING),
                    arg1,
                    arg2,
                    ofTypeWithoutCast(NativeTypes.STRING)
            );
            Matcher<RelNode> matcher = new FunctionCallMatcher(args).resultWillBe(varchar);
            assertPlan("SELECT REGEXP_REPLACE(C1, C1, C1, C2, C3, C1) FROM T", schema, matcher::matches, List.of());
        }
    }

    private static Stream<Arguments> integerInteger() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.TINYINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_SMALLINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.SMALLINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_INT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.INT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame(),

                forTypePair(NumericPair.BIGINT_BIGINT)
                        .firstOpBeSame()
                        .secondOpBeSame()
        );
    }

    @ParameterizedTest
    @EnumSource(NumericPair.class)
    public void systemRange(NumericPair pair) throws Exception {
        IgniteSchema schema = createSchemaWithTwoColumnTable(pair.first(), pair.second());

        {
            List<Matcher<RexNode>> args = List.of(
                    ofTypeWithoutCast(pair.first()),
                    ofTypeWithoutCast(pair.second())
            );

            IgniteTypeFactory tf = Commons.typeFactory();
            RelDataType dataType = new RelDataTypeFactory.Builder(tf)
                    .add("X", tf.createTypeWithNullability(tf.createSqlType(SqlTypeName.BIGINT), false))
                    .build();

            Matcher<RelNode> matcher = new FunctionCallMatcher(args).returnTypeNullability(false).resultWillBe(dataType);
            Predicate<RelNode> tableFunctionMatcher = singleTableFunctionScanAnywhere(matcher);
            assertPlan("SELECT (SELECT x FROM SYSTEM_RANGE(T.C1, T.C2) LIMIT 1) FROM T", schema, tableFunctionMatcher, List.of());
        }
    }

    private static Predicate<RelNode> singleTableFunctionScanAnywhere(Matcher<RelNode> matcher) {
        return (node) -> {
            IgniteRel igniteRel = (IgniteRel) node;
            IgniteTableFunctionScan[] f = new IgniteTableFunctionScan[1];

            IgniteRelShuttle shuttle = new IgniteRelShuttle() {
                @Override
                public IgniteRel visit(IgniteTableFunctionScan rel) {
                    if (f[0] != null) {
                        throw new IllegalStateException("More than one function scan. New:\n" + rel  + "\nCurrent:\n" + f[0]);
                    }

                    f[0] = rel;
                    return super.visit(rel);
                }
            };
            igniteRel.accept(shuttle);

            return f[0] != null && matcher.matches(f[0]);
        };
    }

    private static Stream<Arguments> numeric() {
        return Stream.of(
                Arguments.of(NativeTypes.INT8),
                Arguments.of(NativeTypes.INT16),
                Arguments.of(NativeTypes.INT32),
                Arguments.of(NativeTypes.INT64),
                Arguments.of(Types.DECIMAL_1_0),
                Arguments.of(Types.DECIMAL_2_1),
                Arguments.of(Types.DECIMAL_4_3),
                Arguments.of(Types.DECIMAL_2_0),
                Arguments.of(Types.DECIMAL_3_1),
                Arguments.of(Types.DECIMAL_5_3),
                Arguments.of(Types.DECIMAL_5_0),
                Arguments.of(Types.DECIMAL_6_1),
                Arguments.of(Types.DECIMAL_8_3),
                Arguments.of(NativeTypes.FLOAT),
                Arguments.of(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> integer() {
        return Stream.of(
                Arguments.of(NativeTypes.INT8),
                Arguments.of(NativeTypes.INT16),
                Arguments.of(NativeTypes.INT32),
                Arguments.of(NativeTypes.INT64)
        );
    }
}
