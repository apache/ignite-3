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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of test to verify behavior of type coercion for IN operator, when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit casts are added to operands of the IN operator.
 */
public class NumericInTypeCoercionTest extends BaseTypeCoercionTest {

    private static final NativeType DECIMAL_DEFAULT = NativeTypes.decimalOf(32767, 0);

    private static Stream<Arguments> lhsNonDecimal() {
        return Stream.of(
                // TINYINT

                Arguments.of(
                        NumericPair.TINYINT_TINYINT,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),
                Arguments.of(
                        NumericPair.TINYINT_SMALLINT,
                        castTo(NativeTypes.INT16),
                        castTo(NativeTypes.INT16),
                        castTo(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.TINYINT_INT,
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.TINYINT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                // SMALLINT

                Arguments.of(
                        NumericPair.SMALLINT_SMALLINT,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_INT,
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                // INT

                Arguments.of(
                        NumericPair.INT_INT,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.INT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.INT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                // BIGINT

                Arguments.of(
                        NumericPair.BIGINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.BIGINT_BIGINT,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                // REAL

                Arguments.of(
                        NumericPair.REAL_REAL,
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.REAL_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                // DOUBLE

                Arguments.of(
                        NumericPair.DOUBLE_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                )

        );
    }

    private static Stream<Arguments> inOperandsAllColumns() {
        Stream<Arguments> decimals = Stream.of(
                // a IN (b, c) converted to (a = b OR a = c)
                // the first 2 matchers used to check (a = b) and the second for (a = c)

                // DECIMAL (1, 0)

                Arguments.of(
                        NumericPair.DECIMAL_1_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (4, 3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (3, 1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5, 0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5, 3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (6, 1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (8, 3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                )
        );

        return Stream.concat(
                lhsNonDecimal(),
                decimals
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsAllColumns")
    public void columns(NumericPair typePair,
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

    /**
     * This test ensures that {@link #inOperandsDynamicParamLhs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void inOperandColumnsIncludeAllPairs() {
        checkIncludesAllNumericTypePairs(inOperandsAllColumns());
    }

    private static Stream<Arguments> inOperandsDynamicParamsRhs() {

        Stream<Arguments> decimals = Stream.of(
                // DECIMAL (1, 0)

                Arguments.of(
                        NumericPair.DECIMAL_1_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (2, 1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (4, 3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (3, 1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (5, 0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (5, 3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (6, 1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                ),

                // DECIMAL (8, 3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT)
                )
        );

        return Stream.concat(
                lhsNonDecimal(),
                decimals
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsDynamicParamLhs")
    public void dynamicParamsLhs(
            NumericPair typePair,
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
                SqlTestUtils.generateValueByType(typePair.first().spec().asColumnType())
        );

        Predicate<IgniteTableScan> matcher = checkPlan(first, second, third, forth);
        assertPlan("SELECT c1 FROM T1 WHERE ? IN (c2, c3)", schema, matcher, params);
    }

    /**
     * This test ensures that {@link #inOperandsDynamicParamsRhs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void inOperandsDynamicParamLhsIncludeAllPairs() {
        checkIncludesAllNumericTypePairs(inOperandsDynamicParamLhs());
    }

    private static Stream<Arguments> inOperandsDynamicParamLhs() {
        Stream<Arguments> decimals = Stream.of(
                // DECIMAL (1,0)

                Arguments.of(
                        NumericPair.DECIMAL_1_0_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2,1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (3,1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (4,3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5,0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5,3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (6,1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (8,3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        castTo(DECIMAL_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(DECIMAL_DEFAULT),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                )
        );
        return Stream.concat(
                lhsNonDecimal(),
                decimals
        );
    }

    @ParameterizedTest
    @MethodSource("inOperandsDynamicParamsRhs")
    public void dynamicParamsRhs(
            NumericPair typePair,
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
                SqlTestUtils.generateValueByType(typePair.first().spec().asColumnType()),
                SqlTestUtils.generateValueByType(typePair.second().spec().asColumnType())
        );

        Predicate<IgniteTableScan> matcher = checkPlan(first, second, third, forth);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN (?, ?)", schema, matcher, params);
    }

    /**
     * This test ensures that {@link #inOperandsDynamicParamsRhs()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void inOperandsDynamicParamsRhsIncludeAllPairs() {
        checkIncludesAllNumericTypePairs(inOperandsDynamicParamsRhs());
    }

    @ParameterizedTest
    @MethodSource("inOperandsLiterals")
    public void literals(
            NumericPair numericPair,
            Matcher<RexNode> first,
            Matcher<RexNode> second
    ) throws Exception {

        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", numericPair.first())
                        .addColumn("C2", numericPair.second())
                        .build()
        );

        Table table = schema.getTable("T1");
        RelDataType rowType = table.getRowType(Commons.typeFactory());
        RelDataType secondType = rowType.getFieldList().get(1).getType();

        StringBuilder sb = new StringBuilder("(");

        if (SqlTypeUtil.isIntType(secondType)) {
            sb.append('1');
        } else if (SqlTypeUtil.isDecimal(secondType)) {
            sb.append(decimalLiteral((DecimalNativeType) numericPair.second()));
        } else if (SqlTypeUtil.isApproximateNumeric(secondType)) {
            // Approximate numeric types do no have special literals
            sb.append("2.3");
        } else {
            throw new IllegalStateException("Unexpected type: " + secondType);
        }

        sb.append(")");

        // literal 2.3 is converted to 2:INT or 2:BIGINT depending on the left-hand side.
        if ((numericPair.first() == NativeTypes.INT32 || numericPair.first() == NativeTypes.INT64)
                && numericPair.second() == NativeTypes.FLOAT
        ) {
            first = ofTypeWithoutCast(numericPair.first());
            second = ofTypeWithoutCast(numericPair.first());
        }

        Predicate<IgniteTableScan> matcher = checkPlan(first, second);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN " + sb, schema, matcher);
    }

    private static String decimalLiteral(DecimalNativeType nativeType) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < nativeType.precision() - nativeType.scale(); i++) {
            sb.append(i + 1);
        }

        if (nativeType.scale() != 0) {
            sb.append('.');
            for (int i = 0; i < nativeType.scale(); i++) {
                sb.append(i + 1);
            }
        }

        return sb.toString();
    }

    /**
     * This test ensures that {@link #inOperandsLiterals()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void inOperandsLiteralsIncludeAllPairs() {
        checkIncludesAllNumericTypePairs(inOperandsLiterals());
    }

    private static Stream<Arguments> inOperandsLiterals() {
        return Stream.of(
                // TINYINT

                Arguments.of(
                        NumericPair.TINYINT_TINYINT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),
                Arguments.of(
                        NumericPair.TINYINT_SMALLINT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_INT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_REAL,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_BIGINT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_1_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                // SMALLINT

                Arguments.of(
                        NumericPair.SMALLINT_SMALLINT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_INT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_REAL,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_BIGINT,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_1_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_0,
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                // INT

                Arguments.of(
                        NumericPair.INT_INT,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_REAL,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_BIGINT,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                // BIGINT

                Arguments.of(
                        NumericPair.BIGINT_REAL,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_BIGINT,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                // REAL

                Arguments.of(
                        NumericPair.REAL_REAL,
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.REAL_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                // DOUBLE

                Arguments.of(
                        NumericPair.DOUBLE_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                // DECIMAL (1, 0)

                Arguments.of(
                        NumericPair.DECIMAL_1_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        castTo(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2,1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (3,1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (4,3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5,0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5,3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (6,1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (8,3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                )
        );
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
