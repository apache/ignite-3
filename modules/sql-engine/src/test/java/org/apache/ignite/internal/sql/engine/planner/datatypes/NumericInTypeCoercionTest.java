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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
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

    private static Stream<Arguments> inOperands() {
        return Stream.of(
                // a IN (b, c) converted to (a = b OR a = c)
                // the first 2 matchers used to check (a = b) and the second for (a = c)
                Arguments.of(
                        NumericPair.TINYINT_TINYINT,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),
                Arguments.of(
                        NumericPair.TINYINT_SMALLINT,
                        castTo(NativeTypes.INT16),
                        castTo(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.TINYINT_INT,
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.TINYINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.TINYINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8),
                        castTo(NativeTypes.INT8)
                ),

                // SMALLINT

                Arguments.of(
                        NumericPair.SMALLINT_SMALLINT,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_INT,
                        castTo(NativeTypes.INT32),
                        castTo(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16),
                        castTo(NativeTypes.INT16)
                ),

                // INT

                Arguments.of(
                        NumericPair.INT_INT,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.INT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.INT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.INT_BIGINT,
                        castTo(NativeTypes.INT64),
                        castTo(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT32),
                        ofTypeWithoutCast(NativeTypes.INT32),
                        castTo(NativeTypes.INT32)
                ),

                // BIGINT

                Arguments.of(
                        NumericPair.BIGINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.BIGINT_REAL,
                        castTo(NativeTypes.FLOAT),
                        castTo(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_BIGINT,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.INT64),
                        ofTypeWithoutCast(NativeTypes.INT64),
                        castTo(NativeTypes.INT64)
                ),

                // REAL

                Arguments.of(
                        NumericPair.REAL_REAL,
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT),
                        ofTypeWithoutCast(NativeTypes.FLOAT)
                ),

                Arguments.of(
                        NumericPair.REAL_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                // DOUBLE

                Arguments.of(
                        NumericPair.DOUBLE_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                // DECIMAL (1, 0)

                Arguments.of(
                        NumericPair.DECIMAL_1_0_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        castTo(NativeTypes.decimalOf(1, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        castTo(NativeTypes.decimalOf(1, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),


                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(1, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        castTo(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        castTo(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (2, 1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        castTo(NativeTypes.decimalOf(2, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        castTo(NativeTypes.decimalOf(2, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (4, 3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        castTo(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        castTo(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(2, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(4, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (3, 1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        castTo(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        castTo(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(3, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (5, 0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        castTo(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        castTo(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (5, 3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        castTo(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        castTo(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 0))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(5, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (6, 1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        castTo(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        castTo(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(6, 1)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                ),

                // DECIMAL (8, 3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        castTo(NativeTypes.decimalOf(8, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        castTo(NativeTypes.decimalOf(8, 3))
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3)),
                        ofTypeWithoutCast(NativeTypes.decimalOf(8, 3))
                )
        );
    }

    @ParameterizedTest
    @MethodSource("inOperands")
    public void test(NumericPair typePair, Matcher<RexNode> first, Matcher<RexNode> second, Matcher<RexNode> third) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", typePair.first())
                        .addColumn("C2", typePair.first())
                        .addColumn("C3", typePair.second())
                        .build()
        );

        assertPlan("SELECT c1 FROM T1 WHERE c1 IN (c2, c3)", schema, isInstanceOf(IgniteTableScan.class).and(t -> {
            RexNode condition = t.condition();

            if (condition.getKind() != SqlKind.OR) {
                return false;
            }

            RexCall or = (RexCall) condition;
            List<RexNode> operands = or.getOperands();
            RexCall call1 = (RexCall) operands.get(0);
            RexCall call2 = (RexCall) operands.get(1);

            boolean firstOp = matchCall(call1, first, first);
            boolean secondOp = matchCall(call2, second, third);

            return firstOp && secondOp;
        }));
    }

    /**
     * This test ensures that {@link #inOperands()} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        checkIncludesAllNumericTypePairs(inOperands());
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
