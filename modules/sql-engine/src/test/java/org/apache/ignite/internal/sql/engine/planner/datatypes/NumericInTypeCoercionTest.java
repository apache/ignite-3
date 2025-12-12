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

import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_PRECISION;
import static org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator.DECIMAL_DYNAMIC_PARAM_SCALE;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A set of tests to verify behavior of type coercion for IN operator, when operands belongs to the NUMERIC type family.
 *
 * <p>This tests aim to help to understand in which cases implicit casts are added to operands of the IN operator.
 */
public class NumericInTypeCoercionTest extends BaseTypeCoercionTest {

    private static final NativeType DECIMAL_DYN_PARAM_DEFAULT = NativeTypes.decimalOf(
            DECIMAL_DYNAMIC_PARAM_PRECISION, DECIMAL_DYNAMIC_PARAM_SCALE
    );

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
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0),
                        castTo(Types.DECIMAL_3_0)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_1,
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_3_1,
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1),
                        castTo(Types.DECIMAL_4_1)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_4_3,
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_0,
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_3,
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3),
                        castTo(Types.DECIMAL_6_3)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
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
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_3_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_4_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_0,
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
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
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_0,
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_1,
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_3_1,
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_4_3,
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_0,
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0),
                        castTo(Types.DECIMAL_10_0)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_3,
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_6_1,
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1),
                        castTo(Types.DECIMAL_11_1)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_8_3,
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3),
                        castTo(Types.DECIMAL_13_3)
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
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_0,
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_1,
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_3_1,
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_4_3,
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_0,
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0),
                        castTo(Types.DECIMAL_19_0)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_3,
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_6_1,
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1),
                        castTo(Types.DECIMAL_20_1)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_8_3,
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3),
                        castTo(Types.DECIMAL_22_3)
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

    private static Stream<Arguments> rhsNonDecimal() {
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
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
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
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
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
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.INT_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
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
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.BIGINT_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
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
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0),
                        castTo(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        castTo(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1),
                        castTo(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        castTo(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        castTo(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (2, 1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1)
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
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        castTo(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        castTo(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3),
                        castTo(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (4, 3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3)
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
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (3, 1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        castTo(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5, 0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        castTo(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (5, 3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
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
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (6, 1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        castTo(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                // DECIMAL (8, 3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
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
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (2, 1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (4, 3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (3, 1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (5, 0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (5, 3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (6, 1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (8, 3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT)
                )
        );

        return Stream.concat(
                rhsNonDecimal(),
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
                SqlTestUtils.generateValueByType(typePair.first())
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
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (2,1)

                Arguments.of(
                        NumericPair.DECIMAL_2_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (3,1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (4,3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (5,0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (5,3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (6,1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
                ),

                // DECIMAL (8,3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        castTo(NativeTypes.DOUBLE),
                        ofTypeWithoutCast(NativeTypes.DOUBLE)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT),
                        ofTypeWithoutCast(DECIMAL_DYN_PARAM_DEFAULT),
                        castTo(DECIMAL_DYN_PARAM_DEFAULT)
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
                SqlTestUtils.generateValueByType(typePair.first()),
                SqlTestUtils.generateValueByType(typePair.second())
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
    @MethodSource("inOperandsLiteralsWithinRange")
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

        String value = "(" + generateLiteralForPair(numericPair, true) + ")";

        Predicate<IgniteTableScan> matcher = checkPlan(first, second);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN " + value, schema, matcher);
    }

    private static String generateLiteralForPair(NumericPair numericPair, boolean literalIsInRange) {
        NativeType columnType = numericPair.first();
        NativeType literalType = numericPair.second();

        Map<ColumnType, Integer> precisionPerType = Map.of(
                ColumnType.INT8, 3,
                ColumnType.INT16, 5,
                ColumnType.INT32, 10,
                ColumnType.INT64, 19,
                ColumnType.FLOAT, 10,
                ColumnType.DOUBLE, 19
        );

        Integer columnDigits = precisionPerType.get(columnType.spec());
        int columnFractions = 0;

        if (columnType instanceof DecimalNativeType) {
            DecimalNativeType decimal = (DecimalNativeType) columnType;
            columnDigits = decimal.precision();
            columnFractions = decimal.scale();
            columnDigits -= columnFractions;
        } else if (isFloatingPointType(columnType)) {
            columnFractions = 2;
        }

        Integer literalDigits = precisionPerType.get(literalType.spec());
        int literalFractions = 0;
        if (literalType instanceof DecimalNativeType) {
            DecimalNativeType decimal = (DecimalNativeType) literalType;
            literalDigits = decimal.precision();
            literalFractions = decimal.scale();
            literalDigits -= literalFractions;
        } else if (isFloatingPointType(literalType)) {
            literalFractions = 2;
        }

        int numDigits;
        int numFractions;

        if (literalIsInRange) {
            numDigits = Math.min(columnDigits, literalDigits);
            numFractions = Math.min(columnFractions, literalFractions);
        } else {
            numDigits = Math.max(columnDigits, literalDigits) + 1;
            numFractions = Math.max(columnFractions, literalFractions);
        }

        String intPart = IntStream.rangeClosed(1, numDigits)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(""))
                .substring(0, numDigits);

        if (numFractions > 0) {
            String fracPart = IntStream.rangeClosed(1, numFractions)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(""))
                    .substring(0, numFractions);

            return intPart + "." + fracPart;
        } else {
            return intPart;
        }
    }

    private static boolean isFloatingPointType(NativeType type1) {
        ColumnType secondType = type1.spec();
        return secondType == ColumnType.FLOAT || secondType == ColumnType.DOUBLE;
    }

    @ParameterizedTest
    @MethodSource("inOperandsLiteralsOutOfRange")
    public void literalsOutOfRange(
            NumericPair numericPair
    ) throws Exception {
        IgniteSchema schema = createSchema(
                TestBuilders.table()
                        .name("T1")
                        .distribution(IgniteDistributions.single())
                        .addColumn("C1", numericPair.first())
                        .addColumn("C2", numericPair.second())
                        .build()
        );

        // SHORT values can intersect with a DECIMAL with a 5 digits in integer parts, so for SHORT (INT16) we need to generate values
        // take it into consideration.
        String value = "(" + generateLiteralForPair(numericPair, false) + ")";

        Predicate<IgniteValues> matcher = isInstanceOf(IgniteValues.class);
        assertPlan("SELECT c1 FROM T1 WHERE c1 IN " + value, schema, matcher);
    }

    /**
     * This test ensures that combination of {@link #inOperandsLiteralsWithinRange()} and {@link #inOperandsLiteralsOutOfRange()} doesn't
     * miss any type pair from {@link NumericPair}.
     */
    @Test
    void inOperandsLiteralsIncludeAllPairs() {
        checkIncludesAllNumericTypePairs(Stream.concat(inOperandsLiteralsWithinRange(), inOperandsLiteralsOutOfRange()));
    }

    private static Stream<Arguments> inOperandsLiteralsWithinRange() {
        return Stream.of(
                // TINYINT

                Arguments.of(
                        NumericPair.TINYINT_TINYINT,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
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
                        NumericPair.TINYINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                Arguments.of(
                        NumericPair.TINYINT_DECIMAL_2_0,
                        ofTypeWithoutCast(NativeTypes.INT8),
                        ofTypeWithoutCast(NativeTypes.INT8)
                ),

                // SMALLINT

                Arguments.of(
                        NumericPair.SMALLINT_SMALLINT,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
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
                        NumericPair.SMALLINT_DECIMAL_1_0,
                        ofTypeWithoutCast(NativeTypes.INT16),
                        ofTypeWithoutCast(NativeTypes.INT16)
                ),

                Arguments.of(
                        NumericPair.SMALLINT_DECIMAL_2_0,
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
                        NumericPair.INT_DECIMAL_5_0,
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
                        NumericPair.BIGINT_DECIMAL_5_0,
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
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_1_0_DECIMAL_1_0,
                        ofTypeWithoutCast(Types.DECIMAL_1_0),
                        ofTypeWithoutCast(Types.DECIMAL_1_0)
                ),

                // DECIMAL (2, 0)

                Arguments.of(
                        NumericPair.DECIMAL_2_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_2_0_DECIMAL_2_0,
                        ofTypeWithoutCast(Types.DECIMAL_2_0),
                        ofTypeWithoutCast(Types.DECIMAL_2_0)
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
                        NumericPair.DECIMAL_2_1_DECIMAL_2_1,
                        ofTypeWithoutCast(Types.DECIMAL_2_1),
                        ofTypeWithoutCast(Types.DECIMAL_2_1)
                ),

                // DECIMAL (3,1)

                Arguments.of(
                        NumericPair.DECIMAL_3_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_3_1_DECIMAL_3_1,
                        ofTypeWithoutCast(Types.DECIMAL_3_1),
                        ofTypeWithoutCast(Types.DECIMAL_3_1)
                ),

                // DECIMAL (4,3)

                Arguments.of(
                        NumericPair.DECIMAL_4_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_4_3_DECIMAL_4_3,
                        ofTypeWithoutCast(Types.DECIMAL_4_3),
                        ofTypeWithoutCast(Types.DECIMAL_4_3)
                ),

                // DECIMAL (5,0)

                Arguments.of(
                        NumericPair.DECIMAL_5_0_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_0_DECIMAL_5_0,
                        ofTypeWithoutCast(Types.DECIMAL_5_0),
                        ofTypeWithoutCast(Types.DECIMAL_5_0)
                ),

                // DECIMAL (5,3)

                Arguments.of(
                        NumericPair.DECIMAL_5_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_5_3_DECIMAL_5_3,
                        ofTypeWithoutCast(Types.DECIMAL_5_3),
                        ofTypeWithoutCast(Types.DECIMAL_5_3)
                ),

                // DECIMAL (6,1)

                Arguments.of(
                        NumericPair.DECIMAL_6_1_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_6_1_DECIMAL_6_1,
                        ofTypeWithoutCast(Types.DECIMAL_6_1),
                        ofTypeWithoutCast(Types.DECIMAL_6_1)
                ),

                // DECIMAL (8,3)

                Arguments.of(
                        NumericPair.DECIMAL_8_3_REAL,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DOUBLE,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                ),

                Arguments.of(
                        NumericPair.DECIMAL_8_3_DECIMAL_8_3,
                        ofTypeWithoutCast(Types.DECIMAL_8_3),
                        ofTypeWithoutCast(Types.DECIMAL_8_3)
                )
        );
    }

    private static Stream<Arguments> inOperandsLiteralsOutOfRange() {
        return Stream.of(
                // TINYINT

                NumericPair.TINYINT_SMALLINT,
                NumericPair.TINYINT_INT,
                NumericPair.TINYINT_BIGINT,
                NumericPair.TINYINT_DECIMAL_2_1,
                NumericPair.TINYINT_DECIMAL_3_1,
                NumericPair.TINYINT_DECIMAL_4_3,
                NumericPair.TINYINT_DECIMAL_5_0,
                NumericPair.TINYINT_DECIMAL_5_3,
                NumericPair.TINYINT_DECIMAL_6_1,
                NumericPair.TINYINT_DECIMAL_8_3,

                // SMALLINT

                NumericPair.SMALLINT_INT,
                NumericPair.SMALLINT_BIGINT,
                NumericPair.SMALLINT_DECIMAL_2_1,
                NumericPair.SMALLINT_DECIMAL_3_1,
                NumericPair.SMALLINT_DECIMAL_4_3,
                NumericPair.SMALLINT_DECIMAL_5_0,
                NumericPair.SMALLINT_DECIMAL_5_3,
                NumericPair.SMALLINT_DECIMAL_6_1,
                NumericPair.SMALLINT_DECIMAL_8_3,

                // INT

                NumericPair.INT_BIGINT,
                NumericPair.INT_DECIMAL_2_1,
                NumericPair.INT_DECIMAL_3_1,
                NumericPair.INT_DECIMAL_4_3,
                NumericPair.INT_DECIMAL_5_3,
                NumericPair.INT_DECIMAL_6_1,
                NumericPair.INT_DECIMAL_8_3,

                // BIGINT

                NumericPair.BIGINT_DECIMAL_2_1,
                NumericPair.BIGINT_DECIMAL_3_1,
                NumericPair.BIGINT_DECIMAL_4_3,
                NumericPair.BIGINT_DECIMAL_5_3,
                NumericPair.BIGINT_DECIMAL_6_1,
                NumericPair.BIGINT_DECIMAL_8_3,

                // DECIMAL (1, 0)

                NumericPair.DECIMAL_1_0_DECIMAL_2_0,
                NumericPair.DECIMAL_1_0_DECIMAL_2_1,
                NumericPair.DECIMAL_1_0_DECIMAL_3_1,
                NumericPair.DECIMAL_1_0_DECIMAL_4_3,
                NumericPair.DECIMAL_1_0_DECIMAL_5_0,
                NumericPair.DECIMAL_1_0_DECIMAL_5_3,
                NumericPair.DECIMAL_1_0_DECIMAL_6_1,
                NumericPair.DECIMAL_1_0_DECIMAL_8_3,

                // DECIMAL (2, 0)

                NumericPair.DECIMAL_2_0_DECIMAL_3_1,
                NumericPair.DECIMAL_2_0_DECIMAL_5_0,
                NumericPair.DECIMAL_2_0_DECIMAL_5_3,
                NumericPair.DECIMAL_2_0_DECIMAL_6_1,
                NumericPair.DECIMAL_2_0_DECIMAL_8_3,

                // DECIMAL (2,1)

                NumericPair.DECIMAL_2_1_DECIMAL_2_0,
                NumericPair.DECIMAL_2_1_DECIMAL_3_1,
                NumericPair.DECIMAL_2_1_DECIMAL_4_3,
                NumericPair.DECIMAL_2_1_DECIMAL_5_0,
                NumericPair.DECIMAL_2_1_DECIMAL_5_3,
                NumericPair.DECIMAL_2_1_DECIMAL_6_1,
                NumericPair.DECIMAL_2_1_DECIMAL_8_3,

                // DECIMAL (3,1)

                NumericPair.DECIMAL_3_1_DECIMAL_5_0,
                NumericPair.DECIMAL_3_1_DECIMAL_5_3,
                NumericPair.DECIMAL_3_1_DECIMAL_6_1,
                NumericPair.DECIMAL_3_1_DECIMAL_8_3,

                // DECIMAL (4,3)

                NumericPair.DECIMAL_4_3_DECIMAL_2_0,
                NumericPair.DECIMAL_4_3_DECIMAL_3_1,
                NumericPair.DECIMAL_4_3_DECIMAL_5_0,
                NumericPair.DECIMAL_4_3_DECIMAL_5_3,
                NumericPair.DECIMAL_4_3_DECIMAL_6_1,
                NumericPair.DECIMAL_4_3_DECIMAL_8_3,

                // DECIMAL (5,0)

                NumericPair.DECIMAL_5_0_DECIMAL_6_1,
                NumericPair.DECIMAL_5_0_DECIMAL_8_3,

                // DECIMAL (5,3)

                NumericPair.DECIMAL_5_3_DECIMAL_5_0,
                NumericPair.DECIMAL_5_3_DECIMAL_6_1,
                NumericPair.DECIMAL_5_3_DECIMAL_8_3,

                // DECIMAL (6,1)

                NumericPair.DECIMAL_6_1_DECIMAL_8_3
        ).map(Arguments::of);
    }
}
