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

package org.apache.ignite.internal.sql.engine.exec.coercion;

import static org.apache.ignite.internal.sql.engine.planner.datatypes.BaseTypeCoercionTest.checkIncludesAllNumericTypePairs;
import static org.apache.ignite.internal.sql.engine.planner.datatypes.BaseTypeCoercionTest.forTypePair;

import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Check execution and return results for numeric arithmetics. */
public class NumericArithmeticsExecutionTest extends BaseTypeCheckExecutionTest {
    @ParameterizedTest
    @MethodSource("plusMinusArgs")
    public void sumOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 + c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderReduced(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("plusMinusArgs")
    public void subtractOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 - c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderReduced(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("multArgs")
    public void multOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 * c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, multDivDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("divArgs")
    public void divOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 / c2 FROM t";
        Assumptions.assumeFalse(typePair.first() instanceof DecimalNativeType || typePair.second() instanceof DecimalNativeType,
                "need to be fixed after: https://issues.apache.org/jira/browse/IGNITE-23171");

        try (ClusterWrapper testCluster = testCluster(typePair, dataProviderStrict(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("moduloArgs")
    public void moduloOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 % c2 FROM t";

        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @Test
    public void checkAllTypePairs() {
        checkIncludesAllNumericTypePairs(plusMinusArgs());
        checkIncludesAllNumericTypePairs(multArgs());
        checkIncludesAllNumericTypePairs(divArgs());
    }

    private static Stream<Arguments> plusMinusArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.TINYINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.TINYINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.SMALLINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.SMALLINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_14_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_14_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_14_3),
                forTypePair(NumericPair.INT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.INT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_20_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_21_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_23_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_20_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_21_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_23_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_20_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_21_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_23_3),
                forTypePair(NumericPair.BIGINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.BIGINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_1_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_2_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_4_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_2_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_3_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_5_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_5_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_6_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_8_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.REAL_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.REAL_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DOUBLE_DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> multArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_3),
                forTypePair(NumericPair.TINYINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.TINYINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_13_3),
                forTypePair(NumericPair.SMALLINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.SMALLINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_14_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_13_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_15_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_15_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_16_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_18_3),
                forTypePair(NumericPair.INT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.INT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_20_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_21_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_23_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_21_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_22_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_24_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_24_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_25_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_27_3),
                forTypePair(NumericPair.BIGINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.BIGINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_1_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_4),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_4),
                forTypePair(NumericPair.DECIMAL_2_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_4),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_6),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_4),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_6),
                forTypePair(NumericPair.DECIMAL_4_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_7_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_3),
                forTypePair(NumericPair.DECIMAL_2_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_2),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_4),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_9_2),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_4),
                forTypePair(NumericPair.DECIMAL_3_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_6),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_4),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_13_6),
                forTypePair(NumericPair.DECIMAL_5_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_10_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_11_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_13_3),
                forTypePair(NumericPair.DECIMAL_5_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_12_2),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_14_4),
                forTypePair(NumericPair.DECIMAL_6_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_16_6),
                forTypePair(NumericPair.DECIMAL_8_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.REAL_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.REAL_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DOUBLE_DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> divArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.TINYINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.TINYINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.SMALLINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.SMALLINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.INT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.INT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.BIGINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.BIGINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_1_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_2_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_4_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_2_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_3_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_5_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_5_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_1_1),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_6_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_1_3),
                forTypePair(NumericPair.DECIMAL_8_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.REAL_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.REAL_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DOUBLE_DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> moduloArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_1_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_3),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_1_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_1_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_1_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_1_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_4_3),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_3),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBeEqOrLessPrecision(Types.DECIMAL_5_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBeEqOrLessPrecision(Types.DECIMAL_6_1),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBeEqOrLessPrecision(Types.DECIMAL_8_3)
        );
    }
}
