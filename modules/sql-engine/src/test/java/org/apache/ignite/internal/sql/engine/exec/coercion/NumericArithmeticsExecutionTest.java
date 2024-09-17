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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.planner.datatypes.BaseTypeCoercionTest.checkIncludesAllNumericTypePairs;
import static org.apache.ignite.internal.sql.engine.planner.datatypes.BaseTypeCoercionTest.forTypePair;

import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnMetadata;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Check execution and return results for numeric arithmetics. */
public class NumericArithmeticsExecutionTest extends BaseTypeCheckExecutionTest {
    private static final Set<NativeType> APPROXIMATE_NUMERIC_TYPES = Set.of(NativeTypes.DOUBLE, NativeTypes.FLOAT);

    @ParameterizedTest
    @MethodSource("sumArgs")
    public void sumOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 + c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("subtractArgs")
    public void subtractOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 - c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("multArgs")
    public void multOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 * c2 FROM t";
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("divArgs")
    public void divOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        String sql = "SELECT c1 / c2 FROM t";
        Assumptions.assumeFalse(typePair.first() instanceof DecimalNativeType || typePair.second() instanceof DecimalNativeType,
                "need to be fixed after: https://issues.apache.org/jira/browse/IGNITE-23171");

        try (ClusterWrapper testCluster = testCluster(typePair, binOpDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("moduloArgs")
    public void moduloOp(TypePair typePair, Matcher<Object> resultMatcher) throws Exception {
        // modulo is undefined for fractional types
        if (APPROXIMATE_NUMERIC_TYPES.contains(typePair.first()) || APPROXIMATE_NUMERIC_TYPES.contains(typePair.second())) {
            return;
        }

        String sql = "SELECT c1 % c2 FROM t";

        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    /**
     * This test ensures that object mapping doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void checkAllTypePairs() {
        checkIncludesAllNumericTypePairs(sumArgs());
        checkIncludesAllNumericTypePairs(subtractArgs());
        checkIncludesAllNumericTypePairs(multArgs());
        checkIncludesAllNumericTypePairs(divArgs());
        checkIncludesAllNumericTypePairs(moduloArgs());
    }

    private static DataProvider<Object[]> binOpDataProvider(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = SqlTestUtils.generateValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    private static Stream<Arguments> sumArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.TINYINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.SMALLINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.INT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.BIGINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_3_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_6_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_8_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_8_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.REAL_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.REAL_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DOUBLE_DOUBLE).resultWillBe(NativeTypes.DOUBLE)
        );
    }

    private static Stream<Arguments> subtractArgs() {
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

    private static Stream<Arguments> multArgs() {
        return Stream.of(
                forTypePair(NumericPair.TINYINT_TINYINT).resultWillBe(NativeTypes.INT8),
                forTypePair(NumericPair.TINYINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.TINYINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.TINYINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.TINYINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.TINYINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.TINYINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.TINYINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.TINYINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.SMALLINT_SMALLINT).resultWillBe(NativeTypes.INT16),
                forTypePair(NumericPair.SMALLINT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.SMALLINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.SMALLINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.SMALLINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.SMALLINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.SMALLINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.SMALLINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.INT_INT).resultWillBe(NativeTypes.INT32),
                forTypePair(NumericPair.INT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.INT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.INT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.INT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.INT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.INT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.BIGINT_BIGINT).resultWillBe(NativeTypes.INT64),
                forTypePair(NumericPair.BIGINT_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.BIGINT_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.BIGINT_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.BIGINT_REAL).resultWillBe(NativeTypes.FLOAT),
                forTypePair(NumericPair.BIGINT_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_1_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_4_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_1_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_1_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_1_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_4_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_2_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_2_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_2_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_4_3).resultWillBe(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_2_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_3_1).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_4_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_4_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_4_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_2_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_3_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_2_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_2_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_2_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_3_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_5_0).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_3_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_3_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_3_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_3).resultWillBe(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_5_0).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_6_1).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_5_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_8_6),
                forTypePair(NumericPair.DECIMAL_5_3_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_3_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_5_0).resultWillBe(Types.DECIMAL_2_0),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_6_1).resultWillBe(Types.DECIMAL_3_1),
                forTypePair(NumericPair.DECIMAL_5_0_DECIMAL_8_3).resultWillBe(Types.DECIMAL_5_3),
                forTypePair(NumericPair.DECIMAL_5_0_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_5_0_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_6_1).resultWillBe(Types.DECIMAL_4_2),
                forTypePair(NumericPair.DECIMAL_6_1_DECIMAL_8_3).resultWillBe(Types.DECIMAL_6_4),
                forTypePair(NumericPair.DECIMAL_6_1_REAL).resultWillBe(NativeTypes.DOUBLE),
                forTypePair(NumericPair.DECIMAL_6_1_DOUBLE).resultWillBe(NativeTypes.DOUBLE),

                forTypePair(NumericPair.DECIMAL_8_3_DECIMAL_8_3).resultWillBe(Types.DECIMAL_8_6),
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

/*
    private static ExecutionResultBuilder forTypePair1(TypePair typePair, String exp) {
        return new ExecutionResultBuilder(typePair, exp);
    }*/

/*    static class ExecutionResultBuilder {
        private final TypePair pair;
        private Matcher<?> opMatcher;
        private final String expression;

        private ExecutionResultBuilder(TypePair pair, String exp) {
            this.pair = pair;
            expression = exp;
        }

        Arguments fail() {
            opMatcher = ofBoolType(false);

            return Arguments.of(pair, expression, opMatcher);
        }

        Arguments ok() {
            opMatcher = ofBoolType(true);

            return Arguments.of(pair, expression, opMatcher);
        }

        Arguments type(NumericPair type, Operation op) {
            opMatcher = ofType(type, op);

            return Arguments.of(pair, expression, opMatcher);
        }
    }*/

    private static Matcher<Object> ofBoolType(Boolean compResult) {
        return new BaseMatcher<>() {
            Object actual;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                Pair<Object, ColumnMetadata> pair = (Pair<Object, ColumnMetadata>) actual;
                this.actual = pair.getFirst();
                return compResult.equals(this.actual);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}'", compResult, actual));
            }
        };
    }
}
