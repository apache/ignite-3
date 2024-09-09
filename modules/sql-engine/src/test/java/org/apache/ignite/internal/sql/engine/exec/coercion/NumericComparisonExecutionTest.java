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
import static org.apache.ignite.internal.sql.engine.exec.coercion.BaseTypeCheckExecutionTest.generateConstantValueByType;
import static org.apache.ignite.internal.sql.engine.exec.coercion.BaseTypeCheckExecutionTest.generateDifferentValues;
import static org.apache.ignite.internal.sql.engine.exec.coercion.BaseTypeCheckExecutionTest.testCluster;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.exec.coercion.BaseTypeCheckExecutionTest.ClusterWrapper;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Check execution and return results for numeric operations. */
public class NumericComparisonExecutionTest extends BaseIgniteAbstractTest {
    private static final Set<NativeType> FRACTIONAL_TYPES = Set.of(NativeTypes.DOUBLE, NativeTypes.FLOAT);

    private static final Map<NumericPair, Class<?>> NUMERIC_OP_TYPES = new EnumMap<>(NumericPair.class);

    @ParameterizedTest
    @MethodSource("comparisonWithEqArgs")
    public void comparisonEq(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("comparisonNotEqArgs")
    public void comparisonNotEq(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, nonEqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("sumArgs")
    public void sumOp(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        // IGNITE-23192, eqDataProvider (generate small numeric, no overflow is possible) need to be changed for binOpDataProvider
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("subtractArgs")
    // IGNITE-23192, eqDataProvider (generate small numeric, no overflow is possible) need to be changed for binOpDataProvider
    public void subtractOp(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("divArgs")
    public void divOp(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        Assumptions.assumeFalse(typePair.first() instanceof DecimalNativeType || typePair.second() instanceof DecimalNativeType,
                "need to be fixed after: https://issues.apache.org/jira/browse/IGNITE-23171");

        try (ClusterWrapper testCluster = testCluster(typePair, binOpDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("multArgs")
    // IGNITE-23192, eqDataProvider (generate small numeric, no overflow is possible) need to be changed for binOpDataProvider
    public void multOp(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("moduloArgs")
    public void moduloOp(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        // modulo undefined for fractional types
        if (FRACTIONAL_TYPES.contains(typePair.first()) || FRACTIONAL_TYPES.contains(typePair.second())) {
            return;
        }

        try (ClusterWrapper testCluster = testCluster(typePair, binOpDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    /**
     * This test ensures that {@link #NUMERIC_OP_TYPES} doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        EnumSet<NumericPair> remainingPairs = EnumSet.allOf(NumericPair.class);

        remainingPairs.removeAll(NUMERIC_OP_TYPES.keySet());

        assertTrue(remainingPairs.isEmpty(), () ->
                "Not all types are enlisted, remaining: " + remainingPairs);
    }

    private static DataProvider<Object[]> eqDataProvider(TypePair typePair) {
        Object val1;
        Object val2;

        if (typePair.first().equals(typePair.second())) {
            val1 = generateConstantValueByType(typePair.first());
            val2 = val1;
        } else {
            val1 = generateConstantValueByType(typePair.first());
            val2 = generateConstantValueByType(typePair.second());
        }

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    private static DataProvider<Object[]> nonEqDataProvider(TypePair typePair) {
        Pair<Object, Object> objPair = generateDifferentValues(typePair);
        Object val1 = objPair.getFirst();
        Object val2 = objPair.getSecond();

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    private static DataProvider<Object[]> binOpDataProvider(TypePair typePair) {
        Object val1 = SqlTestUtils.generateValueByType(typePair.first());
        Object val2 = SqlTestUtils.generateValueByType(typePair.second());

        return DataProvider.fromRow(new Object[]{0, val1, val2}, 1);
    }

    private static Stream<Arguments> comparisonWithEqArgs() {
        Stream<Arguments> s1 = Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 = c2 FROM t").ok());
        Stream<Arguments> s2 = Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 <= c2 FROM t").ok());
        Stream<Arguments> s3 = Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 >= c2 FROM t").ok());
        Stream<Arguments> s4 = Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 < c2 FROM t").fail());
        Stream<Arguments> s5 = Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 > c2 FROM t").fail());

        return Stream.of(s1, s2, s3, s4, s5).flatMap(Function.identity());
    }

    private static Stream<Arguments> comparisonNotEqArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 = c2 FROM t").fail());
    }

    private static Stream<Arguments> sumArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 + c2 FROM t").type(a));
    }

    private static Stream<Arguments> subtractArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 - c2 FROM t").type(a));
    }

    private static Stream<Arguments> multArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 * c2 FROM t").type(a));
    }

    private static Stream<Arguments> divArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 / c2 FROM t").type(a));
    }

    private static Stream<Arguments> moduloArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 % c2 FROM t").type(a));
    }

    private static ExecutionResultBuilder forTypePair(TypePair typePair, String exp) {
        return new ExecutionResultBuilder(typePair, exp);
    }

    static class ExecutionResultBuilder {
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

        Arguments type(NumericPair type) {
            opMatcher = ofType(type);

            return Arguments.of(pair, expression, opMatcher);
        }
    }

    private static Matcher<Object> ofBoolType(Boolean compResult) {
        return new BaseMatcher<>() {
            Object actual;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                this.actual = actual;
                return compResult.equals(actual);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}'", compResult, actual));
            }
        };
    }

    private static Matcher<Object> ofType(NumericPair typesPair) {
        return new BaseMatcher<>() {
            Object actual;
            Class<?> innerClazz;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                this.actual = actual;

                Class<?> innerClazz = NUMERIC_OP_TYPES.get(typesPair);

                return innerClazz.isInstance(actual);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}'", innerClazz, actual.getClass()));
            }
        };
    }

    static {
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_TINYINT, Byte.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_SMALLINT, Short.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_INT, Integer.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_BIGINT, Long.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_1_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_REAL, Float.class);
        NUMERIC_OP_TYPES.put(NumericPair.TINYINT_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_SMALLINT, Short.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_INT, Integer.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_BIGINT, Long.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_1_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_REAL, Float.class);
        NUMERIC_OP_TYPES.put(NumericPair.SMALLINT_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.INT_INT, Integer.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_BIGINT, Long.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_1_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_REAL, Float.class);
        NUMERIC_OP_TYPES.put(NumericPair.INT_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_BIGINT, Long.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_1_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_REAL, Float.class);
        NUMERIC_OP_TYPES.put(NumericPair.BIGINT_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_1_0_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_1_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_4_3_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_2_0_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_3_1_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_3_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_0_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_5_0_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_6_1_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_6_1_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, BigDecimal.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_8_3_REAL, Double.class);
        NUMERIC_OP_TYPES.put(NumericPair.DECIMAL_8_3_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.REAL_REAL, Float.class);
        NUMERIC_OP_TYPES.put(NumericPair.REAL_DOUBLE, Double.class);

        NUMERIC_OP_TYPES.put(NumericPair.DOUBLE_DOUBLE, Double.class);
    }
}
