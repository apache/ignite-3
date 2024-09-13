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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
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

/** Check execution and return results for numeric operations. */
public class NumericComparisonExecutionTest extends BaseTypeCheckExecutionTest {
    private static final Set<NativeType> FRACTIONAL_TYPES = Set.of(NativeTypes.DOUBLE, NativeTypes.FLOAT);

    private static final Map<NumericPair, ClassInfoHolder> NUMERIC_OP_TYPES_SUM = new EnumMap<>(NumericPair.class);
    private static final Map<NumericPair, ClassInfoHolder> NUMERIC_OP_TYPES_SUBTRACT = new EnumMap<>(NumericPair.class);
    private static final Map<NumericPair, ClassInfoHolder> NUMERIC_OP_TYPES_MULT = new EnumMap<>(NumericPair.class);
    private static final Map<NumericPair, ClassInfoHolder> NUMERIC_OP_TYPES_DIV = new EnumMap<>(NumericPair.class);
    private static final Map<NumericPair, ClassInfoHolder> NUMERIC_OP_TYPES_MODULO = new EnumMap<>(NumericPair.class);

    enum Operation {
        SUM, SUBTRACT, MULT, DIV, MODULO
    }

    private static final Map<Operation, Map<NumericPair, ClassInfoHolder>> TYPES_MAPPING = Map.of(
            Operation.SUM, NUMERIC_OP_TYPES_SUM,
            Operation.SUBTRACT, NUMERIC_OP_TYPES_SUBTRACT,
            Operation.MULT, NUMERIC_OP_TYPES_MULT,
            Operation.DIV, NUMERIC_OP_TYPES_DIV,
            Operation.MODULO, NUMERIC_OP_TYPES_MODULO
    );


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
        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("subtractArgs")
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

        try (ClusterWrapper testCluster = testCluster(typePair, eqDataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    /**
     * This test ensures that object mapping doesn't miss any type pair from {@link NumericPair}.
     */
    @Test
    void argsIncludesAllTypePairs() {
        for (Map<NumericPair, ClassInfoHolder> ent : TYPES_MAPPING.values()) {
            EnumSet<NumericPair> remainingPairs = EnumSet.allOf(NumericPair.class);

            remainingPairs.removeAll(ent.keySet());

            assertTrue(remainingPairs.isEmpty(), () ->
                    "Not all types are enlisted, remaining: " + remainingPairs);
        }
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
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 + c2 FROM t").type(a, Operation.SUM));
    }

    private static Stream<Arguments> subtractArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 - c2 FROM t").type(a, Operation.SUBTRACT));
    }

    private static Stream<Arguments> multArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 * c2 FROM t").type(a, Operation.MULT));
    }

    private static Stream<Arguments> divArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 / c2 FROM t").type(a, Operation.DIV));
    }

    private static Stream<Arguments> moduloArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 % c2 FROM t").type(a, Operation.MODULO));
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

        Arguments type(NumericPair type, Operation op) {
            opMatcher = ofType(type, op);

            return Arguments.of(pair, expression, opMatcher);
        }
    }

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

    private static Matcher<Object> ofType(NumericPair typesPair, Operation op) {
        return new BaseMatcher<>() {
            Object actual;
            ClassInfoHolder innerClassInfo;
            int precision = 0;
            int scale = 0;
            ColumnMetadata colMeta;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                Pair<Object, ColumnMetadata> pair = (Pair<Object, ColumnMetadata>) actual;
                this.actual = pair.getFirst();
                colMeta = pair.getSecond();

                boolean checkPrecisionScale = false;

                if (this.actual instanceof BigDecimal) {
                    precision = ((BigDecimal) this.actual).precision();
                    scale = ((BigDecimal) this.actual).scale();
                    checkPrecisionScale = true;
                }

                innerClassInfo = TYPES_MAPPING.get(op).get(typesPair);

                boolean precCheck = checkPrecisionScale ? colMeta.precision() >= precision && colMeta.scale() >= scale : true;

                // negative scale in meta for operations with integer and real\double types.
                return precCheck && innerClassInfo.clazz.isInstance(this.actual)
                        && innerClassInfo.precision == precision
                        && innerClassInfo.scale == scale;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}, precision: {}, scale: {}', column meta: {}",
                        innerClassInfo, actual.getClass(), precision, scale, colMeta));
            }
        };
    }

    static {
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_TINYINT, classInfo(Byte.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.TINYINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.SMALLINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.INT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.BIGINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_1_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_4_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_2_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_3_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_5_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_6_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_6_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_8_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.DECIMAL_8_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.REAL_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUM.put(NumericPair.REAL_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUM.put(NumericPair.DOUBLE_DOUBLE, classInfo(Double.class));
    }

    static {
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_TINYINT, classInfo(Byte.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.TINYINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.SMALLINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.INT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.BIGINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_1_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_4_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_2_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_3_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_5_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_6_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_6_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_8_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DECIMAL_8_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.REAL_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.REAL_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_SUBTRACT.put(NumericPair.DOUBLE_DOUBLE, classInfo(Double.class));
    }

    static {
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_TINYINT, classInfo(Byte.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.TINYINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.SMALLINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.INT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.BIGINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_1_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_4_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_2_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_3_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_5_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, classInfo(BigDecimal.class, 4, 2));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, classInfo(BigDecimal.class, 6, 4));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_6_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_6_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, classInfo(BigDecimal.class, 8, 6));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_8_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.DECIMAL_8_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.REAL_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MULT.put(NumericPair.REAL_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MULT.put(NumericPair.DOUBLE_DOUBLE, classInfo(Double.class));
    }

    static {
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_TINYINT, classInfo(Byte.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.TINYINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.SMALLINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.INT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.BIGINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_1_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_4_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_2_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_3_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, classInfo(BigDecimal.class, 2, 0));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_5_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, classInfo(BigDecimal.class, 3, 1));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_6_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_6_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, classInfo(BigDecimal.class, 5, 3));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_8_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.DECIMAL_8_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.REAL_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_DIV.put(NumericPair.REAL_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_DIV.put(NumericPair.DOUBLE_DOUBLE, classInfo(Double.class));
    }

    static {
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_TINYINT, classInfo(Byte.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.TINYINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_SMALLINT, classInfo(Short.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.SMALLINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_INT, classInfo(Integer.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.INT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_BIGINT, classInfo(Long.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.BIGINT_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_1_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_1_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_2_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_4_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_4_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_2_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_2_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DECIMAL_3_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_3_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_DECIMAL_5_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_0_DECIMAL_5_0, classInfo(BigDecimal.class, 1, 0));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_0_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_0_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_0_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_5_0_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_6_1_DECIMAL_6_1, classInfo(BigDecimal.class, 1, 1));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_6_1_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_6_1_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_6_1_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_8_3_DECIMAL_8_3, classInfo(BigDecimal.class, 1, 3));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_8_3_REAL, classInfo(Double.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DECIMAL_8_3_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.REAL_REAL, classInfo(Float.class));
        NUMERIC_OP_TYPES_MODULO.put(NumericPair.REAL_DOUBLE, classInfo(Double.class));

        NUMERIC_OP_TYPES_MODULO.put(NumericPair.DOUBLE_DOUBLE, classInfo(Double.class));
    }
}
