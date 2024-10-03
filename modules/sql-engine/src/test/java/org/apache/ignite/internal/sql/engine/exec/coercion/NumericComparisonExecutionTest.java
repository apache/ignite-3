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

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.NumericPair;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.TypePair;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnMetadata;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Check execution results for numeric comparisons. */
public class NumericComparisonExecutionTest extends BaseTypeCheckExecutionTest {
    @ParameterizedTest
    @MethodSource("comparisonWithEqArgs")
    public void comparisonEq(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("comparisonWithLessEqArgs")
    public void comparisonLessEq(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("comparisonWithGreatEqArgs")
    public void comparisonGreatEq(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("comparisonWithLessArgs")
    public void comparisonLess(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    @ParameterizedTest
    @MethodSource("comparisonWithGreatArgs")
    public void comparisonGreat(TypePair typePair, String sql, Matcher<Object> resultMatcher) throws Exception {
        try (ClusterWrapper testCluster = testCluster(typePair, dataProvider(typePair))) {
            testCluster.process(sql, resultMatcher);
        }
    }

    private static Stream<Arguments> comparisonWithEqArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 = c2 FROM t").ofBooleanType());
    }

    private static Stream<Arguments> comparisonWithLessEqArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 <= c2 FROM t").ofBooleanType());
    }

    private static Stream<Arguments> comparisonWithGreatEqArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 >= c2 FROM t").ofBooleanType());
    }

    private static Stream<Arguments> comparisonWithLessArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 < c2 FROM t").ofBooleanType());
    }

    private static Stream<Arguments> comparisonWithGreatArgs() {
        return Arrays.stream(NumericPair.values()).map(a -> forTypePair(a, "SELECT c1 < c2 FROM t").ofBooleanType());
    }

    private static ExecutionResultHolder forTypePair(NumericPair pair, String expr) {
        return new ExecutionResultHolder(pair, expr);
    }

    static class ExecutionResultHolder {
        private final NumericPair pair;
        private final String expr;

        ExecutionResultHolder(NumericPair pair, String expr) {
            this.pair = pair;
            this.expr = expr;
        }

        Arguments ofBooleanType() {
            return Arguments.of(pair, expr, ofBoolType());
        }
    }

    private static Matcher<Object> ofBoolType() {
        return new BaseMatcher<>() {
            Object actual;

            @Override
            public boolean matches(Object actual) {
                assert actual != null;
                this.actual = ((Pair<Object, ColumnMetadata>) actual).getFirst();
                Pair<Object, ColumnMetadata> pair = (Pair<Object, ColumnMetadata>) actual;
                return pair.getFirst().getClass() == Boolean.class;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("Expected : '{}' but found '{}'", Boolean.class, actual.getClass()));
            }
        };
    }
}
