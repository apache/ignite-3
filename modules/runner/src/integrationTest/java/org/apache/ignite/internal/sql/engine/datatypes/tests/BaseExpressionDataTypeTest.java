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

package org.apache.ignite.internal.sql.engine.datatypes.tests;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for data type in expressions.
 *
 * @param <T> A storage type of a data type.
 */
public abstract class BaseExpressionDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /**
     * Binary comparison operator tests.
     */
    @ParameterizedTest
    @MethodSource("cmp")
    public void testComparison(TestTypeArguments<T> arguments) {
        checkComparison(arguments, 0, 1);
    }

    /**
     * Binary comparison operator tests with dynamic parameters.
     */
    @Test
    public void testComparisonDynamicParameters() {
        Comparable<?> lower = dataSamples.min();
        Comparable<?> higher = dataSamples.max();

        checkComparisonWithDynamicParameters(higher, lower);
    }

    private Stream<TestTypeArguments<T>> cmp() {
        return TestTypeArguments.binary(testTypeSpec, dataSamples, dataSamples.min(), dataSamples.max());
    }

    /**
     * {@code IN} operator.
     */
    @ParameterizedTest
    @MethodSource("in")
    public void testIn(TestTypeArguments<T> arguments) {
        String lhs = arguments.valueExpr(0);
        String value2 = arguments.valueExpr(1);
        String value3 = arguments.valueExpr(2);

        String query = format("SELECT {} IN ({}, {})", lhs, value2, value3);
        checkQuery(query).returns(true).check();
    }

    private Stream<TestTypeArguments<T>> in() {
        T value1 = values.get(0);
        T value2 = values.get(1);

        return TestTypeArguments.nary(testTypeSpec, dataSamples, value1, value1, value2);
    }

    /**
     * {@code NOT IN} operator.
     */
    @ParameterizedTest
    @MethodSource("notIn")
    public void testNotIn(TestTypeArguments<T> arguments) {
        String lhs = arguments.valueExpr(0);
        String value1 = arguments.valueExpr(1);
        String value2 = arguments.valueExpr(2);

        String query = format("SELECT {} NOT IN ({}, {})", lhs, value1, value2);
        checkQuery(query).returns(true).check();
    }

    private Stream<TestTypeArguments<T>> notIn() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        return TestTypeArguments.nary(testTypeSpec, dataSamples, value3, value1, value2);
    }

    /**
     * {@code COALESCE} operator.
     */
    @Test
    public void testCoalesce() {
        T value1 = values.get(0);

        checkQuery("SELECT COALESCE($0, $1)").returns(value1).check();

        // Checks that NULL type is properly handled by by typeFactory::leastRestrictiveType
        checkQuery("SELECT COALESCE(NULL, $0)").returns(value1).check();

        checkQuery("SELECT COALESCE($0, NULL)").returns(value1).check();
    }

    /**
     * {@code NULLIF} operator.
     */
    @Test
    public void testNullIf() {
        T value1 = values.get(0);

        checkQuery("SELECT NULLIF($0, $1)")
                .returns(value1)
                .check();

        checkQuery("SELECT NULLIF($0, $0)")
                .returns(new Object[]{null})
                .check();
    }

    /**
     * {@code CASE WHEN <expr_with_custom_type> THEN .. ELSE .. END}.
     */
    @Test
    public void testCaseInWhen() {
        T value1 = values.get(0);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        String query = format("SELECT CASE WHEN test_key = $0 THEN id ELSE 0 END FROM t ORDER BY id ASC", value1);
        checkQuery(query)
                .returns(1)
                .returns(0)
                .returns(0)
                .check();
    }

    /**
     * {@code CASE WHEN .. THEN <expr_with_custom_type> ELSE .. END}.
     */
    @Test
    public void testCaseInResult() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        String query = format(
                "SELECT CASE WHEN id = 1 THEN test_key WHEN id = 2 THEN test_key ELSE $2 END FROM t ORDER BY id ASC",
                value3
        );
        checkQuery(query)
                .returns(value1)
                .returns(value2)
                .returns(value3)
                .check();
    }

    /**
     * The same as {@link #testCaseInResult()} but for "simple" form of case expression {@code CASE <expr> WHEN .. END}.
     */
    @Test
    public void testSimpleCase() {
        T value1 = values.get(0);
        T value2 = values.get(1);
        T value3 = values.get(2);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        String query = format(
                "SELECT CASE id > 0 WHEN id = 1 THEN test_key WHEN id = 2 THEN test_key ELSE $2 END FROM t ORDER BY id ASC"
        );
        checkQuery(query)
                .returns(value1)
                .returns(value2)
                .returns(value3)
                .check();
    }

    /**
     * {@code BETWEEN} operator.
     */
    @ParameterizedTest
    @MethodSource("between")
    public void testBetweenWithDynamicParameters(TestTypeArguments<T> arguments) {
        checkQuery("SELECT ?::<type> BETWEEN ? AND ?")
                .withParams(arguments.value(0), arguments.value(1), arguments.value(2))
                .returns(true)
                .check();
    }

    /**
     * {@code BETWEEN} operator.
     */
    @ParameterizedTest
    @MethodSource("between")
    public void testBetweenWithLiterals(TestTypeArguments<T> arguments) {
        Assumptions.assumeTrue(testTypeSpec.hasLiterals(), "BETWEEN only works for types that has literals");

        String query = format("SELECT ?::<type> BETWEEN {} AND {}", arguments.argLiteral(1), arguments.argLiteral(2));

        checkQuery(query)
                .withParams(arguments.value(0))
                .returns(true)
                .check();
    }

    private Stream<TestTypeArguments<T>> between() {
        T value1 = orderedValues.first();
        T value2 = orderedValues.higher(value1);
        T value3 = orderedValues.last();

        return TestTypeArguments.nary(testTypeSpec, dataSamples, value2, value1, value3);
    }

    /**
     * {@code TYPE_OF} function.
     */
    @Test
    public void testTypeOf() {
        T value = values.get(0);
        String typeName = testTypeSpec.typeName();

        checkQuery("SELECT typeof(?)").withParams(value).returns(typeName).check();
        checkQuery("SELECT typeof(CAST(? as <type>))").withParams(value).returns(typeName).check();
    }

    /** Dynamic parameter. **/
    @Test
    public void testDynamicParam() {
        T value = dataSamples.min();

        assertQuery("SELECT ?").withParams(value)
                .returns(value)
                .columnMetadata(new MetadataMatcher().type(testTypeSpec.columnType()))
                .check();
    }

    /**
     * Runs binary comparison checks with dynamic parameters.
     */
    protected final void checkComparisonWithDynamicParameters(Comparable<?> high, Comparable<?> low) {
        if (Objects.equals(high, low)) {
            throw new IllegalArgumentException(format("Values must not be equal. Value1: {}, value2: {}", high, low));
        }

        checkQuery("SELECT ? = ?").withParams(high, high).returns(true).check();
        checkQuery("SELECT ? != ?").withParams(high, high).returns(false).check();
        checkQuery("SELECT ? <> ?").withParams(high, high).returns(false).check();

        checkQuery("SELECT ? IS NOT DISTINCT FROM ?").withParams(high, high).returns(true).check();
        checkQuery("SELECT ? IS DISTINCT FROM ?").withParams(high, high).returns(false).check();

        checkQuery("SELECT ? = ?").withParams(high, low).returns(false).check();
        checkQuery("SELECT ? != ?").withParams(high, low).returns(true).check();
        checkQuery("SELECT ? <> ?").withParams(high, low).returns(true).check();

        checkQuery("SELECT ? > ?").withParams(low, high).returns(false).check();
        checkQuery("SELECT ? >= ?").withParams(low, high).returns(false).check();
        checkQuery("SELECT ? < ?").withParams(low, high).returns(true).check();
        checkQuery("SELECT ? <= ?").withParams(low, high).returns(true).check();

        checkQuery("SELECT ? > ?").withParams(high, low).returns(true).check();
        checkQuery("SELECT ? >= ?").withParams(high, low).returns(true).check();
        checkQuery("SELECT ? < ?").withParams(high, low).returns(false).check();
        checkQuery("SELECT ? <= ?").withParams(high, low).returns(false).check();
    }

    /**
     * Runs binary comparison checks for given arguments.
     */
    protected final void checkComparison(TestTypeArguments<?> arguments, int lowIdx, int highIdx) {
        String low = arguments.valueExpr(lowIdx);
        String high = arguments.valueExpr(highIdx);

        checkComparisonQuery(high, "=", high).returns(true).check();
        checkComparisonQuery(high, "!=", high).returns(false).check();
        checkComparisonQuery(high, "<>", high).returns(false).check();

        checkComparisonQuery(high, "IS NOT DISTINCT FROM", high).returns(true).check();
        checkComparisonQuery(high, "IS DISTINCT FROM", high).returns(false).check();

        checkComparisonQuery(high, "=", low).returns(false).check();
        checkComparisonQuery(high, "!=", low).returns(true).check();
        checkComparisonQuery(high, "<>", low).returns(true).check();

        checkComparisonQuery(low, ">", high).returns(false).check();
        checkComparisonQuery(low, ">=", high).returns(false).check();

        checkComparisonQuery(low, "<", high).returns(true).check();
        checkComparisonQuery(low, "<=", high).returns(true).check();

        checkComparisonQuery(high, ">", low).returns(true).check();
        checkComparisonQuery(high, ">=", low).returns(true).check();

        checkComparisonQuery(high, "<", low).returns(false).check();
        checkComparisonQuery(high, "<=", low).returns(false).check();
    }

    private QueryChecker checkComparisonQuery(String lhsExpr, String op, String rhsExpr) {
        String query = format("SELECT {} {} {}", lhsExpr, op, rhsExpr);
        return checkQuery(query);
    }
}
