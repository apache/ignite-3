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

import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for predicates in {@code WHERE} clause of a query.
 */
public abstract class BaseQueryDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /** No predicate. */
    @Test
    public void testSelect() {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");

        checkQuery("SELECT * FROM t")
                .columnTypes(Integer.class, storageType)
                .check();
    }

    /** Query against an empty table. */
    @Test
    public void testSelectFromEmpty() {
        checkQuery("SELECT id, test_key FROM t")
                .columnTypes(Integer.class, storageType)
                .returnNothing()
                .check();
    }

    /** Test for equality predicate. */
    @ParameterizedTest
    @MethodSource("eq")
    public void testEqCondition(TestTypeArguments<T> arguments) {
        Object value1 = testTypeSpec.unwrapIfNecessary(values.get(0));
        Object value2 = testTypeSpec.unwrapIfNecessary(values.get(1));

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);
        runSql("INSERT INTO t VALUES(3, ?)", value1);

        String query = format("SELECT id FROM t where test_key = {} ORDER BY id", arguments.valueExpr(0));
        checkQuery(query)
                .returns(1)
                .returns(3)
                .check();

        String query2 = format("SELECT id FROM t where test_key != {} ORDER BY id", arguments.valueExpr(0));
        checkQuery(query2)
                .returns(2)
                .check();
    }

    /** Test for equality predicate with dynamic parameter. */
    @Test
    public void testEqConditionDynamicParam() {
        Object value1 = unwrap(values.get(0));
        Object value2 = unwrap(values.get(1));

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);
        runSql("INSERT INTO t VALUES(3, ?)", value1);

        checkQuery("SELECT id FROM t where test_key = ? ORDER BY id")
                .withParams(value1)
                .returns(1)
                .returns(3)
                .check();

        checkQuery("SELECT id FROM t where test_key != ? ORDER BY id")
                .withParams(value1)
                .returns(2)
                .check();
    }

    private Stream<TestTypeArguments<T>> eq() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, values.get(0));
    }

    /** {@code IN} expression. */
    @Test
    public void testIn() {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        checkQuery("SELECT id FROM t where test_key IN ($0, $1)")
                .returns(1)
                .returns(2)
                .check();
    }

    /** {@code IN} expression with dynamic parameters. */
    @Test
    public void testInWithDynamicParamsCondition() {
        T value1 = values.get(0);
        T value2 = values.get(1);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        // We are using casts because dynamic parameters inside IN expression can cause type inference failures
        // See https://issues.apache.org/jira/browse/IGNITE-18924
        checkQuery("SELECT id FROM t where test_key IN (?::<type>, ?::<type>)")
                .withParams(value1, value2)
                .returns(1)
                .returns(2)
                .check();
    }

    /** {@code NOT IN} expression with dynamic parameters. */
    @Test
    public void testNotInWithDynamicParamsCondition() {
        T value1 = values.get(0);
        T value2 = values.get(1);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        // We are using casts because inside IN expression cause type inference failures
        // See https://issues.apache.org/jira/browse/IGNITE-18924
        checkQuery("SELECT id FROM t where test_key NOT IN (?::<type>, ?::<type>)")
                .withParams(value1, value2)
                .returns(3)
                .check();
    }

    /** {@code NOT IN} expression. */
    @Test
    public void testNotIn() {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        checkQuery("SELECT id FROM t where test_key NOT IN ($0, $1)")
                .returns(3)
                .check();
    }

    /** {@code IN} operator with dynamic parameters. */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18924")
    public void testInWithDynamicParamsConditionNoCasts() {
        T value1 = values.get(0);
        T value2 = values.get(1);

        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        checkQuery("SELECT id FROM t where test_key IN (?, ?)")
                .withParams(value1, value2)
                .returns(1)
                .returns(2)
                .check();
    }

    /** {@code BETWEEN} operator. */
    @ParameterizedTest
    @MethodSource("between")
    public void testBetweenCondition(TestTypeArguments<T> arguments) {
        Object min = unwrap(orderedValues.first());
        Object mid = unwrap(orderedValues.higher(orderedValues.first()));
        Object max = unwrap(orderedValues.last());

        runSql("INSERT INTO t VALUES(1, ?)", min);
        runSql("INSERT INTO t VALUES(2, ?)", mid);
        runSql("INSERT INTO t VALUES(3, ?)", max);

        checkQuery("SELECT id FROM t WHERE test_key BETWEEN ? AND ? ORDER BY id")
                .withParams(arguments.value(0), arguments.value(1))
                .returns(1)
                .returns(2)
                .check();

        checkQuery("SELECT id FROM t WHERE test_key BETWEEN ? AND ? ORDER BY id")
                .withParams(arguments.value(1), arguments.value(2))
                .returns(2)
                .returns(3)
                .check();
    }

    private Stream<TestTypeArguments<T>> between() {
        T value1 = orderedValues.first();
        T value2 = orderedValues.higher(value1);
        T value3 = orderedValues.last();

        return TestTypeArguments.nary(testTypeSpec, dataSamples, value1, value2, value3);
    }

    /** {@code IS NOT DISTINCT FROM} predicate.*/
    @ParameterizedTest
    @MethodSource("distinctFrom")
    public void testIsNotDistinctFrom(TestTypeArguments<T> arguments) {
        Object value1 = unwrap(values.get(0));
        Object value2 = unwrap(values.get(1));
        Object value3 = unwrap(values.get(2));

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);
        runSql("INSERT INTO t VALUES(3, ?)", value3);

        String query = format("SELECT id FROM t where test_key IS NOT DISTINCT FROM {}", arguments.valueExpr(0));
        checkQuery(query)
                .returns(2)
                .check();
    }

    /**
     * {@code IS NOT DISTINCT FROM} predicate in {@code WHERE clause} with dynamic parameters.
     */
    @ParameterizedTest
    @MethodSource("distinctFrom")
    public void testIsNotDistinctFromWithDynamicParameters(TestTypeArguments<T> arguments) {
        Object value1 = unwrap(values.get(0));
        Object value2 = unwrap(values.get(1));
        Object value3 = unwrap(values.get(2));

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);
        runSql("INSERT INTO t VALUES(3, ?)", value3);

        checkQuery("SELECT id FROM t where test_key IS NOT DISTINCT FROM ?")
                .withParams(arguments.value(0))
                .returns(2)
                .check();
    }

    /** {@code IS DISTINCT FROM} in {@code WHERE clause}. */
    @ParameterizedTest
    @MethodSource("distinctFrom")
    public void testIsDistinctFrom(TestTypeArguments<T> arguments) {
        Object value1 = unwrap(values.get(0));
        Object value2 = unwrap(values.get(1));
        Object value3 = unwrap(values.get(2));

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);
        runSql("INSERT INTO t VALUES(3, ?)", value3);

        String query = format("SELECT id FROM t where test_key IS DISTINCT FROM {}", arguments.valueExpr(0));

        checkQuery(query)
                .returns(1)
                .returns(3)
                .check();
    }

    private Stream<TestTypeArguments<T>> distinctFrom() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, values.get(1));
    }

    /** Ascending ordering.*/
    @Test
    public void testAscOrdering() {
        Object min = unwrap(orderedValues.first());
        Object mid = unwrap(orderedValues.higher(orderedValues.first()));
        Object max = unwrap(orderedValues.last());

        runSql("INSERT INTO t VALUES(1, ?)", min);
        runSql("INSERT INTO t VALUES(2, ?)", mid);
        runSql("INSERT INTO t VALUES(3, ?)", max);

        checkQuery("SELECT id FROM t ORDER BY test_key ASC")
                .returns(1)
                .returns(2)
                .returns(3)
                .check();
    }

    /** Descending ordering. */
    @Test
    public void testDescOrdering() {
        Object min = unwrap(orderedValues.first());
        Object mid = unwrap(orderedValues.higher(orderedValues.first()));
        Object max = unwrap(orderedValues.last());

        runSql("INSERT INTO t VALUES(1, ?)", min);
        runSql("INSERT INTO t VALUES(2, ?)", mid);
        runSql("INSERT INTO t VALUES(3, ?)", max);

        checkQuery("SELECT id FROM t ORDER BY test_key DESC").returns(3).returns(2).returns(1).check();
    }

    /** Predicate in {@code WHERE} clause. */
    @ParameterizedTest
    @MethodSource("filter")
    public void testFilter(TestTypeArguments<T> arguments) {
        String query = format("SELECT id FROM t WHERE t.test_key > {}", arguments.valueExpr(0));

        Object value1 = unwrap(orderedValues.first());
        Object value2 = unwrap(orderedValues.last());

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);

        checkQuery(query)
                .returns(2)
                .check();
    }

    /** Predicate in {@code WHERE} clause with dynamic parameter. */
    @ParameterizedTest
    @MethodSource("filter")
    public void testFilterWithDynamicParameters(TestTypeArguments<T> arguments) {
        String query = format("SELECT id FROM t WHERE t.test_key > ?");

        Object value1 = unwrap(orderedValues.first());
        Object value2 = unwrap(orderedValues.last());

        runSql("INSERT INTO t VALUES(1, ?)", value1);
        runSql("INSERT INTO t VALUES(2, ?)", value2);

        checkQuery(query)
                .withParams(arguments.value(0))
                .returns(2)
                .check();
    }

    private Stream<TestTypeArguments<T>> filter() {
        return TestTypeArguments.unary(testTypeSpec, dataSamples, dataSamples.min());
    }

    protected Object unwrap(@Nullable T value) {
        return testTypeSpec.unwrapIfNecessary(value);
    }
}
