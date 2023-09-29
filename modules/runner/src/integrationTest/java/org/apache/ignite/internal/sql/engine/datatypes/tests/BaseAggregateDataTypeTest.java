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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test cases for aggregate functions.
 *
 * @param <T> A storage type for a data type.
 */
public abstract class BaseAggregateDataTypeTest<T extends Comparable<T>> extends BaseDataTypeTest<T> {

    /** {@code COUNT} aggregate function. */
    @Test
    public void testCount() {
        insertValues();

        checkQuery("SELECT COUNT(test_key) FROM t")
                .returns((long) values.size())
                .check();
    }

    /** {@code MIN} aggregate function. */
    @Test
    public void testMin() {
        T min = orderedValues.first();

        insertValues();

        checkQuery("SELECT MIN(test_key) FROM t").returns(min).check();
    }

    /** {@code MAX} aggregate function. */
    @Test
    public void testMax() {
        T max = orderedValues.last();

        insertValues();

        checkQuery("SELECT MAX(test_key) FROM t").returns(max).check();
    }

    /** {@code GROUP BY}.*/
    @Test
    public void testGroupBy() {
        T min = orderedValues.first();
        T mid = orderedValues.higher(min);
        T max = orderedValues.last();

        insertValues();

        checkQuery("SELECT test_key FROM t GROUP BY test_key ORDER BY test_key")
                .returns(min)
                .returns(mid)
                .returns(max)
                .check();
    }

    /** {@code GROUP BY} and {@code HAVING}. */
    @ParameterizedTest
    @MethodSource("having")
    public void testGroupByHaving(TestTypeArguments<T> arguments) {
        runSql("INSERT INTO t VALUES(1, $0)");
        runSql("INSERT INTO t VALUES(4, $0)");
        runSql("INSERT INTO t VALUES(2, $1)");
        runSql("INSERT INTO t VALUES(3, $2)");

        String query = format("SELECT test_key FROM t GROUP BY test_key HAVING COUNT(test_key) = 2");

        checkQuery(query)
                .returns(orderedValues.first())
                .check();
    }

    private Stream<TestTypeArguments<T>> having() {
        T min = orderedValues.first();

        return TestTypeArguments.unary(testTypeSpec, dataSamples, min);
    }

    /** {@code SOME} aggregate function. */
    @ParameterizedTest
    @MethodSource("some")
    public void testSome(TestTypeArguments<T> arguments) {
        T min = orderedValues.first();
        T mid = orderedValues.higher(min);
        T max = orderedValues.last();

        insertValues();

        String query = format(
                "SELECT test_key, SOME(test_key = {}) FROM t GROUP BY test_key ORDER BY test_key",
                arguments.valueExpr(0));

        checkQuery(query)
                .returns(min, false)
                .returns(mid, false)
                .returns(max, true)
                .check();
    }

    private Stream<TestTypeArguments<T>> some() {
        T max = orderedValues.last();

        return TestTypeArguments.unary(testTypeSpec, dataSamples, max);
    }

    /** {@code ANY_VALUE} aggregate function. */
    @Test
    public void testAnyValue() {
        insertValues();

        List<List<Object>> rows = runSql("SELECT ANY_VALUE(test_key) FROM t");
        assertEquals(1, rows.size());

        List<Object> row = rows.get(0);
        T firstValue = testTypeSpec.wrapIfNecessary(row.get(0));
        assertThat(values, hasItem(firstValue));
    }
}
