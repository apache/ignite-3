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

package org.apache.ignite.internal.sql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test floating point special values test. */
public class ItFloatingPointTest extends BaseSqlMultiStatementTest {
    @BeforeAll
    void createTestTable() {
        sqlScript("CREATE TABLE test (id INT PRIMARY KEY, f FLOAT, d DOUBLE);"
                + "CREATE INDEX test_f_idx on test (f);"
                + "CREATE INDEX test_d_idx on test (d);"
        );

        sql("INSERT INTO test VALUES (?, ?, ?)", 1, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
        sql("INSERT INTO test VALUES (?, ?, ?)", 2, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);
        sql("INSERT INTO test VALUES (?, ?, ?)", 3, Float.NaN, Double.NaN);
        sql("INSERT INTO test VALUES (?, ?, ?)", 4, -1.0f, -1.0d);
        sql("INSERT INTO test VALUES (?, ?, ?)", 5, 1.0f, 1.0d);
        sql("INSERT INTO test VALUES (?, ?, ?)", 6, -0.0f, -0.0d);
        sql("INSERT INTO test VALUES (?, ?, ?)", 7, +0.0f, +0.0d);
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void testArithmetic() {
        assertEquals(-0.0f, sql("SELECT -0.0::FLOAT").get(0).get(0));
        assertEquals(0.0f, sql("SELECT 0.0::FLOAT").get(0).get(0));

        assertEquals(Float.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::FLOAT - 1").get(0).get(0));
        assertEquals(Float.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::FLOAT + 1").get(0).get(0));

        assertEquals(Float.POSITIVE_INFINITY, sql("SELECT '+Infinity'::FLOAT - 1").get(0).get(0));
        assertEquals(Float.POSITIVE_INFINITY, sql("SELECT '+Infinity'::FLOAT + 1").get(0).get(0));

        assertEquals(Float.NaN, sql("SELECT 'NaN'::FLOAT - 1").get(0).get(0));
        assertEquals(Float.NaN, sql("SELECT '-NaN'::FLOAT + 1").get(0).get(0));
        assertEquals(Float.NaN, sql("SELECT '-Infinity'::FLOAT + '+Infinity'::FLOAT").get(0).get(0));
        assertEquals(Float.NaN, sql("SELECT '-Infinity'::FLOAT / '+Infinity'::FLOAT").get(0).get(0));

        assertEquals(Double.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::DOUBLE - 1").get(0).get(0));
        assertEquals(Double.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::DOUBLE + 1").get(0).get(0));

        assertEquals(Double.POSITIVE_INFINITY, sql("SELECT '+Infinity'::DOUBLE - 1").get(0).get(0));
        assertEquals(Double.POSITIVE_INFINITY, sql("SELECT '+Infinity'::DOUBLE + 1").get(0).get(0));

        assertEquals(Double.NaN, sql("SELECT 'NaN'::DOUBLE - 1").get(0).get(0));
        assertEquals(Double.NaN, sql("SELECT '-NaN'::DOUBLE + 1").get(0).get(0));
        assertEquals(Double.NaN, sql("SELECT '-Infinity'::DOUBLE + '+Infinity'::DOUBLE").get(0).get(0));
        assertEquals(Double.NaN, sql("SELECT '-Infinity'::DOUBLE / '+Infinity'::DOUBLE").get(0).get(0));
    }

    @Test
    void testLiterals() {
        assertEquals(-0.0f, sql("SELECT -0.0::FLOAT").get(0).get(0));
        assertEquals(0.0f, sql("SELECT 0.0::FLOAT").get(0).get(0));
        assertEquals(Float.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::FLOAT").get(0).get(0));
        assertEquals(Float.POSITIVE_INFINITY, sql("SELECT '+Infinity'::FLOAT").get(0).get(0));
        assertEquals(Float.NaN, sql("SELECT 'NaN'::FLOAT").get(0).get(0));
        assertEquals(Float.NaN, sql("SELECT -'NaN'::FLOAT").get(0).get(0));

        assertEquals(-0.0d, sql("SELECT -0.0::DOUBLE").get(0).get(0));
        assertEquals(0.0d, sql("SELECT 0.0::DOUBLE").get(0).get(0));
        assertEquals(Double.NEGATIVE_INFINITY, sql("SELECT '-Infinity'::DOUBLE").get(0).get(0));
        assertEquals(Double.POSITIVE_INFINITY, sql("SELECT '+Infinity'::DOUBLE").get(0).get(0));
        assertEquals(Double.NaN, sql("SELECT 'NaN'::DOUBLE").get(0).get(0));
        assertEquals(Double.NaN, sql("SELECT -'NaN'::DOUBLE").get(0).get(0));
    }

    @ParameterizedTest
    @ValueSource(floats = {
            -0.0f,
            0.0f,
            Float.NEGATIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            Float.NaN
    })
    void testParameters(float f) {
        assertEquals(f, (float) sql("SELECT ?", f).get(0).get(0));
    }

    @ParameterizedTest
    @ValueSource(doubles = {
            -0.0d,
            0.0d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NaN
    })
    void testParameters(double f) {
        assertEquals(f, (double) sql("SELECT ?", f).get(0).get(0));
    }

    @Test
    void testOrderBySpecialValues() {
        {
            List<Float> values = sql("SELECT f FROM test ORDER BY f")
                    .stream().flatMap(List::stream).map(Float.class::cast).collect(Collectors.toList());

            assertThat(values, Matchers.contains(
                    equalTo(Float.NEGATIVE_INFINITY),
                    equalTo(-1.0f),
                    equalTo(-0.0f),
                    equalTo(0.0f),
                    equalTo(1.0f),
                    equalTo(Float.POSITIVE_INFINITY),
                    equalTo(Float.NaN) // NaN first or last depending on DB
            ));
        }
        {
            List<Double> values = sql("SELECT d FROM test ORDER BY d")
                    .stream().flatMap(List::stream).map(Double.class::cast).collect(Collectors.toList());
            assertThat(values, Matchers.contains(
                    equalTo(Double.NEGATIVE_INFINITY),
                    equalTo(-1.0d),
                    equalTo(-0.0d),
                    equalTo(0.0d),
                    equalTo(1.0d),
                    equalTo(Double.POSITIVE_INFINITY),
                    equalTo(Double.NaN) // NaN first or last depending on DB
            ));
        }
    }

    @Test
    void testComparisons() {
        assertEquals(1L, sql("SELECT COUNT(*) FROM test WHERE f = '-Infinity'::FLOAT").get(0).get(0));
        assertEquals(1L, sql("SELECT COUNT(*) FROM test WHERE f = '-Infinity'::DOUBLE").get(0).get(0));

        assertEquals(1L, sql("SELECT COUNT(*) FROM test WHERE f = '+Infinity'::FLOAT").get(0).get(0));
        assertEquals(1L, sql("SELECT COUNT(*) FROM test WHERE f = '+Infinity'::DOUBLE").get(0).get(0));

        assertEquals(0L, sql("SELECT COUNT(*) FROM test WHERE f = 'NaN'::FLOAT").get(0).get(0)); // NaN never equals
        assertEquals(0L, sql("SELECT COUNT(*) FROM test WHERE f = 'NaN'::DOUBLE").get(0).get(0)); // NaN never equals

        assertEquals(2L, sql("SELECT COUNT(*) FROM test WHERE f > 0").get(0).get(0)); // 1.0, +Infinity
        assertEquals(2L, sql("SELECT COUNT(*) FROM test WHERE f < 0").get(0).get(0)); // -Infinity, -1,0

        assertEquals(5L, sql("SELECT COUNT(*) FROM test "
                + "WHERE f IS DISTINCT FROM '+Infinity'::FLOAT AND d IS DISTINCT FROM '-Infinity'::FLOAT").get(0).get(0)); // NaN, 1.0, -1,0
        assertEquals(5L, sql("SELECT COUNT(*) FROM test "
                + "WHERE f IS DISTINCT FROM '+Infinity'::DOUBLE AND d IS DISTINCT FROM '-Infinity'::DOUBLE").get(0).get(0));

        // NaN not equal to NaN
        assertEquals(7L, sql("SELECT COUNT(*) FROM test WHERE f IS DISTINCT FROM 'NaN'::FLOAT").get(0).get(0));
        assertEquals(7L, sql("SELECT COUNT(*) FROM test WHERE d IS DISTINCT FROM 'NaN'::DOUBLE").get(0).get(0));
    }

    @Test
    void testAggregations() {
        List<List<Object>> minRows = sql("SELECT MIN(f), MIN(d) FROM test");
        List<List<Object>> maxRows = sql("SELECT MAX(f), MAX(d) FROM test");
        List<List<Object>> avgRows = sql("SELECT AVG(f), AVG(d) FROM test");

        assertThat(minRows.get(0), contains(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        assertThat(maxRows.get(0), contains(Float.NaN, Double.NaN));
        assertThat(avgRows.get(0), contains(Double.NaN, Double.NaN));
    }
}
