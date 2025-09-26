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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScanIgnoreBounds;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsTableScan;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
    }

    @BeforeEach
    void resetTableState() {
        sqlScript("DELETE FROM test;"
                + "INSERT INTO test VALUES (1, '-Infinity'::FLOAT, '-Infinity'::DOUBLE);"
                + "INSERT INTO test VALUES (2, 'Infinity'::FLOAT, 'Infinity'::DOUBLE);"
                + "INSERT INTO test VALUES (3, 'NaN'::FLOAT, 'NaN'::DOUBLE);"
                + "INSERT INTO test VALUES (4, -0.0::FLOAT, -0.0::DOUBLE);"
                + "INSERT INTO test VALUES (5, 0.0::FLOAT, 0.0::DOUBLE);"
                + "INSERT INTO test VALUES (6, -1.0::FLOAT, -1.0::DOUBLE);"
                + "INSERT INTO test VALUES (7, 1.0::FLOAT, 1.0::DOUBLE);"
        );
    }

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void testArithmetic() {
        assertQuery("SELECT -0.0::FLOAT").returns(-0.0f).check();
        assertQuery("SELECT 0.0::FLOAT").returns(0.0f).check();

        assertQuery("SELECT '-Infinity'::FLOAT - 1").returns(Float.NEGATIVE_INFINITY).check();
        assertQuery("SELECT '-Infinity'::FLOAT + 1").returns(Float.NEGATIVE_INFINITY).check();

        assertQuery("SELECT '+Infinity'::FLOAT - 1").returns(Float.POSITIVE_INFINITY).check();
        assertQuery("SELECT '+Infinity'::FLOAT + 1").returns(Float.POSITIVE_INFINITY).check();

        assertQuery("SELECT 'NaN'::FLOAT - 1").returns(Float.NaN).check();
        assertQuery("SELECT '-NaN'::FLOAT + 1").returns(Float.NaN).check();
        assertQuery("SELECT '-Infinity'::FLOAT + '+Infinity'::FLOAT").returns(Float.NaN).check();
        assertQuery("SELECT '-Infinity'::FLOAT / '+Infinity'::FLOAT").returns(Float.NaN).check();

        assertQuery("SELECT '-Infinity'::DOUBLE - 1").returns(Double.NEGATIVE_INFINITY).check();
        assertQuery("SELECT '-Infinity'::DOUBLE + 1").returns(Double.NEGATIVE_INFINITY).check();

        assertQuery("SELECT '+Infinity'::DOUBLE - 1").returns(Double.POSITIVE_INFINITY).check();
        assertQuery("SELECT '+Infinity'::DOUBLE + 1").returns(Double.POSITIVE_INFINITY).check();

        assertQuery("SELECT 'NaN'::DOUBLE - 1").returns(Double.NaN).check();
        assertQuery("SELECT '-NaN'::DOUBLE + 1").returns(Double.NaN).check();
        assertQuery("SELECT '-Infinity'::DOUBLE + '+Infinity'::DOUBLE").returns(Double.NaN).check();
        assertQuery("SELECT '-Infinity'::DOUBLE / '+Infinity'::DOUBLE").returns(Double.NaN).check();
    }

    @Test
    void testLiterals() {
        assertQuery("SELECT -0.0::FLOAT").returns(-0.0f).check();
        assertQuery("SELECT 0.0::FLOAT").returns(0.0f).check();
        assertQuery("SELECT '-Infinity'::FLOAT").returns(Float.NEGATIVE_INFINITY).check();
        assertQuery("SELECT '+Infinity'::FLOAT").returns(Float.POSITIVE_INFINITY).check();
        assertQuery("SELECT 'NaN'::FLOAT").returns(Float.NaN).check();
        assertQuery("SELECT -'NaN'::FLOAT").returns(Float.NaN).check();

        assertQuery("SELECT -0.0::DOUBLE").returns(-0.0d).check();
        assertQuery("SELECT 0.0::DOUBLE").returns(0.0d).check();
        assertQuery("SELECT '-Infinity'::DOUBLE").returns(Double.NEGATIVE_INFINITY).check();
        assertQuery("SELECT '+Infinity'::DOUBLE").returns(Double.POSITIVE_INFINITY).check();
        assertQuery("SELECT 'NaN'::DOUBLE").returns(Double.NaN).check();
        assertQuery("SELECT -'NaN'::DOUBLE").returns(Double.NaN).check();
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
        assertQuery("SELECT ?").withParam(f).returns(f).check();
    }

    @ParameterizedTest
    @ValueSource(doubles = {
            -0.0d,
            0.0d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.NaN
    })
    void testParameters(double d) {
        assertQuery("SELECT ?").withParam(d).returns(d).check();
    }

    @Test
    void testOrderBy() {
        { // Table scan + Sort
            assertQuery("SELECT /*+ NO_INDEX */ f FROM test ORDER BY f")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .ordered()
                    .returns(Float.NEGATIVE_INFINITY)
                    .returns(-1.0f)
                    .returns(-0.0f)
                    .returns(0.0f)
                    .returns(1.0f)
                    .returns(Float.POSITIVE_INFINITY)
                    .returns(Float.NaN)
                    .check();

            assertQuery("SELECT /*+ NO_INDEX */ d FROM test ORDER BY d")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .ordered()
                    .returns(Double.NEGATIVE_INFINITY)
                    .returns(-1.0d)
                    .returns(-0.0d)
                    .returns(0.0d)
                    .returns(1.0d)
                    .returns(Double.POSITIVE_INFINITY)
                    .returns(Double.NaN)
                    .check();
        }

        { // Index scan
            assertQuery("SELECT /*+ FORCE_INDEX */ f FROM test ORDER BY f")
                    .matches(containsIndexScanIgnoreBounds("PUBLIC", "TEST", "TEST_F_IDX"))
                    .ordered()
                    .returns(Float.NEGATIVE_INFINITY)
                    .returns(-1.0f)
                    .returns(-0.0f)
                    .returns(0.0f)
                    .returns(1.0f)
                    .returns(Float.POSITIVE_INFINITY)
                    .returns(Float.NaN)
                    .check();

            assertQuery("SELECT /*+ FORCE_INDEX */ d FROM test ORDER BY d")
                    .matches(containsIndexScanIgnoreBounds("PUBLIC", "TEST", "TEST_D_IDX"))
                    .ordered()
                    .returns(Double.NEGATIVE_INFINITY)
                    .returns(-1.0d)
                    .returns(-0.0d)
                    .returns(0.0d)
                    .returns(1.0d)
                    .returns(Double.POSITIVE_INFINITY)
                    .returns(Double.NaN)
                    .check();
        }
    }

    @Test
    void testComparisons() {
        { // Equality condition.
            assertQuery("SELECT f FROM test WHERE f = '-Infinity'::FLOAT").returns(Float.NEGATIVE_INFINITY).check();
            assertQuery("SELECT f FROM test WHERE f = '+Infinity'::FLOAT").returns(Float.POSITIVE_INFINITY).check();
            assertQuery("SELECT f FROM test WHERE f = 'NaN'::FLOAT").returnNothing().check(); // NaN never equals

            assertQuery("SELECT d FROM test WHERE d = '-Infinity'::DOUBLE").returns(Double.NEGATIVE_INFINITY).check();
            assertQuery("SELECT d FROM test WHERE d = '+Infinity'::DOUBLE").returns(Double.POSITIVE_INFINITY).check();
            assertQuery("SELECT d FROM test WHERE d = 'NaN'::DOUBLE").returnNothing().check(); // NaN never equals
        }

        { // Inequality conditions with index scan.
            // Greater-than
            assertQuery("SELECT /*+ FORCE_INDEX(test_f_idx) */ f FROM test WHERE f > ?")
                    .withParam(Float.NEGATIVE_INFINITY)
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_F_IDX"))
                    .returns(-0.0f).returns(0.0f)
                    .returns(-1.0f).returns(1.0f)
                    .returns(Float.POSITIVE_INFINITY)
                    .check();
            assertQuery("SELECT /*+ FORCE_INDEX(test_d_idx) */ d FROM test WHERE d > '-Infinity'::DOUBLE")
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_D_IDX"))
                    .returns(-0.0d).returns(0.0d)
                    .returns(-1.0d).returns(1.0d)
                    .returns(Double.POSITIVE_INFINITY)
                    .check();

            assertQuery("SELECT /*+ FORCE_INDEX(test_f_idx) */ f FROM test WHERE f > ?")
                    .withParam(Float.NaN)
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_F_IDX"))
                    .returnNothing()
                    .check();
            assertQuery("SELECT /*+ FORCE_INDEX(test_d_idx) */ d FROM test WHERE d > 'NaN'::DOUBLE")
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_D_IDX"))
                    .returnNothing()
                    .check();

            // Lesser-than
            assertQuery("SELECT /*+ FORCE_INDEX(test_f_idx) */ f FROM test WHERE f < ?")
                    .withParam(+0.0f)
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_F_IDX"))
                    .returns(-1.0f)
                    .returns(Float.NEGATIVE_INFINITY)
                    .check();
            assertQuery("SELECT /*+ FORCE_INDEX(test_d_idx) */ d FROM test WHERE d < +0.0::DOUBLE")
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_D_IDX"))
                    .returns(-1.0d)
                    .returns(Double.NEGATIVE_INFINITY)
                    .check();

            assertQuery("SELECT /*+ FORCE_INDEX(test_f_idx) */ f FROM test WHERE f < ?")
                    .withParam(Float.NaN)
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_F_IDX"))
                    .returnNothing()
                    .check();
            assertQuery("SELECT /*+ FORCE_INDEX(test_d_idx) */ d FROM test WHERE d < '-NaN'::DOUBLE")
                    .matches(containsIndexScan("PUBLIC", "TEST", "TEST_D_IDX"))
                    .returnNothing()
                    .check();
        }

        { // Inequality conditions with table scan + sort.
            // Greater-than
            assertQuery("SELECT /*+ NO_INDEX */ f FROM test WHERE f > -0.0::FLOAT")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returns(1.0f)
                    .returns(Float.POSITIVE_INFINITY)
                    .check();
            assertQuery("SELECT /*+ NO_INDEX */ d FROM test WHERE d > -0.0::DOUBLE")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returns(1.0d)
                    .returns(Double.POSITIVE_INFINITY)
                    .check();

            assertQuery("SELECT /*+ NO_INDEX */ f FROM test WHERE f > ?")
                    .withParam(Float.NaN)
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returnNothing()
                    .check();
            assertQuery("SELECT /*+ NO_INDEX */ d FROM test WHERE d > 'NaN'::DOUBLE")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returnNothing()
                    .check();

            // Lesser-than
            assertQuery("SELECT /*+ NO_INDEX */ f FROM test WHERE f < +0.0::FLOAT")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returns(-1.0f)
                    .returns(Float.NEGATIVE_INFINITY)
                    .check();
            assertQuery("SELECT /*+ NO_INDEX */ d FROM test WHERE d < +0.0::DOUBLE")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returns(-1.0d)
                    .returns(Double.NEGATIVE_INFINITY)
                    .check();

            assertQuery("SELECT /*+ NO_INDEX */ f FROM test WHERE f < ?")
                    .withParam(Float.NaN)
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returnNothing()
                    .check();
            assertQuery("SELECT /*+ NO_INDEX */ d FROM test WHERE d < 'NaN'::DOUBLE")
                    .matches(containsTableScan("PUBLIC", "TEST"))
                    .returnNothing()
                    .check();
        }

        { // IS DISTINCT FROM clause
            assertQuery("SELECT f FROM test WHERE f IS DISTINCT FROM '+Infinity'::FLOAT AND f IS DISTINCT FROM '-Infinity'::FLOAT")
                    .returns(0.0f).returns(-0.0f)
                    .returns(1.0f).returns(-1.0f)
                    .returns(Float.NaN)
                    .check();
            assertQuery("SELECT d FROM test WHERE d IS DISTINCT FROM '+Infinity'::DOUBLE AND d IS DISTINCT FROM '-Infinity'::DOUBLE")
                    .returns(0.0d).returns(-0.0d)
                    .returns(1.0d).returns(-1.0d)
                    .returns(Double.NaN)
                    .check();

            assertQuery("SELECT f FROM test WHERE f IS DISTINCT FROM '-0.0'::FLOAT")
                    .returns(1.0f).returns(-1.0f)
                    .returns(Float.NEGATIVE_INFINITY).returns(Float.POSITIVE_INFINITY)
                    .returns(Float.NaN)
                    .check();
            assertQuery("SELECT d FROM test WHERE d IS DISTINCT FROM '-0.0'::DOUBLE")
                    .returns(1.0d).returns(-1.0d)
                    .returns(Double.NEGATIVE_INFINITY).returns(Double.POSITIVE_INFINITY)
                    .returns(Double.NaN)
                    .check();

            assertQuery("SELECT f FROM test WHERE f IS DISTINCT FROM 'NaN'::FLOAT").returnRowCount(7).check(); // NaN not equal to NaN
            assertQuery("SELECT d FROM test WHERE d IS DISTINCT FROM 'NaN'::DOUBLE").returnRowCount(7).check(); // NaN not equal to NaN
        }
    }

    @Test
    void testAggregations() {
        assertQuery("SELECT MIN(f), MIN(d) FROM test").returns(Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY).check();
        assertQuery("SELECT MAX(f), MAX(d) FROM test").returns(Float.NaN, Double.NaN).check();
        assertQuery("SELECT AVG(f), AVG(d) FROM test").returns(Double.NaN, Double.NaN).check();
    }

    @Test
    void testGrouping() {
        // Insert more data.
        sqlScript("INSERT INTO test VALUES (8, '-Infinity'::FLOAT, '-Infinity'::DOUBLE);"
                + "INSERT INTO test VALUES (9, 'Infinity'::FLOAT, 'Infinity'::DOUBLE);"
                + "INSERT INTO test VALUES (10, 'NaN'::FLOAT, 'NaN'::DOUBLE);"
                + "INSERT INTO test VALUES (11, -0.0::FLOAT, -0.0::DOUBLE);"
                + "INSERT INTO test VALUES (12, 0.0::FLOAT, 0.0::DOUBLE);"
                + "INSERT INTO test VALUES (13, -1.0::FLOAT, -1.0::DOUBLE);"
                + "INSERT INTO test VALUES (14, 1.0::FLOAT, 1.0::DOUBLE);"
        );
        assertQuery("SELECT * FROM test").returnRowCount(14).check();

        assertQuery("SELECT f FROM test GROUP BY f")
                .returns(Float.NEGATIVE_INFINITY)
                .returns(Float.POSITIVE_INFINITY)
                .returns(Float.NaN)
                .returns(-1.0f)
                .returns(1.0f)
                .returns(0.0f)
                .returns(-0.0f)
                .check();

        assertQuery("SELECT d FROM test GROUP BY d")
                .returns(Double.NEGATIVE_INFINITY)
                .returns(Double.POSITIVE_INFINITY)
                .returns(Double.NaN)
                .returns(-1.0d)
                .returns(1.0d)
                .returns(0.0d)
                .returns(-0.0d)
                .check();
    }
}
