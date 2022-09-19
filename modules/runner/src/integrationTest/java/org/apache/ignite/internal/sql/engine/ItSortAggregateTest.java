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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Sort aggregate integration test.
 */
public class ItSortAggregateTest extends AbstractBasicIntegrationTest {
    public static final int ROWS = 103;

    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        sql("CREATE TABLE test (id INT PRIMARY KEY, grp0 INT, grp1 INT, val0 INT, val1 INT) WITH replicas=2,partitions=10");
        sql("CREATE TABLE test_one_col_idx (pk INT PRIMARY KEY, col0 INT)");

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17304 uncomment this
        // sql("CREATE INDEX test_idx ON test(grp0, grp1)");
        // sql("CREATE INDEX test_one_col_idx_idx ON test_one_col_idx(col0)");

        for (int i = 0; i < ROWS; i++) {
            sql("INSERT INTO test (id, grp0, grp1, val0, val1) VALUES (?, ?, ?, ?, ?)", i, i / 10, i / 100, 1, 2);
            sql("INSERT INTO test_one_col_idx (pk, col0) VALUES (?, ?)", i, i);
        }
    }

    @Test
    public void mapReduceAggregate() {
        var res = sql(
                "SELECT /*+ DISABLE_RULE('HashAggregateConverterRule') */"
                        + "SUM(val0), SUM(val1), grp0 FROM TEST "
                        + "GROUP BY grp0 "
                        + "HAVING SUM(val1) > 10"
        );

        assertEquals(ROWS / 10, res.size());

        res.forEach(r -> {
            long s0 = (Long) r.get(0);
            long s1 = (Long) r.get(1);

            assertEquals(s0 * 2, s1);
        });
    }

    @Test
    public void correctCollationsOnMapReduceSortAgg() {
        var cursors = sql("SELECT PK FROM TEST_ONE_COL_IDX WHERE col0 IN (SELECT col0 FROM TEST_ONE_COL_IDX)");

        assertEquals(ROWS, cursors.size());
    }
}
