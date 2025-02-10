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

package org.apache.ignite.internal.sql.engine.statistic;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test to check SQL statistics. */
public class ItStatisticTest extends BaseSqlIntegrationTest {
    private SqlStatisticManagerImpl sqlStatisticManager;

    @BeforeAll
    void beforeAll() {
        sqlStatisticManager = (SqlStatisticManagerImpl) queryProcessor().sqlStatisticManager();
        sql("CREATE TABLE t(ID INTEGER PRIMARY KEY, VAL INTEGER)");
    }

    @AfterAll
    void afterAll() {
        sql("DROP TABLE IF EXISTS t");
    }

    @Test
    public void testStatisticsRowCount() throws Exception {
        // For test we should always update statistics.
        long prevValueOfThreshold = sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(0);
        try {
            insertAndUpdateRunQuery(500);
            assertQuery(getUniqueQuery())
                    .matches(nodeRowCount("TableScan", is(500)))
                    .check();

            insertAndUpdateRunQuery(600);
            assertQuery(getUniqueQuery())
                    .matches(nodeRowCount("TableScan", is(1100)))
                    .check();

            sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(Long.MAX_VALUE);
            insertAndUpdateRunQuery(900);

            // Statistics shouldn't be updated despite we inserted new rows.
            assertQuery(getUniqueQuery())
                    .matches(nodeRowCount("TableScan", is(1100)))
                    .check();
        } finally {
            sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(prevValueOfThreshold);
        }
    }

    private static AtomicInteger counter = new AtomicInteger(0);

    private void insertAndUpdateRunQuery(int numberOfRecords) throws ExecutionException, TimeoutException, InterruptedException {
        int start = counter.get();
        int end = counter.addAndGet(numberOfRecords) - 1;
        sql("INSERT INTO t SELECT x, x FROM system_range(?, ?)", start, end);

        // run unique sql to update statistics
        sql(getUniqueQuery());

        // wait to update statistics
        sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);
    }

    private static String getUniqueQuery() {
        return "SELECT " + counter.incrementAndGet() + " FROM t";
    }
}
