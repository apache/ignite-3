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

import static org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl.PLAN_UPDATER_INITIAL_DELAY;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test to check SQL statistics. */
public class ItStatisticTest extends BaseSqlIntegrationTest {
    private SqlStatisticManagerImpl sqlStatisticManager;

    private static final AtomicInteger counter = new AtomicInteger(0);

    @BeforeAll
    void beforeAll() {
        sqlStatisticManager = (SqlStatisticManagerImpl) queryProcessor().sqlStatisticManager();

        sqlScript(""
                + "CREATE TABLE t1(ID INTEGER PRIMARY KEY, VAL INTEGER);"
                + "CREATE TABLE t2(ID INTEGER PRIMARY KEY, VAL INTEGER);"
        );
    }

    @AfterAll
    void afterAll() {
        sqlScript(""
                + "DROP TABLE IF EXISTS t1;"
                + "DROP TABLE IF EXISTS t2;");
    }

    @Test
    public void statisticUpdatesChangeQueryPlans() {
        sqlStatisticManager.setThresholdTimeToPostponeUpdateMs(Long.MAX_VALUE);

        sql("INSERT INTO t1 SELECT x, x FROM system_range(?, ?)", 0, 10);

        sqlStatisticManager.forceUpdateAll();

        String query = "SELECT /*+ DISABLE_RULE('HashJoinConverter', 'MergeJoinConverter', 'CorrelatedNestedLoopJoin') */ "
                + "t1.* FROM t2, t1 WHERE t2.id = t1.id";

        assertQuery(query)
                // expecting right source has less rows than left
                .matches(QueryChecker.matches(".*TableScan.*PUBLIC.T1.*TableScan.*PUBLIC.T2.*"))
                .returnNothing()
                .check();

        sql("INSERT INTO t2 SELECT x, x FROM system_range(?, ?)", 0, 100);

        sqlStatisticManager.forceUpdateAll();

        Awaitility.await().timeout(Math.max(10_000, 2 * PLAN_UPDATER_INITIAL_DELAY), TimeUnit.MILLISECONDS).untilAsserted(() ->
                assertQuery(query)
                        // expecting right source has less rows than left
                        .matches(QueryChecker.matches(".*TableScan.*PUBLIC.T2.*TableScan.*PUBLIC.T1.*"))
                        .check()
        );
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

    private void insertAndUpdateRunQuery(int numberOfRecords) throws ExecutionException, TimeoutException, InterruptedException {
        int start = counter.get();
        int end = counter.addAndGet(numberOfRecords) - 1;
        sql("INSERT INTO t1 SELECT x, x FROM system_range(?, ?)", start, end);

        // run unique sql to update statistics
        sql(getUniqueQuery());

        // wait to update statistics
        sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);
    }

    private static String getUniqueQuery() {
        return "SELECT " + counter.incrementAndGet() + " FROM t1";
    }
}
