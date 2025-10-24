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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl.PLAN_UPDATER_INITIAL_DELAY;
import static org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl.PLAN_UPDATER_REFRESH_PERIOD;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.INITIAL_DELAY;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.REFRESH_PERIOD;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test to check SQL statistics. */
public class ItStatisticTest extends BaseSqlIntegrationTest {
    private SqlStatisticManagerImpl sqlStatisticManager;

    @BeforeAll
    void beforeAll() {
        sqlStatisticManager = (SqlStatisticManagerImpl) queryProcessor().sqlStatisticManager();

        sql("CREATE ZONE zone_with_repl (replicas 2) storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']");
        sql("CREATE TABLE t(ID INTEGER PRIMARY KEY, VAL INTEGER) ZONE zone_with_repl");
    }

    @AfterAll
    void afterAll() {
        sql("DROP TABLE IF EXISTS t;");
    }

    @AfterEach
    void tearDown() {
        sql("DELETE FROM t;");
    }

    /** Simple case demonstrating that the tables size is being updated during statistic refresh interval. */
    @Test
    public void testTableSizeUpdates() {
        long milestone1 = computeNextMilestone(0, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        String selectQuery = "select * from t";

        insert(0, milestone1);

        sql(selectQuery);

        AtomicInteger inc = new AtomicInteger();

        long timeout = 2 * Math.max(REFRESH_PERIOD, INITIAL_DELAY);
        // max 4 times cache pollution
        long pollInterval = timeout / 4;

        Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                .timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                        assertQuery(format("select {} from t", inc.incrementAndGet()))
                                .matches(nodeRowCount("TableScan", is((int) milestone1)))
                                .check()
        );
    }

    @Test
    public void testTableSizeUpdatesForcibly() {
        long milestone1 = computeNextMilestone(0, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        insert(0, milestone1);

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().join();

        // query not cached in plans
        assertQuery("select 1 from t")
                .matches(nodeRowCount("TableScan", is((int) milestone1)))
                .check();

        long milestone2 = computeNextMilestone(milestone1, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        insert(milestone1, milestone1 + milestone2);

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().join();

        // query not cached in plans
        assertQuery("select 2 from t")
                .matches(nodeRowCount("TableScan", is((int) (milestone1 + milestone2))))
                .check();
    }

    @Test
    public void statisticUpdatesChangeQueryPlans() throws Exception {
        try {
            sqlScript(""
                    + "CREATE TABLE j1(ID INTEGER PRIMARY KEY, VAL INTEGER);"
                    + "CREATE TABLE j2(ID INTEGER PRIMARY KEY, VAL INTEGER);"
            );

            sql("INSERT INTO j1 SELECT x, x FROM system_range(?, ?)", 0, 10);

            sqlStatisticManager.forceUpdateAll();
            sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);

            String query = "SELECT /*+ DISABLE_RULE('HashJoinConverter', 'MergeJoinConverter', 'CorrelatedNestedLoopJoin') */ "
                    + "j1.* FROM j2, j1 WHERE j2.id = j1.id";

            int statRefresh = 2 * Math.max(PLAN_UPDATER_REFRESH_PERIOD, PLAN_UPDATER_INITIAL_DELAY);

            Awaitility.await().timeout(statRefresh, TimeUnit.MILLISECONDS).untilAsserted(() ->
                    assertQuery(query)
                            // expecting right source has less rows than left
                            .matches(QueryChecker.matches(".*TableScan.*PUBLIC.J1.*TableScan.*PUBLIC.J2.*"))
                            .returnNothing()
                            .check()
            );

            sql("INSERT INTO j2 SELECT x, x FROM system_range(?, ?)", 0, 100);

            sqlStatisticManager.forceUpdateAll();
            sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);

            Awaitility.await().timeout(statRefresh, TimeUnit.MILLISECONDS).untilAsserted(() ->
                    assertQuery(query)
                            // expecting right source has less rows than left
                            .matches(QueryChecker.matches(".*TableScan.*PUBLIC.J2.*TableScan.*PUBLIC.J1.*"))
                            .check()
            );
        } finally {
            sqlScript(""
                    + "DROP TABLE IF EXISTS j1;"
                    + "DROP TABLE IF EXISTS j2;");
        }
    }

    // copy-paste from private method: PartitionModificationCounter#computeNextMilestone
    // if implementation will changes, it need to be changed too
    private static long computeNextMilestone(
            long currentSize,
            double staleRowsFraction,
            long minStaleRowsCount
    ) {
        return Math.max((long) (currentSize * staleRowsFraction), minStaleRowsCount);
    }

    /** Inclusively 'from', exclusively 'to' bounds. */
    private static void insert(long from, long to) {
        sql("INSERT INTO t SELECT x, x FROM system_range(?, ?)", from, to - 1);
    }
}
