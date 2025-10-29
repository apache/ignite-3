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
import org.apache.ignite.internal.util.HashCalculator;
import org.apache.ignite.internal.util.IgniteUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test to check SQL statistics. */
public class ItStatisticTest extends BaseSqlIntegrationTest {
    private SqlStatisticManagerImpl sqlStatisticManager;
    private static final int PARTITIONS = 3;

    @BeforeAll
    void beforeAll() {
        sqlStatisticManager = (SqlStatisticManagerImpl) queryProcessor().sqlStatisticManager();

        sql(format("CREATE ZONE zone_with_repl (replicas 2, partitions {}) storage profiles ['"
                + DEFAULT_STORAGE_PROFILE + "']", PARTITIONS));
        sql("CREATE TABLE t(ID BIGINT PRIMARY KEY, VAL INTEGER) ZONE zone_with_repl");
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

        long update = insert(0, milestone1);

        sql(selectQuery);

        AtomicInteger inc = new AtomicInteger();

        long timeout = calcStatisticUpdateTimeout();
        // max 10 times cache pollution
        long pollInterval = timeout / 10;

        Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                .timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() ->
                        assertQuery(format("select {} from t", inc.incrementAndGet()))
                                .matches(nodeRowCount("TableScan", is((int) update)))
                                .check()
        );
    }

    @Test
    public void testTableSizeUpdatesForcibly() {
        long milestone = computeNextMilestone(0, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        long updates1 = insert(0L, milestone);

        long timeout = calcStatisticUpdateTimeout();

        // max 10 times cache pollution
        long pollInterval = timeout / 10;

        Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                .timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                    sqlStatisticManager.forceUpdateAll();
                    sqlStatisticManager.lastUpdateStatisticFuture().join();

                    assertQuery("select 1 from t")
                            .matches(nodeRowCount("TableScan", is((int) updates1)))
                            .check();
                }
        );

        milestone = computeNextMilestone(milestone, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        long updates2 = insert(updates1, milestone);

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().join();

        // query not cached in plans
        Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                .timeout(timeout, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                    sqlStatisticManager.forceUpdateAll();
                    sqlStatisticManager.lastUpdateStatisticFuture().join();

                    assertQuery("select 1 from t")
                            .matches(nodeRowCount("TableScan", is((int) updates2)))
                            .check();
                }
        );
    }

    @Test
    public void statisticUpdatesChangeQueryPlans() throws Exception {
        try {
            sqlScript(""
                    + "CREATE TABLE j1(ID INTEGER PRIMARY KEY, VAL INTEGER) ZONE zone_with_repl;"
                    + "CREATE TABLE j2(ID INTEGER PRIMARY KEY, VAL INTEGER) ZONE zone_with_repl;"
            );

            sql("INSERT INTO j1 SELECT x, x FROM system_range(?, ?)", 0, 10);

            sqlStatisticManager.forceUpdateAll();
            sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);

            String query = "SELECT /*+ DISABLE_RULE('HashJoinConverter', 'MergeJoinConverter', 'CorrelatedNestedLoopJoin') */ "
                    + "j1.* FROM j2, j1 WHERE j2.id = j1.id";

            long statRefresh = calcStatisticUpdateTimeout();

            // max 10 times cache pollution
            long pollInterval = statRefresh / 10;

            Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                    .timeout(statRefresh, TimeUnit.MILLISECONDS).untilAsserted(() ->
                            assertQuery(query)
                                    // expecting right source has less rows than left
                                    .matches(QueryChecker.matches(".*TableScan.*PUBLIC.J1.*TableScan.*PUBLIC.J2.*"))
                                    .returnNothing()
                                    .check()
            );

            sql("INSERT INTO j2 SELECT x, x FROM system_range(?, ?)", 0, 3 * DEFAULT_MIN_STALE_ROWS_COUNT);

            sqlStatisticManager.forceUpdateAll();
            sqlStatisticManager.lastUpdateStatisticFuture().get(5, TimeUnit.SECONDS);

            Awaitility.await().pollInterval(Duration.ofMillis(pollInterval))
                    .timeout(statRefresh, TimeUnit.MILLISECONDS).untilAsserted(() ->
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

    private static long calcStatisticUpdateTimeout() {
        long inc = TimeUnit.SECONDS.toMillis(2);
        // need to wait at least 2 statistic updates.
        long statisticAggregationTimeout = INITIAL_DELAY + 2 * REFRESH_PERIOD;

        return PLAN_UPDATER_INITIAL_DELAY > statisticAggregationTimeout
                ? PLAN_UPDATER_INITIAL_DELAY + PLAN_UPDATER_REFRESH_PERIOD
                // need to wait at least one planner cache re-calculation step with a bit timeout for it.
                : statisticAggregationTimeout + PLAN_UPDATER_REFRESH_PERIOD + inc;
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

    /**
     * Calculates number of rows need to be inserted with guarantee that {@code insertsPerPartition} will be reached for every partition.
     * Inclusively 'from', exclusively 'to' bounds.
     */
    private static long insert(long from, long insertsPerPartition) {
        long numberOfInsertions = 0;

        long[] partitionUpdates = new long[PARTITIONS];

        HashCalculator calc = new HashCalculator();

        for (long i = from; i < Integer.MAX_VALUE; ++i) {
            //PartitionCalculator calc = new PartitionCalculator(3, new NativeType[]{NativeTypes.INT64});
            //calc.append(i);
            calc.appendLong(i);
            int partition = IgniteUtils.safeAbs(calc.hash()) % PARTITIONS;
            partitionUpdates[partition] += 1;
            calc.reset();
            numberOfInsertions = i;
            boolean filled = true;
            for (int pos = 0; pos < PARTITIONS; ++pos) {
                if (partitionUpdates[pos] < insertsPerPartition) {
                    filled = false;
                    break;
                }
            }

            if (filled) {
                break;
            }
        }

        sql("INSERT INTO t SELECT x, x FROM system_range(?, ?)", from, numberOfInsertions);

        return numberOfInsertions + 1;
    }
}
