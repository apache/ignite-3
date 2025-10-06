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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.INITIAL_DELAY;
import static org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl.REFRESH_PERIOD;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.nodeRowCount;
import static org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory.DEFAULT_STALE_ROWS_FRACTION;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test to check SQL statistics. */
public class ItStatisticTest extends BaseSqlIntegrationTest {
    private SqlStatisticManagerImpl sqlStatisticManager;

    private static final String TWO_REPL_ZONE = "zone1";

    @BeforeAll
    void beforeAll() {
        sqlStatisticManager = (SqlStatisticManagerImpl) queryProcessor().sqlStatisticManager();

        sql(format("CREATE ZONE {} (REPLICAS 2) storage profiles ['default'];", TWO_REPL_ZONE));
        sql(format("CREATE TABLE t(ID INTEGER PRIMARY KEY, VAL INTEGER) ZONE {}", TWO_REPL_ZONE));
        //sql("CREATE TABLE t(ID INTEGER PRIMARY KEY, VAL INTEGER)");
    }

    @AfterAll
    void afterAll() {
        sql("DROP TABLE IF EXISTS t;");
        sql(format("DROP ZONE IF EXISTS {}", TWO_REPL_ZONE));
    }

    @AfterEach
    void tearDown() {
        sql("DELETE FROM t;");
    }

    /*@Override
    protected int initialNodes() { // change  !!!
        return 1;
    }*/

    @Test
    public void testTableSizeUpdates() throws InterruptedException {
        long milestone1 = computeNextMilestone(0, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        String selectQuery = "select * from t";

        insert(0, milestone1 - 1);

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

        String selectQuery = "select * from t";

        sql(selectQuery);

        insert(0, milestone1 - 1);

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().join();

        // query not cached in plans
        assertQuery("select 1 from t")
                .matches(nodeRowCount("TableScan", is((int) milestone1)))
                .check();

        long milestone2 = computeNextMilestone(milestone1, DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        insert(milestone1, milestone1 + milestone2 - 1);

        sqlStatisticManager.forceUpdateAll();
        sqlStatisticManager.lastUpdateStatisticFuture().join();

        assertQuery("select 2 from t")
                .matches(nodeRowCount("TableScan", is((int) (milestone1 + milestone2))))
                .check();
    }

    // copy-paste from private method: PartitionModificationCounter.computeNextMilestone
    // if implementation will changes, it need to be changed too
    private static long computeNextMilestone(
            long currentSize,
            double staleRowsFraction,
            long minStaleRowsCount
    ) {
        return Math.max((long) (currentSize * staleRowsFraction), minStaleRowsCount);
    }

    private static void insert(long from, long to) {
        sql("INSERT INTO t SELECT x, x FROM system_range(?, ?)", from, to);
    }
}
