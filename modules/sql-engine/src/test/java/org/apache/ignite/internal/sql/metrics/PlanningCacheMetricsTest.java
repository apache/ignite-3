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

package org.apache.ignite.internal.sql.metrics;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.framework.PredefinedSchemaManager;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.sql.ParserService;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Test planning cache metrics.
 */
public class PlanningCacheMetricsTest extends AbstractPlannerTest {

    private final MetricManager metricManager = new MetricManagerImpl();

    @Test
    public void plannerCacheStatisticsTest() throws Exception {
        // Run clean up tasks in the current thread, so no eviction event is delayed.
        CacheFactory cacheFactory = CaffeineCacheFactory.create(Runnable::run);

        IgniteTable table = TestBuilders.table()
                .name("T")
                .addColumn("A", NativeTypes.INT32, false)
                .addColumn("B", NativeTypes.INT32, false)
                .distribution(IgniteDistributions.single())
                .build();

        IgniteSchema schema = createSchema(table);

        PrepareService prepareService = new PrepareServiceImpl(
                "test", 2, cacheFactory, null, 15_000L, 2, metricManager, new PredefinedSchemaManager(schema)
        );

        prepareService.start();

        try {
            checkCachePlanStatistics("SELECT * FROM T", prepareService, 0, 2);
            checkCachePlanStatistics("SELECT * FROM T", prepareService, 1, 2);

            checkCachePlanStatistics("SELECT * FROM T t1, T t2", prepareService, 1, 4);
            checkCachePlanStatistics("SELECT * FROM T t1, T t2", prepareService, 2, 4);
            checkCachePlanStatistics("SELECT * FROM T t1, T t2", prepareService, 3, 4);

            checkCachePlanStatistics("SELECT * FROM T", prepareService, 4, 4);

            checkCachePlanStatistics("SELECT * FROM T t1, T t2, T t3", prepareService, 4, 6);

            // Here, the very first plan has been evicted from cache.
            checkCachePlanStatistics("SELECT * FROM T", prepareService, 4, 8);
            checkCachePlanStatistics("SELECT * FROM T", prepareService, 5, 8);
        } finally {
            prepareService.stop();
        }
    }

    private void checkCachePlanStatistics(String qry, PrepareService prepareService, int hits, int misses) {
        SqlOperationContext ctx = operationContext();

        ParserService parserService = new ParserServiceImpl();
        ParsedResult parsedResult = parserService.parse(qry);

        await(prepareService.prepareAsync(parsedResult, ctx));

        MetricSet metricSet = metricManager.metricSnapshot().get1().get(SqlPlanCacheMetricSource.NAME);

        assertEquals(String.valueOf(hits), metricSet.get("Hits").getValueAsString(), "Hits");
        assertEquals(String.valueOf(misses), metricSet.get("Misses").getValueAsString(), "Misses");
    }
}
