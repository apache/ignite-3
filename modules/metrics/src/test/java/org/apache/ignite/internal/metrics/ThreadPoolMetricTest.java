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

package org.apache.ignite.internal.metrics;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Metrics for thread pool.
 */
public class ThreadPoolMetricTest {
    @Test
    public void test() throws Exception {
        // Should be one per node.
        MetricRegistry registry = new MetricRegistry();

        // ------------------------------------------------------------------------

        // System component, e.g. thread pool executor
        ThreadPoolExecutor exec = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        try {
            // Metrics source for thread pool
            ThreadPoolMetricSource src = new ThreadPoolMetricSource("example.thread_pool.ExamplePool", exec);

            // Register source after the component created.
            registry.registerSource(src);

            // ------------------------------------------------------------------------

            // Enable metrics by signal (or because configuration)
            MetricSet metricSet = registry.enable(src.name());

            LongMetric completedTaskCount = metricSet.get("CompletedTaskCount");

            assertEquals(0L, completedTaskCount.value());

            exec.submit(() -> {}).get(1, TimeUnit.SECONDS);

            assertTrue(waitForCondition(() -> completedTaskCount.value() > 0, 10, TimeUnit.SECONDS.toMillis(1)));

            assertEquals(1L, completedTaskCount.value());

            // ------------------------------------------------------------------------

            // Disable metrics by signal
            registry.disable(src.name());

            // Component is stopped\destroyed
            registry.unregisterSource(src);
        } finally {
            exec.shutdown();
        }
    }
}
