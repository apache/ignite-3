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

package org.apache.ignite.internal.table.distributed.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.MetricRegistry;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link SchemaSyncMetricSource}. */
class SchemaSyncMetricSourceTest extends BaseIgniteAbstractTest {
    private final SchemaSyncMetricSource source = new SchemaSyncMetricSource();
    private final MetricRegistry registry = new MetricRegistry();
    private DistributionMetric waits;

    @BeforeEach
    void setUp() {
        registry.registerSource(source);
        MetricSet metricSet = registry.enable(source);

        waits = metricSet.get("Waits");
        assertNotNull(waits);
    }

    @Test
    void allBucketsAreZeroInitially() {
        for (long count : waits.value()) {
            assertEquals(0L, count);
        }
    }

    @Test
    void recordWaitPopulatesCorrectBucket() {
        source.recordWait(7);   // falls in bucket 2: (5..10]
        source.recordWait(200); // falls in bucket 5: (100..500]
        source.recordWait(3000); // falls in bucket 7: (1000..5000]

        assertEquals(1L, waits.value()[2]);
        assertEquals(1L, waits.value()[5]);
        assertEquals(1L, waits.value()[7]);
    }

    @Test
    void recordWaitAccumulatesCountsInSameBucket() {
        source.recordWait(1); // bucket 0: [0..1]
        source.recordWait(0); // bucket 0: [0..1]
        source.recordWait(1); // bucket 0: [0..1]

        assertEquals(3L, waits.value()[0]);
    }

    @Test
    void recordWaitIsNoOpWhenSourceIsDisabled() {
        SchemaSyncMetricSource disabledSource = new SchemaSyncMetricSource();
        // not registered or enabled - should not throw
        disabledSource.recordWait(100);
    }
}
