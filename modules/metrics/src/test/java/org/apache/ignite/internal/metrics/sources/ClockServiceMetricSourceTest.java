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

package org.apache.ignite.internal.metrics.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.MetricRegistry;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Test for clock service metric source.
 */
public class ClockServiceMetricSourceTest extends BaseIgniteAbstractTest {
    private static final long MAX_CLOCK_SKEW_MILLIS = 100;
    @Mock
    private ClockWaiter clockWaiter;

    @Test
    public void test() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        HybridClock clock = new HybridClockImpl();
        ClockServiceMetricSource clockServiceMetricSource = new ClockServiceMetricSource();

        ClockServiceImpl clockService = new ClockServiceImpl(
                clock,
                clockWaiter,
                () -> MAX_CLOCK_SKEW_MILLIS,
                clockServiceMetricSource::onMaxClockSkewExceeded
        );
        registry.registerSource(clockServiceMetricSource);

        MetricSet metricSet = registry.enable(clockServiceMetricSource);
        assertNotNull(metricSet);

        DistributionMetric clockSkewExceedingMaxClockSkew = metricSet.get("ClockSkewExceedingMaxClockSkew");
        assertNotNull(clockSkewExceedingMaxClockSkew);

        for (int i = 0; i < clockSkewExceedingMaxClockSkew.value().length; i++) {
            assertEquals(0L, clockSkewExceedingMaxClockSkew.value()[i]);
        }

        // Less than max clock skew, should not upgrade the metric.
        clockService.updateClock(clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS / 2));

        // Should update bucket number 9
        clockService.updateClock(clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 2));

        // Should update bucket number 12
        clockService.updateClock(clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 10));
        clockService.updateClock(clock.current().addPhysicalTime(MAX_CLOCK_SKEW_MILLIS * 10));

        assertEquals(1L, clockSkewExceedingMaxClockSkew.value()[9]);
        assertEquals(2L, clockSkewExceedingMaxClockSkew.value()[12]);

        for (int i = 0; i < clockSkewExceedingMaxClockSkew.value().length; i++) {
            if (i != 9 && i != 12) {
                assertEquals(0L, clockSkewExceedingMaxClockSkew.value()[i]);
            }
        }
    }
}
