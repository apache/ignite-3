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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link DistributionMetric}.
 */
public class DistributionMetricTest {
    /**
     * Check that calling constructor with invalid parameters causes errors.
     */
    @Test
    public void creationTest() {
        assertThrows(Throwable.class, () -> new DistributionMetric("test", null, null));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[0]));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[] { 10, 1}));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[] { 1, 10, 10, 100}));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[] { -1, 1, 10, 100}));
    }

    /**
     * Create a distribution metric and fill it with some values.
     *
     * @return Metric.
     */
    private DistributionMetric createAndPrepareMetric() {
        long[] bounds = new long[] {50, 500};

        DistributionMetric distribution = new DistributionMetric("distribution", null, bounds);

        assertEquals(bounds.length + 1, distribution.value().length);

        distribution.add(10);
        distribution.add(51);
        distribution.add(60);
        distribution.add(600);
        distribution.add(600);
        distribution.add(600);

        return distribution;
    }

    /**
     * Check that the values in distribution buckets are correct.
     */
    @Test
    public void testBucketValues() {
        DistributionMetric distribution = createAndPrepareMetric();

        distribution.value();

        assertEquals(1, distribution.value()[0]);
        assertEquals(2, distribution.value()[1]);
        assertEquals(3, distribution.value()[2]);
    }

    /**
     * Test scalar metrics that are returned by {@link DistributionMetric#asScalarMetrics()}.
     */
    @Test
    public void testScalarMetrics() {
        DistributionMetric distribution = createAndPrepareMetric();

        List<Metric> scalarMetrics = distribution.asScalarMetrics();

        assertEquals("distribution_0_50", scalarMetrics.get(0).name());
        assertEquals("distribution_50_500", scalarMetrics.get(1).name());
        assertEquals("distribution_500_inf", scalarMetrics.get(2).name());

        for (int i = 0; i < scalarMetrics.size(); i++) {
            LongMetric lm = (LongMetric) scalarMetrics.get(i);
            assertEquals(i + 1, lm.value());
        }

        distribution.add(1);
        distribution.add(100);
        distribution.add(1000);

        for (int i = 0; i < scalarMetrics.size(); i++) {
            LongMetric lm = (LongMetric) scalarMetrics.get(i);
            assertEquals(i + 2, lm.value());
        }
    }

    /**
     * Test the correctness of {@link DistributionMetric#getValueAsString()}.
     */
    @Test
    public void testGetValueAsString() {
        DistributionMetric distribution = createAndPrepareMetric();

        assertEquals("[distribution_0_50:1, distribution_50_500:2, distribution_500_inf:3]", distribution.getValueAsString());
    }
}
