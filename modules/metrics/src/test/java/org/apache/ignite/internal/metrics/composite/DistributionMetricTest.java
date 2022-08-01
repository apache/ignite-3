/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.metrics.composite;

import java.util.List;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.scalar.LongMetric;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DistributionMetricTest {
    @Test
    public void test() {
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, null));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[0]));
        assertThrows(AssertionError.class, () -> new DistributionMetric("test", null, new long[] { 10, 1}));

        long[] bounds = new long[] {50, 500};

        DistributionMetric distribution = new DistributionMetric("distribution", null, bounds);

        assertEquals(bounds.length + 1, distribution.value().length);

        distribution.value(10);
        distribution.value(51);
        distribution.value(60);
        distribution.value(600);
        distribution.value(600);
        distribution.value(600);

        distribution.value();

        assertEquals(1, distribution.value()[0]);
        assertEquals(2, distribution.value()[1]);
        assertEquals(3, distribution.value()[2]);

        List<Metric> scalarMetrics = distribution.asScalarMetrics();

        assertEquals("0_50", scalarMetrics.get(0).name());
        assertEquals("50_500", scalarMetrics.get(1).name());
        assertEquals("500_", scalarMetrics.get(2).name());

        for (int i = 0; i < scalarMetrics.size(); i++) {
            LongMetric lm = (LongMetric) scalarMetrics.get(i);
            assertEquals(i + 1, lm.value());
        }

        distribution.value(1);
        distribution.value(100);
        distribution.value(1000);

        for (int i = 0; i < scalarMetrics.size(); i++) {
            LongMetric lm = (LongMetric) scalarMetrics.get(i);
            assertEquals(i + 2, lm.value());
        }

        assertEquals("[0_50:2, 50_500:3, 500_:4]", distribution.getAsString());
    }
}
