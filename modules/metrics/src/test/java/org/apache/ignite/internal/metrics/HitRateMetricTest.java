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
package org.apache.ignite.internal.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HitRateMetricTest {
    @Test
    public void testHitrateMetric() {
        HitRateMetric hitRateMetric = new HitRateMetric("hitRate", null, 100);

        hitRateMetric.increment();
        hitRateMetric.add(2);

        assertEquals(3, hitRateMetric.value());

        doSleep(110);

        hitRateMetric.increment();

        doSleep(100);

        assertEquals(2, hitRateMetric.value());
    }

    private void doSleep(long time) {
        try {
            Thread.sleep(time);
        }
        catch (InterruptedException e) {
            // No-op.
        }
    }
}
