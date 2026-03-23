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

package org.apache.ignite.internal.metastorage.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.MetricRegistry;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** Tests for {@link MetaStorageMetricSource}. */
class MetaStorageMetricSourceTest extends BaseIgniteAbstractTest {
    @Test
    void availablePeersMetricReadsFromSupplier() {
        int[] value = {3};

        MetaStorageMetricSource source = new MetaStorageMetricSource(() -> 0L, () -> value[0], () -> 0);
        MetricSet metricSet = enableSource(source);

        IntMetric availablePeers = metricSet.get("AvailablePeers");
        assertNotNull(availablePeers);
        assertEquals(3, availablePeers.value());

        value[0] = 1;
        assertEquals(1, availablePeers.value());
    }

    @Test
    void availableMetricReadsFromSupplier() {
        int[] value = {0};

        MetaStorageMetricSource source = new MetaStorageMetricSource(() -> 0L, () -> 0, () -> value[0]);
        MetricSet metricSet = enableSource(source);

        IntMetric available = metricSet.get("AvailableMajority");
        assertNotNull(available);
        assertEquals(0, available.value());

        value[0] = 1;
        assertEquals(1, available.value());
    }

    @Test
    void idempotentCacheSizeIsUpdatedViaCallback() {
        MetaStorageMetricSource source = new MetaStorageMetricSource(() -> 0L, () -> 0, () -> 0);
        MetricSet metricSet = enableSource(source);

        IntMetric cacheSize = metricSet.get("IdempotentCacheSize");
        assertNotNull(cacheSize);
        assertEquals(0, cacheSize.value());

        source.onIdempotentCacheSizeChange(42);
        assertEquals(42, cacheSize.value());
    }

    private static MetricSet enableSource(MetaStorageMetricSource source) {
        MetricRegistry registry = new MetricRegistry();
        registry.registerSource(source);
        return registry.enable(source);
    }
}
