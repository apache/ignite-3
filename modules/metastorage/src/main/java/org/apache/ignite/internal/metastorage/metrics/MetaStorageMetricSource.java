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

import java.util.HashMap;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source which provides MetaStorage-related metrics.
 */
public class MetaStorageMetricSource implements MetricSource {
    private static final String SOURCE_NAME = "metastorage";

    private final MetaStorageMetrics metaStorageMetrics;

    private boolean enabled;

    /**
     * Constructor.
     */
    public MetaStorageMetricSource(MetaStorageMetrics metaStorageMetrics) {
        this.metaStorageMetrics = metaStorageMetrics;
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        var metrics = new HashMap<String, Metric>();

        metrics.put(
                "SafeTimeLag",
                new LongGauge(
                        "SafeTimeLag",
                        "Number of milliseconds the local MetaStorage SafeTime lags behind the local logical clock.",
                        metaStorageMetrics::safeTimeLag
                )
        );

        enabled = true;

        return new MetricSet(SOURCE_NAME, metrics);
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }
}
