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

package org.apache.ignite.internal.network;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source for the {@link DefaultMessagingService}.
 */
class DefaultMessagingServiceMetricSource implements MetricSource {
    /** Metrics map. Only modified in {@code synchronized} context. */
    private final Map<String, Metric> metrics = new HashMap<>();

    /** Enabled flag. Only modified in {@code synchronized} context. */
    private boolean enabled;

    @Override
    public String name() {
        return "network.messaging.default";
    }

    @Override
    public @Nullable String description() {
        return "Metrics for the default messaging service.";
    }

    /** Adds metric to the source. */
    synchronized <T extends Metric> T addMetric(T metric) {
        assert !enabled : "Cannot add metrics when source is enabled";

        metrics.put(metric.name(), metric);

        return metric;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }

        enabled = true;

        return new MetricSet(name(), description(), group(), Map.copyOf(metrics));
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
