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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Test implementation without exporters. */
public class TestMetricManager implements MetricManager {
    private final MetricRegistry registry = new MetricRegistry();

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public void configure(MetricConfiguration metricConfiguration, Supplier<UUID> clusterIdSupplier, String nodeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(Map<String, MetricExporter> availableExporters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(Iterable<MetricExporter> exporters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSource(MetricSource src) {
        registry.registerSource(src);
    }

    @Override
    public void unregisterSource(MetricSource src) {
        registry.unregisterSource(src);
    }

    @Override
    public void unregisterSource(String srcName) {
        registry.unregisterSource(srcName);
    }

    @Override
    public MetricSet enable(MetricSource src) {
        return registry.enable(src);
    }

    @Override
    public MetricSet enable(String srcName) {
        return registry.enable(srcName);
    }

    @Override
    public void disable(MetricSource src) {
        registry.disable(src);
    }

    @Override
    public void disable(String srcName) {
        registry.disable(srcName);
    }

    @Override
    public MetricSnapshot metricSnapshot() {
        return registry.snapshot();
    }

    @Override
    public Collection<MetricSource> metricSources() {
        return registry.metricSources();
    }

    @Override
    public Collection<MetricExporter> enabledExporters() {
        return List.of();
    }

    /** Returns the metric for the arguments if it exists. */
    @TestOnly
    public @Nullable Metric metric(String sourceName, String metricName) {
        MetricSnapshot snapshot = metricSnapshot();

        MetricSet metrics = snapshot.metrics().get(sourceName);

        return metrics == null ? null : metrics.get(metricName);
    }
}
