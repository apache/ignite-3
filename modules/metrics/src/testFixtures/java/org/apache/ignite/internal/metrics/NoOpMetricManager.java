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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;

/**
 * The metric manager does nothing in all operations. It is designed to be used in tests where not all component workflow steps might be
 * fulfilled.
 */
public class NoOpMetricManager implements MetricManager {
    @Override
    public void configure(MetricConfiguration metricConfiguration) {
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        return nullCompletedFuture();
    }

    @Override
    public void start(Map<String, MetricExporter> availableExporters) {
    }

    @Override
    public void start(Iterable<MetricExporter<?>> exporters) {
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        return nullCompletedFuture();
    }

    @Override
    public void registerSource(MetricSource src) {
    }

    @Override
    public void unregisterSource(MetricSource src) {
    }

    @Override
    public void unregisterSource(String srcName) {
    }

    @Override
    public MetricSet enable(MetricSource src) {
        return null;
    }

    @Override
    public MetricSet enable(String srcName) {
        return null;
    }

    @Override
    public void disable(MetricSource src) {
    }

    @Override
    public void disable(String srcName) {
    }

    @Override
    public IgniteBiTuple<Map<String, MetricSet>, Long> metricSnapshot() {
        return new IgniteBiTuple<>(Collections.emptyMap(), 1L);
    }

    @Override
    public Collection<MetricSource> metricSources() {
        return Collections.emptyList();
    }
}
