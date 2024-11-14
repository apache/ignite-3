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

package org.apache.ignite.internal.metrics.exporters.otlp;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A reporter which outputs measurements to a {@link MetricExporter}.
 */
class MetricReporter implements AutoCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MetricReporter.class);

    private final Collection<MetricData> metrics = new CopyOnWriteArrayList<>();
    private final Resource resource;

    private MetricExporter exporter;

    MetricReporter(MetricExporter exporter, String clusterId, String nodeName) {
        this.exporter = exporter;

        this.resource = Resource.builder()
                .put("service.name", clusterId)
                .put("service.instance.id", nodeName)
                .build();
    }

    void addMetricSet(MetricSet metricSet) {
        Collection<MetricData> metrics0 = new ArrayList<>();

        InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(metricSet.name())
                .build();

        for (Metric metric : metricSet) {
            MetricData metricData = toMetricData(resource, scope, metric);

            if (metricData != null) {
                metrics0.add(metricData);
            }
        }

        metrics.addAll(metrics0);
    }

    void removeMetricSet(String metricSetName) {
        metrics.removeIf(metricData -> metricSetName.equals(metricData.getName()));
    }

    void report() {
        exporter.export(metrics);
    }

    @Override
    public void close() throws Exception {
        exporter.close();
    }

    @TestOnly
    void exporter(MetricExporter exporter) {
        this.exporter = exporter;
    }

    private static @Nullable MetricData toMetricData(Resource resource, InstrumentationScopeInfo scope, Metric metric) {
        if (metric instanceof IntMetric) {
            return new IgniteIntMetricData(resource, scope, (IntMetric) metric);
        }

        if (metric instanceof LongMetric) {
            return new IgniteLongMetricData(resource, scope, (LongMetric) metric);
        }

        if (metric instanceof DoubleMetric) {
            return new IgniteDoubleMetricData(resource, scope, (DoubleMetric) metric);
        }

        if (metric instanceof DistributionMetric) {
            return new IgniteDistributionMetricData(resource, scope, (DistributionMetric) metric);
        }

        LOG.debug("Unknown metric class for export " + metric.getClass());

        return null;
    }
}
