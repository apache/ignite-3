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

import com.google.auto.service.AutoService;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.OtlpExporterView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Otlp(OpenTelemetry) metrics exporter.
 */
@AutoService(MetricExporter.class)
public class OtlpPushMetricExporter extends PushMetricExporter<OtlpExporterView> {
    public static final String EXPORTER_NAME = "otlp";

    private volatile @Nullable MetricReporter reporter;

    @Override
    public synchronized void start(MetricProvider metricsProvider, OtlpExporterView view, Supplier<UUID> clusterIdSupplier,
            String nodeName) {
        MetricReporter reporter0 = new MetricReporter(view, clusterIdSupplier.get().toString(), nodeName);

        for (MetricSet metricSet : metricsProvider.metrics().getKey().values()) {
            reporter0.addMetricSet(metricSet);
        }

        reporter = reporter0;

        super.start(metricsProvider, view, clusterIdSupplier, nodeName);
    }

    @Override
    public synchronized void stop() {
        super.stop();

        try {
            IgniteUtils.closeAll(reporter);
        } catch (Exception e) {
            log.error("Failed to stop metric exporter: " + name(), e);
        }

        reporter = null;
    }

    @Override
    public synchronized void reconfigure(OtlpExporterView newVal) {
        super.reconfigure(newVal);

        reporter = new MetricReporter(newVal, clusterId().toString(), nodeName());
    }

    @Override
    public void addMetricSet(MetricSet metricSet) {
        MetricReporter reporter0 = reporter;

        assert reporter0 != null;

        reporter0.addMetricSet(metricSet);
    }

    @Override
    public void removeMetricSet(String metricSetName) {
        MetricReporter reporter0 = reporter;

        assert reporter0 != null;

        reporter0.removeMetricSet(metricSetName);
    }

    @Override
    protected long period() {
        return configuration().period();
    }

    @Override
    public void report() {
        MetricReporter reporter0 = reporter;

        assert reporter0 != null;

        reporter0.report();
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }

    @TestOnly
    @Nullable MetricReporter reporter() {
        return reporter;
    }
}
