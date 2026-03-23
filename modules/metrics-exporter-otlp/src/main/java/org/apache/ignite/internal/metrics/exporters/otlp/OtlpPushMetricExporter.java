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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.OtlpExporterView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Otlp(OpenTelemetry) metrics exporter.
 */
@AutoService(MetricExporter.class)
public class OtlpPushMetricExporter extends PushMetricExporter {
    public static final String EXPORTER_NAME = "otlp";

    private final AtomicReference<MetricReporter> reporter = new AtomicReference<>();

    @Override
    public void stop() {
        super.stop();

        changeReporter(null);
    }

    @Override
    public void reconfigure(ExporterView newVal) {
        var newReporter = new MetricReporter((OtlpExporterView) newVal, clusterIdSupplier(), nodeName());

        for (MetricSet metricSet : snapshot().metrics().values()) {
            newReporter.addMetricSet(metricSet);
        }

        changeReporter(newReporter);

        super.reconfigure(newVal);
    }

    @Override
    public void addMetricSet(MetricSet metricSet) {
        MetricReporter reporter0 = reporter.get();

        assert reporter0 != null;

        reporter0.addMetricSet(metricSet);
    }

    @Override
    public void removeMetricSet(MetricSet metricSet) {
        MetricReporter reporter0 = reporter.get();

        assert reporter0 != null;

        reporter0.removeMetricSet(metricSet.name());
    }

    @Override
    protected long period(ExporterView exporterView) {
        return ((OtlpExporterView) exporterView).periodMillis();
    }

    @Override
    public void report() {
        MetricReporter reporter0 = reporter.get();

        assert reporter0 != null;

        reporter0.report();
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }

    @TestOnly
    @Nullable MetricReporter reporter() {
        return reporter.get();
    }

    private void changeReporter(@Nullable MetricReporter newReporter) {
        MetricReporter oldReporter = reporter.getAndSet(newReporter);

        try {
            IgniteUtils.closeAll(oldReporter);
        } catch (Exception e) {
            log.error("Failed to stop metric exporter: " + name(), e);
        }
    }
}
