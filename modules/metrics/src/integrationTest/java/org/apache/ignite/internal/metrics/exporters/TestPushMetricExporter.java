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

package org.apache.ignite.internal.metrics.exporters;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;

/**
 * Test push metrics exporter.
 */
@AutoService(MetricExporter.class)
public class TestPushMetricExporter extends PushMetricExporter<TestPushMetricsExporterView> {
    public static final String EXPORTER_NAME = "testPush";

    private static OutputStream outputStream;

    private long period;

    @Override
    public void start(MetricProvider metricsProvider, TestPushMetricsExporterView configuration) {
        period = configuration.period();

        super.start(metricsProvider, configuration);
    }

    public static void setOutputStream(OutputStream outputStream) {
        TestPushMetricExporter.outputStream = outputStream;
    }

    @Override
    protected long period() {
        return period;
    }

    @Override
    public void report() {
        var report = new StringBuilder();

        for (MetricSet metricSet : metrics().get1().values()) {
            report.append(metricSet.name()).append(":\n");

            for (Metric metric : metricSet) {
                report.append(metric.name())
                        .append(":")
                        .append(metric.getValueAsString())
                        .append("\n");
            }

            report.append("\n");
        }

        write(report.toString());
    }

    private void write(String report) {
        try {
            outputStream.write(report.getBytes(StandardCharsets.UTF_8));

            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }

    @Override
    public void addMetricSet(MetricSet metricSet) {
    }

    @Override
    public void removeMetricSet(String metricSetName) {
    }
}
