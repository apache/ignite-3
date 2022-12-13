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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;

/**
 * Simple pull exporter, which simulate the pull principe throw primitive wait/notify API
 * instead of the complex TCP/IP etc. endpoints.
 */
@AutoService(MetricExporter.class)
public class TestPullMetricExporter extends BasicMetricExporter<TestPullMetricsExporterView> {
    public static final String EXPORTER_NAME = "testPull";

    private static OutputStream outputStream;

    private static final Object obj = new Object();

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void setOutputStream(OutputStream outputStream) {
        TestPullMetricExporter.outputStream = outputStream;
    }

    /**
     * Simulate metric request.
     */
    public static void requestMetrics() {
        synchronized (obj) {
            obj.notify();
        }
    }

    @Override
    public void start(MetricProvider metricProvider, TestPullMetricsExporterView conf) {
        super.start(metricProvider, conf);

        executorService.execute(() -> {
            while (true) {
                waitForRequest();

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

                try {
                    outputStream.write(report.toString().getBytes(StandardCharsets.UTF_8));

                    outputStream.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void stop() {
        executorService.shutdown();
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

    private void waitForRequest() {
        synchronized (obj) {
            try {
                obj.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
