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

package org.apache.ignite.internal.metrics.exporters.log;

import com.google.auto.service.AutoService;
import java.util.Comparator;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterView;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * Log push metrics exporter.
 */
@AutoService(MetricExporter.class)
public class LogPushExporter extends PushMetricExporter<LogPushExporterView> {
    public static final String EXPORTER_NAME = "logPush";

    private static IgniteLogger LOG = Loggers.forClass(LogPushExporter.class);

    private long period;

    @Override
    public void start(MetricProvider metricsProvider, LogPushExporterView configuration) {
        period = configuration.period();

        super.start(metricsProvider, configuration);
    }

    @Override
    protected long period() {
        return period;
    }

    @Override
    public void report() {
        if (CollectionUtils.nullOrEmpty(metrics().get1().values())) {
            return;
        }

        var report = new StringBuilder("Metric report: \n");

        for (MetricSet metricSet : metrics().get1().values()) {
            report.append(metricSet.name()).append(":\n");

            StreamSupport.stream(metricSet.spliterator(), false).sorted(Comparator.comparing(Metric::name)).forEach(metric ->
                    report.append(metric.name())
                            .append(':')
                            .append(metric.getValueAsString())
                            .append('\n'));
        }

        LOG.info(report.toString());
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
