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
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterView;
import org.apache.ignite.internal.util.CollectionUtils;

/**
 * Log push metrics exporter.
 */
@AutoService(MetricExporter.class)
public class LogPushExporter extends PushMetricExporter {
    public static final String EXPORTER_NAME = "logPush";

    /** Padding for individual metric output. */
    private static final String PADDING = "  ";

    @Override
    protected long period(ExporterView exporterView) {
        return ((LogPushExporterView) exporterView).periodMillis();
    }

    @Override
    public void report() {
        Collection<MetricSet> metricSets = snapshot().metrics().values();

        if (CollectionUtils.nullOrEmpty(metricSets)) {
            return;
        }

        var report = new StringBuilder("Metric report: \n");

        for (MetricSet metricSet : metricSets) {
            report.append(metricSet.name()).append(":\n");

            StreamSupport.stream(metricSet.spliterator(), false).sorted(Comparator.comparing(Metric::name)).forEach(metric ->
                    report.append(PADDING)
                            .append(metric.name())
                            .append(':')
                            .append(metric.getValueAsString())
                            .append('\n'));
        }

        log.info(report.toString());
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }
}
