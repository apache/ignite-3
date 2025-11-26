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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.IgniteUtils.findAny;
import static org.apache.ignite.internal.util.IgniteUtils.forEachIndexed;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    private volatile boolean oneLinePerMetricSource;

    private volatile List<String> enabledMetrics;

    @Override
    protected long period(ExporterView exporterView) {
        return ((LogPushExporterView) exporterView).periodMillis();
    }

    @Override
    public void reconfigure(ExporterView view) {
        super.reconfigure(view);

        LogPushExporterView v = (LogPushExporterView) view;
        oneLinePerMetricSource = v.oneLinePerMetricSource();
        enabledMetrics = Arrays.asList(v.enabledMetrics());
    }

    @Override
    public void report() {
        Collection<MetricSet> metricSets = snapshot().metrics().values();

        if (CollectionUtils.nullOrEmpty(metricSets) || CollectionUtils.nullOrEmpty(enabledMetrics)) {
            return;
        }

        var report = new StringBuilder("Metric report:");

        for (MetricSet metricSet : metricSets) {
            boolean hasMetricsWhiteList = hasMetricsWhiteList(metricSet);

            if (hasMetricsWhiteList || metricEnabled(metricSet.name())) {
                report.append('\n').append(metricSet.name()).append(oneLinePerMetricSource ? ' ' : ':');

                appendMetrics(report, metricSet, hasMetricsWhiteList);
            }
        }

        log.info(report.toString());
    }

    private void appendMetrics(StringBuilder sb, MetricSet metricSet, boolean hasMetricsWhiteList) {
        List<Metric> metrics = StreamSupport.stream(metricSet.spliterator(), false)
                .sorted(comparing(Metric::name))
                .filter(m -> !hasMetricsWhiteList || metricEnabled(fqn(metricSet, m)))
                .collect(toList());

        sb.append(metricSetPrefix());

        forEachIndexed(metrics, (m, i) -> appendMetricWithValue(oneLinePerMetricSource, sb, m, i));

        sb.append(metricSetPostfix());
    }

    private static String commaInEnum(int i) {
        return i == 0 ? "" : ", ";
    }

    private String metricSetPrefix() {
        return oneLinePerMetricSource ? "[" : "";
    }

    private String metricSetPostfix() {
        return oneLinePerMetricSource ? "]" : "";
    }

    private static void appendMetricWithValue(boolean oneLinePerMetricSource, StringBuilder sb, Metric m, int index) {
        if (oneLinePerMetricSource) {
            sb.append(commaInEnum(index)).append(m.name()).append('=').append(m.getValueAsString());
        } else {
            sb.append('\n').append(PADDING).append(m.name()).append(": ").append(m.getValueAsString());
        }
    }

    private boolean metricEnabled(String name) {
        return findAny(enabledMetrics, em -> nameMatches(name, em)).isPresent();
    }

    private static String fqn(MetricSet ms, Metric m) {
        return ms.name() + '.' + m.name();
    }

    private boolean hasMetricsWhiteList(MetricSet ms) {
        return findAny(
                enabledMetrics,
                em -> (nameMatches(ms.name(), em) || em.startsWith(ms.name()))
                        && em.length() != ms.name().length()
        ).isPresent();
    }

    private static boolean nameMatches(String name, String template) {
        return template.endsWith("*") && name.startsWith(template.substring(0, template.length() - 1))
                || template.equals(name);
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }
}
