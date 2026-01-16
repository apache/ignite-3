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

    /** Padding for individual metric output in multiline mode. */
    private static final String PADDING = "  ^-- ";

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

        var report = new StringBuilder("Metrics for local node:");
        boolean needSeparator = oneLinePerMetricSource;
        for (MetricSet metricSet : metricSets) {
            boolean hasMetricsWhiteList = hasMetricsWhiteList(metricSet);

            if (hasMetricsWhiteList || metricEnabled(metricSet.name())) {
                if (metricSet.name().startsWith("thread.pools.")) {
                    continue;
                }
                addSeparatorIfNeeded(report, needSeparator);
                needSeparator = appendMetricsOneLine(report, metricSet, hasMetricsWhiteList, needSeparator);
            }
        }

        appendThreadPoolMetrics(report, metricSets, needSeparator);
        log.info(report.toString());
    }

    private void addSeparatorIfNeeded(StringBuilder report, boolean needSeparator) {
        if (needSeparator) {
            if (oneLinePerMetricSource) {
                report.append(System.lineSeparator()).append(PADDING);
            } else {
                report.append(", ");
            }
        }
    }

    /**
     * Appends metrics in one-line format.
     *
     * @param sb String builder.
     * @param metricSet Metric set.
     * @param hasMetricsWhiteList Whether metrics whitelist is present.
     * @return True if separator is needed for next item.
     */
    private boolean appendMetricsOneLine(StringBuilder sb, MetricSet metricSet, boolean hasMetricsWhiteList, boolean needSeparator) {
        List<Metric> metrics = StreamSupport.stream(metricSet.spliterator(), false)
                .sorted(comparing(Metric::name))
                .filter(m -> !hasMetricsWhiteList || metricEnabled(fqn(metricSet, m)))
                .collect(toList());

        sb.append(metricSet.name()).append(' ').append('[');
        for (int i = 0; i < metrics.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Metric m = metrics.get(i);
            sb.append(m.name()).append('=').append(m.getValueAsString());
        }
        sb.append(']');
        needSeparator = true;
        return needSeparator;
    }

    /**
     * Appends thread pool metrics to the report.
     *
     * @param report Report string builder.
     * @param metricSets Collection of metric sets.
     * @param needSeparator Whether separator is needed.
     * @return True if separator is needed for next item.
     */
    private boolean appendThreadPoolMetrics(StringBuilder report, Collection<MetricSet> metricSets, boolean needSeparator) {
        // Find thread pool metrics (extensible for other pools in the future).
        boolean hasThreadPools = false;

        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().startsWith("thread.pools.")) {
                if (report.length() > 0 && hasThreadPools) {
                    report.append(", ");
                }
                if (!hasThreadPools) {
                    addSeparatorIfNeeded(report, needSeparator);
                    report.append("threadPools [");
                    hasThreadPools = true;
                }

                String poolName = metricSet.name().substring("thread.pools.".length());
                report.append(poolName).append('(');

                List<Metric> poolMetrics = StreamSupport.stream(metricSet.spliterator(), false)
                        .sorted(comparing(Metric::name))
                        .collect(toList());

                for (int i = 0; i < poolMetrics.size(); i++) {
                    if (i > 0) {
                        report.append(", ");
                    }
                    Metric m = poolMetrics.get(i);
                    report.append(m.name()).append('=').append(m.getValueAsString());
                }

                report.append(')');
            }
        }

        if (hasThreadPools) {
            report.append(']');
        }
        return hasThreadPools || needSeparator;
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
