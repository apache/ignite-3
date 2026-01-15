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
import static org.apache.ignite.internal.util.IgniteUtils.readableSize;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
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

        UUID clusterId = clusterIdSupplier().get();
        if (clusterId != null) {
            if (oneLinePerMetricSource) {
                report.append(System.lineSeparator()).append(PADDING);
            } else {
                report.append(", ");
            }
            report.append("clusterId=").append(clusterId);
        }

        int nodeCount = getClusterNodeCount(metricSets);
        if (nodeCount > 0) {
            if (oneLinePerMetricSource) {
                report.append(System.lineSeparator()).append(PADDING);
            } else {
                report.append(", ");
            }
            report.append("topology=").append(nodeCount).append(" nodes");
        }

        appendNetworkInfo(report, metricSets);

        appendCpuInfo(report, metricSets);

        appendHeapInfo(report, metricSets);

        for (MetricSet metricSet : metricSets) {
            boolean hasMetricsWhiteList = hasMetricsWhiteList(metricSet);

            if (hasMetricsWhiteList || metricEnabled(metricSet.name())) {
                if (oneLinePerMetricSource) {
                    report.append(System.lineSeparator()).append(PADDING).append(metricSet.name()).append(' ');
                    appendMetricsOneLine(report, metricSet, hasMetricsWhiteList);
                } else {
                    report.append(", ").append(metricSet.name()).append(' ');
                    appendMetricsOneLine(report, metricSet, hasMetricsWhiteList);
                }
            }
        }

        if (!oneLinePerMetricSource) {
            appendThreadPoolMetrics(report, metricSets);
        }

        log.info(report.toString());
    }

    /**
     * Gets the cluster node count from topology metrics.
     *
     * @param metricSets Collection of metric sets.
     * @return Number of nodes in the cluster or -1 if not available.
     */
    private int getClusterNodeCount(Collection<MetricSet> metricSets) {
        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().equals("topology.cluster")) {
                for (Metric metric : metricSet) {
                    if (metric.name().equals("TotalNodes")) {
                        return Integer.parseInt(metric.getValueAsString());
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Appends network information to the report. Format: Network [addrs=[addr1, addr2, ...], commPort=YYYY].
     *
     * @param report Report string builder.
     * @param metricSets Collection of metric sets.
     */
    private void appendNetworkInfo(StringBuilder report, Collection<MetricSet> metricSets) {
        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().equals("topology.local")) {
                String address = null;
                Integer port = null;

                for (Metric metric : metricSet) {
                    String metricName = metric.name();
                    if (metricName.equals("NetworkAddress")) {
                        address = metric.getValueAsString();
                    } else if (metricName.equals("NetworkPort")) {
                        port = Integer.parseInt(metric.getValueAsString());
                    }
                }

                if (address != null && !address.isEmpty() && port != null && port > 0) {
                    if (oneLinePerMetricSource) {
                        report.append(System.lineSeparator()).append(PADDING);
                    } else {
                        report.append(", ");
                    }
                    report.append("Network [addrs=[").append(address).append(']')
                            .append(", commPort=").append(port)
                            .append(']');
                    break;
                }
            }
        }
    }

    /**
     * Appends CPU information. Format: CPU [CPUs=20, curLoad=38.57%, avgLoad=16.53%, GC=0%].
     *
     * @param report Report string builder.
     * @param metricSets Collection of metric sets.
     */
    private void appendCpuInfo(StringBuilder report, Collection<MetricSet> metricSets) {
        Integer cpuCount = null;
        Double curLoad = null;
        Double avgLoad = null;
        Double gcPercent = null;

        // Get CPU metrics from os metric source.
        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().equals("os")) {
                for (Metric metric : metricSet) {
                    if (metric.name().equals("AvailableProcessors")) {
                        cpuCount = Integer.parseInt(metric.getValueAsString());
                    } else if (metric.name().equals("CpuLoad")) {
                        curLoad = Double.parseDouble(metric.getValueAsString());
                    } else if (metric.name().equals("LoadAverage")) {
                        avgLoad = Double.parseDouble(metric.getValueAsString());
                    }
                }
            } else if (metricSet.name().equals("jvm")) {
                for (Metric metric : metricSet) {
                    if (metric.name().equals("gc.CollectionTimePercent")) {
                        gcPercent = Double.parseDouble(metric.getValueAsString());
                    }
                }
            }
        }

        if (cpuCount == null || cpuCount <= 0) {
            return;
        }

        if (oneLinePerMetricSource) {
            report.append(System.lineSeparator()).append(PADDING);
        } else {
            report.append(", ");
        }

        report.append("CPU [CPUs=").append(cpuCount);

        if (curLoad != null && curLoad >= 0) {
            report.append(", curLoad=").append(String.format("%.2f%%", curLoad * 100));
        }

        if (avgLoad != null && avgLoad >= 0) {
            report.append(", loadAvg=").append(String.format("%.2f", avgLoad));
        }

        if (gcPercent != null) {
            report.append(", GC=").append(String.format("%.0f%%", gcPercent));
        }

        report.append(']');
    }

    /**
     * Appends Heap memory information. Format: Heap [used=6950MB, free=43.44%, comm=12288MB].
     *
     * @param report Report string builder.
     * @param metricSets Collection of metric sets.
     */
    private void appendHeapInfo(StringBuilder report, Collection<MetricSet> metricSets) {
        Long used = null;
        Long committed = null;
        Double freePercent = null;

        // Get heap memory metrics from jvm metric source.
        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().equals("jvm")) {
                for (Metric metric : metricSet) {
                    if (metric.name().equals("memory.heap.Used")) {
                        used = Long.parseLong(metric.getValueAsString());
                    } else if (metric.name().equals("memory.heap.Committed")) {
                        committed = Long.parseLong(metric.getValueAsString());
                    } else if (metric.name().equals("memory.heap.FreePercent")) {
                        freePercent = Double.parseDouble(metric.getValueAsString());
                    }
                }
                break;
            }
        }

        if (used == null || committed == null || freePercent == null) {
            return;
        }

        if (oneLinePerMetricSource) {
            report.append(System.lineSeparator()).append(PADDING);
        } else {
            report.append(", ");
        }

        report.append("Heap [used=").append(readableSize(used, false))
                .append(", free=").append(String.format("%.2f%%", freePercent))
                .append(", comm=").append(readableSize(committed, false))
                .append(']');
    }

    /**
     * Appends metrics in one-line format.
     *
     * @param sb String builder.
     * @param metricSet Metric set.
     * @param hasMetricsWhiteList Whether metrics whitelist is present.
     */
    private void appendMetricsOneLine(StringBuilder sb, MetricSet metricSet, boolean hasMetricsWhiteList) {
        List<Metric> metrics = StreamSupport.stream(metricSet.spliterator(), false)
                .sorted(comparing(Metric::name))
                .filter(m -> !hasMetricsWhiteList || metricEnabled(fqn(metricSet, m)))
                .collect(toList());

        sb.append('[');
        for (int i = 0; i < metrics.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Metric m = metrics.get(i);
            sb.append(m.name()).append('=').append(m.getValueAsString());
        }
        sb.append(']');
    }

    /**
     * Appends thread pool metrics to the report.
     *
     * @param report Report string builder.
     * @param metricSets Collection of metric sets.
     */
    private void appendThreadPoolMetrics(StringBuilder report, Collection<MetricSet> metricSets) {
        // Find thread pool metrics (extensible for other pools in the future).
        boolean hasThreadPools = false;

        for (MetricSet metricSet : metricSets) {
            if (metricSet.name().startsWith("thread.pools.")) {
                if (report.length() > 0 && hasThreadPools) {
                    report.append(", ");
                }
                if (!hasThreadPools) {
                    report.append("threadPools=[");
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
