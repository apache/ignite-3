package org.apache.ignite.internal.metrics.exporters;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;

public class TestSimpleExporter extends BasicMetricExporter<TestSimpleExporterView> {
    static final String EXPORTER_NAME = "simple";

    public Map<String, Map<String, String>> pull() {
        Map<String, Map<String, String>> results = new HashMap<>();

        for (MetricSet metricSet: metrics().get1().values()) {
            Map<String, String> metricSetMetrics = new HashMap<>();

            for (Metric metric: metricSet) {
                metricSetMetrics.put(metric.name(), metric.getValueAsString());
            }

            results.put(metricSet.name(), metricSetMetrics);
        }

        return results;
    }


    @Override
    public void stop() {
        // No-op
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
    }

    @Override
    public void addMetricSet(MetricSet metricSet) {
        // No-op
    }

    @Override
    public void removeMetricSet(String metricSetName) {
        // No-op
    }
}
