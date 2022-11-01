package org.apache.ignite.internal.metrics.exporters;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;

/**
 * Simple metrics exporter for test purposes.
 * It has a trivial API to receive all available metrics as map: (sourceName -> [(metricName -> metricValue), ...])
 */
public class TestSimpleExporter extends BasicMetricExporter<TestSimpleExporterView> {
    /** Exporter name. */
    static final String EXPORTER_NAME = "simple";

    /**
     * Receives all metrics as map (sourceName -> [(metricName -> metricValue), ...]).
     *
     * @return All available metrics.
     */
    Map<String, Map<String, String>> pull() {
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

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return EXPORTER_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public void addMetricSet(MetricSet metricSet) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override
    public void removeMetricSet(String metricSetName) {
        // No-op
    }
}
