package org.apache.ignite.internal.metrics.exporters.jmx;

import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.BasicMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterView;
import org.apache.ignite.internal.util.IgniteUtils;

public class JmxExporter extends BasicMetricExporter<JmxExporterView> {

    public static final String JMX_EXPORTER_NAME = "jmx";

    private static final String JMX_METRIC_GROUP = "metrics";

    private static IgniteLogger LOG = Loggers.forClass(JmxExporter.class);

    private final List<ObjectName> mBeans = new ArrayList<>();

    @Override
    public synchronized void start(MetricProvider metricsProvider, JmxExporterView configuration) {
        super.start(metricsProvider, configuration);

        for(MetricSet metricSet: metricsProvider.metrics().get1().values()) {
            register(metricSet);
        }
    }

    @Override
    public synchronized void stop() {
        mBeans.forEach(this::unregBean);

        mBeans.clear();
    }

    @Override
    public String name() {
        return JMX_EXPORTER_NAME;
    }

    @Override
    public synchronized void addMetricSet(MetricSet metricSet) {
        register(metricSet);
    }

    @Override
    public synchronized void removeMetricSet(String metricSet) {
        unregister(metricSet);
    }

    private void register(MetricSet metricSet) {
        try {
            MetricSourceMBean mregBean = new MetricSourceMBean(metricSet);

            ObjectName mbean = IgniteUtils.registerMBean(
                    ManagementFactory.getPlatformMBeanServer(),
                    makeMBeanName(JMX_METRIC_GROUP, metricSet.name()),
                    mregBean);

            mBeans.add(mbean);
        }
        catch (JMException e) {
            LOG.error("MBean for metric registry '" + metricSet.name() + "' can't be created.", e);
        }
    }

    /**
     * Unregister JMX bean for specific metric registry.
     *
     * @param metricSetName Metric registry.
     */
    private void unregister(String metricSetName) {
        try {
            ObjectName mbeanName = makeMBeanName(JMX_METRIC_GROUP, metricSetName);

            boolean rmv = mBeans.remove(mbeanName);

            if (rmv) {
                unregBean(mbeanName);
            } else {
                LOG.warn("Tried to unregister the MBean for non-registered metric set " + metricSetName);
            }
        }
        catch (MalformedObjectNameException e) {
            LOG.error("MBean for metric source '" + metricSetName + "' can't be unregistered.", e);
        }
    }

    /**
     * @param bean Bean name to unregister.
     */
    private void unregBean(ObjectName bean) {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(bean);
        } catch (JMException e) {
            LOG.error("Failed to unregister MBean: " + bean, e);
        }
    }
}
