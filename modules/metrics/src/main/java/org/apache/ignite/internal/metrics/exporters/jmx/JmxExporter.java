package org.apache.ignite.internal.metrics.exporters.jmx;

import static org.apache.ignite.internal.util.IgniteUtils.makeMBeanName;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.metrics.exporters.BasicMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;

public class JmxExporter extends BasicMetricExporter<JmxExporterView> {

    private static IgniteLogger LOG = Loggers.forClass(JmxExporter.class);

    private final List<ObjectName> mBeans = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void init(MetricProvider metricsProvider, JmxExporterView configuration) {
        super.init(metricsProvider, configuration);
        for(MetricSource metricSource: metricsProvider.metrics()) {

        }
    }

    @Override
    public void stop() {

    }

    @Override
    public String name() {
        return null;
    }

    private void register(MetricSet metricSet) {
        IgniteBiTuple<String, String> n = parse(metricSet.name());

        try {
            MetricSourceMBean mregBean = new MetricSourceMBean(metricSet);

            ObjectName mbean = IgniteUtils.registerMBean(
                    ManagementFactory.getPlatformMBeanServer(),
                    makeMBeanName(n.get1(), n.get2()),
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
     * @param mreg Metric registry.
     */
    private void unregister(MetricSet mreg) {
        IgniteBiTuple<String, String> n = parse(mreg.name());

        try {
            ObjectName mbeanName = makeMBeanName(n.get1(), n.get2());

            boolean rmv = mBeans.remove(mbeanName);

            assert rmv;

            unregBean(mbeanName);
        }
        catch (MalformedObjectNameException e) {
            LOG.error("MBean for metric registry '" + n.get1() + ',' + n.get2() + "' can't be unregistered.", e);
        }
    }

    /**
     * @param bean Bean name to unregister.
     */
    private void unregBean(ObjectName bean) {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(bean);
        }
        catch (JMException e) {
            LOG.error("Failed to unregister MBean: " + bean, e);
        }
    }

    private IgniteBiTuple<String, String> parse(String regName) {
        int firstDot = regName.indexOf('.');

        if (firstDot == -1)
            // TODO: KKK appropriate exception here
            throw  new RuntimeException("Incorrect metric name, can't parse.");

        String grp = regName.substring(0, firstDot);
        String beanName = regName.substring(firstDot + 1);

        return new IgniteBiTuple<>(grp, beanName);
    }

    private ObjectName makeMBeanName(String group, String name) {
        // TODO: KKK implement
        throw new UnsupportedOperationException()

    }
}
