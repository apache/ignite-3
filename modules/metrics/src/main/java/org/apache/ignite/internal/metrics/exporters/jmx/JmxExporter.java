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

package org.apache.ignite.internal.metrics.exporters.jmx;

import static org.apache.ignite.internal.util.IgniteUtils.makeMbeanName;

import com.google.auto.service.AutoService;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.BasicMetricExporter;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.ExporterView;

/**
 * Exporter for Ignite metrics to JMX API.
 * For each enabled {@link org.apache.ignite.internal.metrics.MetricSource} exporter provides
 * a separate MBean with corresponding attribute per source's metric.
 */
@AutoService(MetricExporter.class)
public class JmxExporter extends BasicMetricExporter {
    /** Exporter name. Must be the same for configuration and exporter itself. */
    public static final String JMX_EXPORTER_NAME = "jmx";

    /** Logger. */
    private final IgniteLogger log;

    /** Current registered MBeans. */
    private final List<ObjectName> mbeans = new CopyOnWriteArrayList<>();

    public JmxExporter() {
        log = Loggers.forClass(JmxExporter.class);
    }

    public JmxExporter(IgniteLogger log) {
        this.log = log;
    }

    @Override
    public void start(
            MetricProvider metricsProvider,
            ExporterView configuration,
            Supplier<UUID> clusterIdSupplier,
            String nodeName
    ) {
        super.start(metricsProvider, configuration, clusterIdSupplier, nodeName);

        for (MetricSet metricSet : metricsProvider.snapshot().metrics().values()) {
            register(metricSet);
        }
    }

    @Override
    public void stop() {
        mbeans.forEach(this::unregBean);

        mbeans.clear();
    }

    @Override
    public String name() {
        return JMX_EXPORTER_NAME;
    }

    @Override
    public void reconfigure(ExporterView newValue) {
    }

    /**
     * {@inheritDoc}
     *
     * <p>Register new MBean for received metric set.
     */
    @Override
    public void addMetricSet(MetricSet metricSet) {
        register(metricSet);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Unregister MBean for removed metric set.
     */
    @Override
    public void removeMetricSet(MetricSet metricSet) {
        unregister(metricSet);
    }

    /**
     * Register new MBean per metric set.
     *
     * @param metricSet Metric set.
     */
    private void register(MetricSet metricSet) {
        try {
            MetricSetMbean metricSetMbean = new MetricSetMbean(metricSet);

            ObjectName mbean = ManagementFactory.getPlatformMBeanServer()
                    .registerMBean(
                            metricSetMbean,
                            makeMbeanName(nodeName(), metricSet.group(), metricSet.name()))
                    .getObjectName();

            mbeans.add(mbean);
        } catch (JMException e) {
            log.error("MBean for metric set can't be created [name={}].", e, metricSet.name());
        }
    }

    /**
     * Unregister MBean for specific metric set.
     *
     * @param metricSet Named metric set.
     */
    private void unregister(MetricSet metricSet) {
        try {
            ObjectName mbeanName = makeMbeanName(nodeName(), metricSet.group(), metricSet.name());

            boolean rmv = mbeans.remove(mbeanName);

            if (rmv) {
                unregBean(mbeanName);
            } else {
                log.warn("Tried to unregister the MBean for non-registered metric set [name={}].", metricSet.name());
            }
        } catch (MalformedObjectNameException e) {
            log.error("MBean for metric set can't be unregistered [name={}].", e, metricSet.name());
        }
    }

    /**
     * Unregister MBean by its name.
     *
     * @param bean MBean name to unregister.
     */
    private void unregBean(ObjectName bean) {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(bean);
        } catch (JMException e) {
            log.error("Failed to unregister MBean [bean={}].", e, bean);
        }
    }
}
