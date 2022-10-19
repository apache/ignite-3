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

package org.apache.ignite.internal.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.TestPullMetricsExporterConfigurationSchema;
import org.apache.ignite.internal.metrics.exporters.TestPushMetricsExporterConfigurationSchema;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterView;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({ConfigurationExtension.class})
public class JmxExporterTest {

    @InjectConfiguration(
            value = "mock.exporters = {"
                    + "jmx = {exporterName = jmx}"
                    + "}",
            polymorphicExtensions = {
                    TestPushMetricsExporterConfigurationSchema.class,
                    TestPullMetricsExporterConfigurationSchema.class
            }
    )
    private MetricConfiguration metricConfiguration;

    @Test
    public void testStart()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {
        var exporter = new JmxExporter();

        var metrics = new HashMap<String, MetricSet>();
        metrics.put("testSource", new MetricSet("testSource", Map.of("testMetric", new IntGauge("testMetric", "", () -> 1))));
        var metricsProvider = mock(MetricProvider.class);
        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(metrics, 1L));

        exporter.start(metricsProvider, (JmxExporterView) metricConfiguration.exporters().get("jmx").value());

        DynamicMBean mBean = getMxBean("metrics", "testSource");

        assertEquals(1, mBean.getAttribute("testMetric"));
    }

    @Test
    public void testAddMetric()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {
        var exporter = new JmxExporter();

        var metricsProvider = mock(MetricProvider.class);
        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(new HashMap<>(), 1L));

        exporter.start(metricsProvider, (JmxExporterView) metricConfiguration.exporters().get("jmx").value());

        assertThrows(InstanceNotFoundException.class, () -> ManagementFactory.getPlatformMBeanServer().getMBeanInfo(IgniteUtils.makeMBeanName("metrics", "testSource")),
            "Expected that mbean won't find, but it was");

        exporter.addMetricSet(new MetricSet("testSource", Map.of("testMetric", new IntGauge("testMetric", "", () -> 1))));

        DynamicMBean mBean = getMxBean("metrics", "testSource");

        assertEquals(1, mBean.getAttribute("testMetric"));
    }

    @Test
    public void testRemoveMetrics()
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {
        var exporter = new JmxExporter();
        var metrics = new HashMap<String, MetricSet>();
        metrics.put("testSource", new MetricSet("testSource", Map.of("testMetric", new IntGauge("testMetric", "", () -> 1))));
        var metricsProvider = mock(MetricProvider.class);
        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(metrics, 1L));

        exporter.start(metricsProvider, (JmxExporterView) metricConfiguration.exporters().get("jmx").value());

        DynamicMBean mBean = getMxBean("metrics", "testSource");

        assertEquals(1, mBean.getAttribute("testMetric"));

        exporter.removeMetricSet("testSource");

        assertThrows(InstanceNotFoundException.class, () -> ManagementFactory.getPlatformMBeanServer().getMBeanInfo(IgniteUtils.makeMBeanName("metrics", "testSource")),
                "Expected that mbean won't find, but it was");
    }

    public static DynamicMBean getMxBean(String grp, String name) throws MalformedObjectNameException {
        ObjectName mbeanName = IgniteUtils.makeMBeanName(grp, name);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }
}
