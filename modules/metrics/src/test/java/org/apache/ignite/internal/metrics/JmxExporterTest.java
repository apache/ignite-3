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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterView;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link JmxExporter}.
 */
@ExtendWith({ConfigurationExtension.class})
public class JmxExporterTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(value = "mock.exporters = {jmx = {exporterName = jmx}}")
    private MetricConfiguration metricConfiguration;

    private JmxExporterView jmxExporterConf;

    private static final MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

    private static final String SRC_NAME = "testSource";

    private static final String MTRC_NAME = "testMetric";

    /**
     * Metric set with all available metric types.
     */
    private static final MetricSet metricSet =
            new MetricSet(
                    SRC_NAME,
                    Map.of(
                            "intGauge", new IntGauge("intGauge", "", () -> 1),
                            "longGauge", new LongGauge("longGauge", "", () -> 1L),
                            "doubleGauge", new DoubleGauge("doubleGauge", "", () -> 1d),
                            "atomicInt", new AtomicIntMetric("atomicInt", ""),
                            "atomicLong", new AtomicLongMetric("atomicLong", ""),
                            "atomicDouble", new AtomicDoubleMetric("atomicDouble", ""),
                            "longAdder", new LongAdderMetric("longAdder", ""),
                            "doubleAdder", new DoubleAdderMetric("doubleAdder", ""),
                            "distributionMetric", new DistributionMetric("distributionMetric", "", new long[] {0, 1}),
                            "hitRate", new HitRateMetric("hitRate", "", Long.MAX_VALUE)
                    )
            );

    private ObjectName mbeanName;

    private MetricProvider metricsProvider;

    private JmxExporter jmxExporter;

    @BeforeEach
    void setUp() throws MalformedObjectNameException {
        jmxExporterConf = (JmxExporterView) metricConfiguration.exporters().get("jmx").value();

        mbeanName = IgniteUtils.makeMbeanName("metrics", SRC_NAME);

        jmxExporter = new JmxExporter(Loggers.forClass(JmxExporter.class));

        metricsProvider = mock(MetricProvider.class);
    }

    @AfterEach
    void tearDown() throws MBeanRegistrationException {
        try {
            mbeanSrv.unregisterMBean(mbeanName);
        } catch (InstanceNotFoundException e) {
            // No op
        }
    }

    @Test
    public void testStart()
            throws ReflectionException, AttributeNotFoundException, MBeanException {
        Map<String, MetricSet> metrics = Map.of(metricSet.name(), metricSet);

        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(metrics, 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();
    }

    @Test
    public void testAddMetric()
            throws ReflectionException, AttributeNotFoundException, MBeanException {
        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(new HashMap<>(), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf);

        assertThrows(
                InstanceNotFoundException.class,
                this::getMbeanInfo,
                "Expected that mbean won't find, but it was");

        jmxExporter.addMetricSet(metricSet);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();
    }

    @Test
    public void testRemoveMetric()
            throws ReflectionException, AttributeNotFoundException, MBeanException {
        Map<String, MetricSet> metrics = Map.of(metricSet.name(), metricSet);

        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(metrics, 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();

        jmxExporter.removeMetricSet("testSource");

        assertThrows(InstanceNotFoundException.class, this::getMbeanInfo,
                "Expected that mbean won't find, but it was");
    }

    @Test
    public void testMetricUpdate() throws ReflectionException, AttributeNotFoundException, MBeanException {
        var intMetric = new AtomicIntMetric(MTRC_NAME, "");

        MetricSet metricSet = new MetricSet(SRC_NAME, Map.of(MTRC_NAME, intMetric));

        when(metricsProvider.metrics()).thenReturn(new IgniteBiTuple<>(Map.of(metricSet.name(), metricSet), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf);

        assertEquals(0, mbean().getAttribute(MTRC_NAME));

        intMetric.add(1);

        // test, that MBean has no stale metric value.
        assertEquals(1, mbean().getAttribute(MTRC_NAME));
    }

    /**
     * Check, that all MBean attributes has the same values as original metric values.
     */
    private void assertThatMbeanAttributeAndMetricValuesAreTheSame()
            throws ReflectionException, AttributeNotFoundException, MBeanException {
        for (Iterator<Metric> it = metricSet.iterator(); it.hasNext(); ) {
            Metric metric = it.next();

            Object beanAttribute = mbean().getAttribute(metric.name());

            String errorMsg = "Wrong MBean attribute value for the metric with name " + metric.name();

            if (metric instanceof IntMetric) {
                assertEquals(((IntMetric) metric).value(), beanAttribute, errorMsg);
            } else if (metric instanceof LongMetric) {
                assertEquals(((LongMetric) metric).value(), beanAttribute, errorMsg);
            } else if (metric instanceof DoubleMetric) {
                assertEquals(((DoubleMetric) metric).value(), beanAttribute, errorMsg);
            } else if (metric instanceof DistributionMetric) {
                assertArrayEquals(((DistributionMetric) metric).value(), (long[]) beanAttribute, errorMsg);
            }
        }
    }

    private DynamicMBean mbean() {
        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }

    private MBeanInfo getMbeanInfo() throws ReflectionException, InstanceNotFoundException, IntrospectionException {
        return ManagementFactory.getPlatformMBeanServer().getMBeanInfo(mbeanName);
    }
}
