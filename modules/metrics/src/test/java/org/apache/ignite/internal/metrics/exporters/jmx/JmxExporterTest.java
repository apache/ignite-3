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

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.metrics.AtomicDoubleMetric;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.CompositeMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.DoubleAdderMetric;
import org.apache.ignite.internal.metrics.DoubleGauge;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.HitRateMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSnapshot;
import org.apache.ignite.internal.metrics.StringGauge;
import org.apache.ignite.internal.metrics.UuidGauge;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.JmxExporterView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
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

    private static final UUID uuid = UUID.randomUUID();

    private static final String nodeName = "nodeName";

    /** Metric set with all available metric types. */
    private static final MetricSet metricSet =
            new MetricSet(
                    SRC_NAME,
                    Stream.of(
                            new IgniteBiTuple<>("intGauge", new IntGauge("intGauge", "", () -> 1)),
                            new IgniteBiTuple<>("longGauge", new LongGauge("longGauge", "", () -> 1L)),
                            new IgniteBiTuple<>("doubleGauge", new DoubleGauge("doubleGauge", "", () -> 1d)),
                            new IgniteBiTuple<>("atomicInt", new AtomicIntMetric("atomicInt", "")),
                            new IgniteBiTuple<>("atomicLong", new AtomicLongMetric("atomicLong", "")),
                            new IgniteBiTuple<>("atomicDouble", new AtomicDoubleMetric("atomicDouble", "")),
                            new IgniteBiTuple<>("longAdder", new LongAdderMetric("longAdder", "")),
                            new IgniteBiTuple<>("doubleAdder", new DoubleAdderMetric("doubleAdder", "")),
                            new IgniteBiTuple<>("distributionMetric", new DistributionMetric("distributionMetric", "", new long[] {0, 1})),
                            new IgniteBiTuple<>("hitRate", new HitRateMetric("hitRate", "", Long.MAX_VALUE)),
                            new IgniteBiTuple<>("customIntMetric", new CustomIntMetric()),
                            new IgniteBiTuple<>("customLongMetric", new CustomLongMetric()),
                            new IgniteBiTuple<>("customDoubleMetric", new CustomDoubleMetric()),
                            new IgniteBiTuple<>("customCompositeMetric", new CustomCompositeMetric()),
                            new IgniteBiTuple<>("stringGauge", new StringGauge("stringGauge", "", () -> "testString")),
                            new IgniteBiTuple<>("uuidGauge", new UuidGauge("uuidGauge", "", () -> uuid))
                    ).collect(toMap(IgniteBiTuple::getKey, IgniteBiTuple::getValue))
            );

    private ObjectName mbeanName;

    private MetricProvider metricsProvider;

    private JmxExporter jmxExporter;

    @BeforeEach
    void setUp() throws MalformedObjectNameException {
        jmxExporterConf = (JmxExporterView) metricConfiguration.exporters().get("jmx").value();

        mbeanName = IgniteUtils.makeMbeanName(nodeName, metricSet.group(), metricSet.name());

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
    public void testStart() throws ReflectionException, AttributeNotFoundException, MBeanException {
        Map<String, MetricSet> metrics = Map.of(metricSet.name(), metricSet);

        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(metrics, 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();
    }

    @Test
    public void testAddMetric() throws ReflectionException, AttributeNotFoundException, MBeanException {
        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(new HashMap<>(), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertThrows(
                InstanceNotFoundException.class,
                this::getMbeanInfo,
                "Expected that mbean won't find, but it was");

        jmxExporter.addMetricSet(metricSet);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();
    }

    @Test
    public void testRemoveMetric() throws ReflectionException, AttributeNotFoundException, MBeanException {
        Map<String, MetricSet> metrics = Map.of(metricSet.name(), metricSet);

        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(metrics, 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertThatMbeanAttributeAndMetricValuesAreTheSame();

        jmxExporter.removeMetricSet(metricSet);

        assertThrows(InstanceNotFoundException.class, this::getMbeanInfo,
                "Expected that mbean won't find, but it was");
    }

    @Test
    public void testMetricUpdate() throws ReflectionException, AttributeNotFoundException, MBeanException {
        var intMetric = new AtomicIntMetric(MTRC_NAME, "");

        MetricSet metricSet = new MetricSet(SRC_NAME, Map.of(MTRC_NAME, intMetric));

        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(Map.of(metricSet.name(), metricSet), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertEquals(0, mbean().getAttribute(MTRC_NAME));

        intMetric.add(1);

        // test, that MBean has no stale metric value.
        assertEquals(1, mbean().getAttribute(MTRC_NAME));
    }

    @Test
    public void testCustomMetrics() throws Exception {
        var intMetric = new CustomIntMetric();
        var longMetric = new CustomLongMetric();
        var doubleMetric = new CustomDoubleMetric();

        String intMetricName = "customInt";
        String longMetricName = "customLong";
        String doubleMetricName = "customDouble";

        MetricSet metricSet = new MetricSet(SRC_NAME, Map.of(
                intMetricName, intMetric,
                longMetricName, longMetric,
                doubleMetricName, doubleMetric
        ));

        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(Map.of(metricSet.name(), metricSet), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertEquals(42, mbean().getAttribute(intMetricName));
        assertEquals(42L, mbean().getAttribute(longMetricName));
        assertEquals(42.0, mbean().getAttribute(doubleMetricName));
    }

    @Test
    public void testCustomCompositeMetric() throws Exception {
        var metric = new CustomCompositeMetric();

        MetricSet metricSet = new MetricSet(SRC_NAME, Map.of(MTRC_NAME, metric));

        when(metricsProvider.snapshot()).thenReturn(new MetricSnapshot(Map.of(metricSet.name(), metricSet), 1L));

        jmxExporter.start(metricsProvider, jmxExporterConf, UUID::randomUUID, nodeName);

        assertEquals(metric.getValueAsString(), mbean().getAttribute(MTRC_NAME));
    }

    /**
     * Check, that all MBean attributes has the same values as original metric values.
     */
    private void assertThatMbeanAttributeAndMetricValuesAreTheSame()
            throws ReflectionException, AttributeNotFoundException, MBeanException {
        for (Metric metric : metricSet) {
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
            } else if (metric instanceof CompositeMetric) {
                assertEquals(metric.getValueAsString(), beanAttribute, errorMsg);
            } else if (metric instanceof StringGauge) {
                assertEquals(((StringGauge) metric).value(), beanAttribute, errorMsg);
            } else if (metric instanceof UuidGauge) {
                assertEquals(((UuidGauge) metric).value(), beanAttribute, errorMsg);
            }
        }
    }

    private DynamicMBean mbean() {
        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }

    private MBeanInfo getMbeanInfo() throws ReflectionException, InstanceNotFoundException, IntrospectionException {
        return ManagementFactory.getPlatformMBeanServer().getMBeanInfo(mbeanName);
    }

    private static class CustomIntMetric implements IntMetric {
        @Override
        public int value() {
            return 42;
        }

        @Override public String name() {
            return "customIntMetric";
        }

        @Override public @Nullable String description() {
            return null;
        }
    }

    private static class CustomLongMetric implements LongMetric {
        @Override
        public long value() {
            return 42;
        }

        @Override
        public String name() {
            return "customLongMetric";
        }

        @Override
        public @Nullable String description() {
            return "";
        }
    }

    private static class CustomDoubleMetric implements DoubleMetric {
        @Override
        public double value() {
            return 42.0;
        }

        @Override
        public String name() {
            return "customDoubleMetric";
        }

        @Override
        public @Nullable String description() {
            return "";
        }
    }

    private static class CustomCompositeMetric implements CompositeMetric {
        static final List<Metric> SCALAR_METRICS = List.of(
                new IntGauge("m1", "m1", () -> 1),
                new IntGauge("m2", "m2", () -> 2)
        );

        @Override
        public List<Metric> asScalarMetrics() {
            return SCALAR_METRICS;
        }

        @Override
        public String name() {
            return "customCompositeMetric";
        }

        @Override
        public @Nullable String description() {
            return "";
        }

        @Override
        public @Nullable String getValueAsString() {
            return SCALAR_METRICS.toString();
        }
    }
}
