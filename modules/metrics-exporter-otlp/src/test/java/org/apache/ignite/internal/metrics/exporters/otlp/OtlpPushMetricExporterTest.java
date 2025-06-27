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

package org.apache.ignite.internal.metrics.exporters.otlp;

import static io.opentelemetry.api.common.AttributeType.STRING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.internal.InternalAttributeKeyImpl;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicDoubleMetric;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
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
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.configuration.MetricChange;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.configuration.OtlpExporterChange;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OtlpPushMetricExporterTest extends BaseIgniteAbstractTest {
    @InjectConfiguration("mock.exporters = {otlp = {exporterName = otlp, periodMillis = 300, endpoint = \"http://localhost:4317\"}}")
    private MetricConfiguration metricConfiguration;

    private static final UUID CLUSTER_ID = UUID.randomUUID();

    private static final String SRC_NAME = "testSource";

    /**
     * Metric set with all available metric types.
     * Note that OTLP does not support {@link org.apache.ignite.internal.metrics.UuidGauge} and
     * {@link org.apache.ignite.internal.metrics.StringGauge} types, so they are not included here.
     **/
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

    private MetricManager metricManager;

    private MetricExporter metricsExporter;

    private OtlpPushMetricExporter exporter;

    @Captor
    private ArgumentCaptor<Collection<MetricData>> metricsCaptor;

    @BeforeEach
    void setUp() {
        metricManager = new MetricManagerImpl();

        metricManager.configure(metricConfiguration, () -> CLUSTER_ID, "nodeName");
        metricManager.registerSource(new TestMetricSource(SRC_NAME, metricSet));

        exporter = new OtlpPushMetricExporter();

        metricsExporter = mock(MetricExporter.class);
        doReturn(CompletableResultCode.ofSuccess()).when(metricsExporter).export(anyCollection());
    }

    @AfterEach
    void tearDown() {
        exporter.stop();
    }

    @Test
    public void testStart() {
        metricManager.enable(metricSet.name());

        metricManager.start(Map.of("otlp", exporter));
        exporter.reporter().exporter(metricsExporter);

        verify(metricsExporter, timeout(500L).times(1)).export(metricsCaptor.capture());

        assertThatExportedMetricsAndMetricValuesAreTheSame(metricsCaptor.getValue());
    }

    @Test
    void testConfigurationUpdate() {
        metricManager.enable(metricSet.name());

        metricManager.start(Map.of("otlp", exporter));

        @Nullable MetricReporter reporter = exporter.reporter();

        assertEquals(reporter, exporter.reporter());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("otlp", exporterChange ->
                        exporterChange.convert(OtlpExporterChange.class)
                                .changePeriodMillis(1000L)
                )
        );

        assertNotEquals(reporter, exporter.reporter());
    }

    /**
     * Check, that all exported has the same values as original metric values.
     */
    private static void assertThatExportedMetricsAndMetricValuesAreTheSame(Collection<MetricData> metrics) {
        for (Metric metric : metricSet) {
            MetricData otlpMetric = metrics.stream().filter(m -> m.getName().equals(metric.name())).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Failed to find metric with name " + metric.name()));

            Resource res = otlpMetric.getResource();

            assertEquals(CLUSTER_ID.toString(), res.getAttribute(InternalAttributeKeyImpl.create("service.name", STRING)));
            assertEquals("nodeName", res.getAttribute(InternalAttributeKeyImpl.create("service.instance.id", STRING)));

            if (metric instanceof IntMetric) {
                assertEquals(((IntMetric) metric).value(), CollectionUtils.first(otlpMetric.getLongGaugeData().getPoints()).getValue());
            } else if (metric instanceof LongMetric) {
                assertEquals(((LongMetric) metric).value(), CollectionUtils.first(otlpMetric.getLongGaugeData().getPoints()).getValue());
            } else if (metric instanceof DoubleMetric) {
                assertEquals(((DoubleMetric) metric).value(),
                        CollectionUtils.first(otlpMetric.getDoubleGaugeData().getPoints()).getValue());
            } else if (metric instanceof DistributionMetric) {
                @Nullable HistogramPointData point = CollectionUtils.first(otlpMetric.getHistogramData().getPoints());

                assertArrayEquals(
                        ((DistributionMetric) metric).bounds(),
                        point.getBoundaries().stream().mapToLong(Double::longValue).toArray()
                );
                assertArrayEquals(
                        ((DistributionMetric) metric).value(),
                        point.getCounts().stream().mapToLong(Long::longValue).toArray()
                );
            }
        }
    }

    private static void mutateConfiguration(MetricConfiguration configuration, Consumer<MetricChange> consumer) {
        CompletableFuture<MetricConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);

        assertThat(future, willCompleteSuccessfully());
    }

    private static class TestMetricSource extends AbstractMetricSource {
        private final MetricSet metricSet;

        TestMetricSource(String name, MetricSet metricSet) {
            super(name);

            this.metricSet = metricSet;
        }

        @Override
        protected Holder createHolder() {
            return () -> metricSet;
        }
    }
}
