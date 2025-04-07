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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicDoubleMetric;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.DoubleAdderMetric;
import org.apache.ignite.internal.metrics.DoubleGauge;
import org.apache.ignite.internal.metrics.HitRateMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.configuration.MetricChange;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterChange;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.logging.log4j.core.LogEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link JmxExporter}.
 */
@ExtendWith({ConfigurationExtension.class})
public class LogPushExporterTest extends BaseIgniteAbstractTest {
    @InjectConfiguration("mock.exporters = {log = {exporterName = logPush, periodMillis = 300}}")
    private MetricConfiguration metricConfiguration;

    private static final UUID CLUSTER_ID = UUID.randomUUID();

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

    private MetricManager metricManager;

    private LogPushExporter exporter;

    @BeforeEach
    void setUp() {
        metricManager = new MetricManagerImpl();
        metricManager.configure(metricConfiguration, () -> CLUSTER_ID, "nodeName");
        metricManager.registerSource(new TestMetricSource(metricSet));
        metricManager.enable(metricSet.name());

        exporter = new LogPushExporter();
    }

    @AfterEach
    void tearDown() {
        exporter.stop();
    }

    @Test
    void testStart() {
        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metric report"),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testMetricUpdate() {
        var intMetric = new AtomicIntMetric(MTRC_NAME, "");
        var additionalMetricSet = new MetricSet(
                "additionalSource",
                Map.of(intMetric.name(), intMetric)
        );

        metricManager.registerSource(new TestMetricSource(additionalMetricSet));
        metricManager.enable(additionalMetricSet.name());
        metricManager.start(Map.of("logPush", exporter));

        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains(MTRC_NAME + ":1"),
                logInspector -> {
                    intMetric.add(1);

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testConfigurationUpdate() {
        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metric report"),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .during(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );

        ScheduledFuture<?> fut = IgniteTestUtils.getFieldValue(exporter, PushMetricExporter.class, "fut");

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changePeriodMillis(1000L)
                )
        );

        Awaitility.await()
                .atMost(Duration.ofMillis(200L))
                .until(fut::isCancelled);

        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metric report"),
                logInspector -> {
                    Awaitility.await()
                            .between(Duration.ofMillis(800L), Duration.ofMillis(1200L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testSkipReconfigureScheduledTask() {
        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metric report"),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .during(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );

        ScheduledFuture<?> fut = IgniteTestUtils.getFieldValue(exporter, PushMetricExporter.class, "fut");

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changePeriodMillis(300L)
                )
        );

        Awaitility.await()
                .during(Duration.ofMillis(200L))
                .until(fut::isCancelled, equalTo(false));
    }

    private static void withLogInspector(Predicate<LogEvent> predicate, Consumer<LogInspector> action) {
        LogInspector logInspector = new LogInspector(LogPushExporter.class.getName(), predicate);

        try {
            logInspector.start();

            assertFalse(logInspector.isMatched());

            action.accept(logInspector);
        } finally {
            logInspector.stop();
        }
    }

    private static void mutateConfiguration(MetricConfiguration configuration, Consumer<MetricChange> consumer) {
        CompletableFuture<MetricConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);

        assertThat(future, willCompleteSuccessfully());
    }

    private static class TestMetricSource extends AbstractMetricSource {
        private final MetricSet metricSet;

        TestMetricSource(MetricSet metricSet) {
            super(metricSet.name());

            this.metricSet = metricSet;
        }

        @Override
        protected Holder createHolder() {
            return () -> metricSet;
        }
    }
}
