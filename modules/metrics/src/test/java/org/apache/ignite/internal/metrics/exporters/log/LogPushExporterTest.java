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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.StringGauge;
import org.apache.ignite.internal.metrics.UuidGauge;
import org.apache.ignite.internal.metrics.configuration.MetricChange;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.LogPushExporterChange;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.internal.metrics.sources.JvmMetricSource;
import org.apache.ignite.internal.metrics.sources.OsMetricSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.logging.log4j.core.LogEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link JmxExporter}.
 */
@ExtendWith({ConfigurationExtension.class})
public class LogPushExporterTest extends BaseIgniteAbstractTest {
    @InjectConfiguration("mock.exporters {log {"
            + "exporterName = logPush, "
            + "periodMillis = 300, "
            + "oneLinePerMetricSource = false,"
            + "enabledMetrics = ["
            + "  " + SRC_NAME + ", "
            + "  " + ADDITIONAL_SRC_NAME + ", "
            + "  full, "
            + "  \"partial.Included*\", "
            + "  \"nameWithWildcard*\", "
            + "  \"similar.name.*\", "
            + "  \"ignored\""
            + "]"
            + "}}")
    private MetricConfiguration metricConfiguration;

    private static final UUID CLUSTER_ID = UUID.randomUUID();

    private static final String SRC_NAME = "testSource";

    private static final String ADDITIONAL_SRC_NAME = "additionalSource";

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

    private static final MetricSet fullyIncludedMetricSet =
            new MetricSet(
                    "full",
                    Map.of(
                            "FullMetricInt", new IntGauge("FullMetricInt", "", () -> 42),
                            "FullMetricLong", new LongGauge("FullMetricLong", "", () -> 42L)
                    )
            );

    private static final MetricSet partiallyIncludedMetricSet =
            new MetricSet(
                    "partial",
                    Map.of(
                            "IncludedMetric", new IntGauge("IncludedMetric", "", () -> 42),
                            "NotIncludedMetric", new IntGauge("NotIncludedMetric", "", () -> 1)
                    )
            );

    private static final MetricSet metricSetNameWithWildcard =
            new MetricSet(
                    "nameWithWildcard",
                    Map.of("NameWithWildcardMetric", new IntGauge("NameWithWildcardMetric", "", () -> 42))
            );

    private static final MetricSet metricSetWithSimilarNameOne =
            new MetricSet(
                    "similar.name.one",
                    Map.of(
                            "SimilarNameOneMetricOne", new IntGauge("SimilarNameOneMetricOne", "", () -> 3),
                            "SimilarNameOneMetricTwo", new IntGauge("SimilarNameOneMetricTwo", "", () -> 4)
                    )
            );

    private static final MetricSet metricSetWithSimilarNameTwo =
            new MetricSet(
                    "similar.name.two",
                    Map.of(
                            "SimilarNameTwoMetricOne", new IntGauge("SimilarNameTwoMetricOne", "", () -> 5),
                            "SimilarNameTwoMetricTwo", new IntGauge("SimilarNameTwoMetricTwo", "", () -> 6)
                    )
            );

    private static final MetricSet ignoredMetrics =
            new MetricSet(
                    "ignored.metrics",
                    Map.of("IgnoredMetric", new IntGauge("IgnoredMetric", "", () -> 1))
            );

    private MetricManagerImpl metricManager;

    private LogPushExporter exporter;

    @BeforeEach
    void setUp() {
        metricManager = new MetricManagerImpl("nodeName", () -> CLUSTER_ID, metricConfiguration);

        // Register JVM and OS metric sources for system metrics (heap, CPU).
        metricManager.registerSource(new JvmMetricSource());
        metricManager.registerSource(new OsMetricSource());
        metricManager.enable("jvm");
        metricManager.enable("os");

        metricManager.registerSource(new TestMetricSource(metricSet));
        metricManager.registerSource(new TestMetricSource(fullyIncludedMetricSet));
        metricManager.registerSource(new TestMetricSource(partiallyIncludedMetricSet));
        metricManager.registerSource(new TestMetricSource(metricSetNameWithWildcard));
        metricManager.registerSource(new TestMetricSource(metricSetWithSimilarNameOne));
        metricManager.registerSource(new TestMetricSource(metricSetWithSimilarNameTwo));
        metricManager.registerSource(new TestMetricSource(ignoredMetrics));

        metricManager.enable(metricSet.name());
        metricManager.enable(fullyIncludedMetricSet.name());
        metricManager.enable(partiallyIncludedMetricSet.name());
        metricManager.enable(metricSetNameWithWildcard.name());
        metricManager.enable(metricSetWithSimilarNameOne.name());
        metricManager.enable(metricSetWithSimilarNameTwo.name());
        metricManager.enable(ignoredMetrics.name());

        exporter = new LogPushExporter();
    }

    @AfterEach
    void tearDown() {
        exporter.stop();
    }

    @Test
    void testStart() {
        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node"),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testNewPrefixFormat() {
        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node:"),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testTopologyNodeCountIncluded() {
        var topologyClusterMetricSet = new MetricSet("topology.cluster", Map.of(
                "TotalNodes", new IntGauge("TotalNodes", "", () -> 3),
                "ClusterId", new UuidGauge("ClusterId", "", () -> CLUSTER_ID)
        ));

        metricManager.registerSource(new TestMetricSource(topologyClusterMetricSet));
        metricManager.enable(topologyClusterMetricSet.name());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeEnabledMetrics("testSource", "topology.cluster")
                )
        );
    }

    @Test
    void testThreadPoolMetricsIncluded() {
        var threadPoolMetricSet = new MetricSet("thread.pools.partitions-executor", Map.of(
                "Active", new IntGauge("Active", "", () -> 5),
                "QueueSize", new IntGauge("QueueSize", "", () -> 10),
                "Completed", new LongGauge("Completed", "", () -> 100L)
        ));

        metricManager.registerSource(new TestMetricSource(threadPoolMetricSet));
        metricManager.enable(threadPoolMetricSet.name());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeOneLinePerMetricSource(false)
                                .changeEnabledMetrics("testSource", "thread.pools.*")
                )
        );

        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("thread.pools.partitions-executor ["),
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testThreadPoolMetricsWithMultiplePools() {
        var pool1MetricSet = new MetricSet("thread.pools.partitions-executor", Map.of(
                "Active", new IntGauge("Active", "", () -> 5)
        ));
        var pool2MetricSet = new MetricSet("thread.pools.rebalance-scheduler", Map.of(
                "Active", new IntGauge("Active", "", () -> 2)
        ));

        metricManager.registerSource(new TestMetricSource(pool1MetricSet));
        metricManager.registerSource(new TestMetricSource(pool2MetricSet));
        metricManager.enable(pool1MetricSet.name());
        metricManager.enable(pool2MetricSet.name());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeOneLinePerMetricSource(false)
                                .changeEnabledMetrics("testSource", "thread.pools.*")
                )
        );

        withLogInspector(
                evt -> {
                    String msg = evt.getMessage().getFormattedMessage();
                    return msg.contains("thread.pools.partitions-executor [")
                            && msg.contains("thread.pools.rebalance-scheduler [");
                },
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testSingleLineOutput() {
        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeOneLinePerMetricSource(false)
                )
        );

        withLogInspector(
                evt -> {
                    String msg = evt.getMessage().getFormattedMessage();
                    return msg.contains("Metrics for local node") && !msg.contains(System.lineSeparator());
                },
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testMultilineOutput() {
        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeOneLinePerMetricSource(true)
                )
        );

        withLogInspector(
                evt -> {
                    String msg = evt.getMessage().getFormattedMessage();
                    // Check that metrics are formatted with newlines and padding
                    return msg.contains("Metrics for local node")
                            && msg.contains(System.lineSeparator() + "  ^-- " + SRC_NAME + " ")
                            && msg.contains(System.lineSeparator() + "  ^-- ");
                },
                logInspector -> {
                    metricManager.start(Map.of("logPush", exporter));

                    Awaitility.await()
                            .atMost(Duration.ofMillis(500L))
                            .until(logInspector::isMatched);
                }
        );
    }

    @Test
    void testAllSystemMetricsInOneLog() {
        var metastorageMetricSet = new MetricSet("metastorage", Map.of(
                "TestMetric", new IntGauge("TestMetric", "", () -> 1)
        ));
        var placementDriverMetricSet = new MetricSet("placement-driver", Map.of(
                "TestMetric", new IntGauge("TestMetric", "", () -> 1)
        ));

        metricManager.registerSource(new TestMetricSource(metastorageMetricSet));
        metricManager.registerSource(new TestMetricSource(placementDriverMetricSet));
        metricManager.enable(metastorageMetricSet.name());
        metricManager.enable(placementDriverMetricSet.name());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeEnabledMetrics("testSource", "metastorage", "placement-driver")
                )
        );
    }

    @Test
    void testNetworkInfoFromTopologyLocal() {
        var topologyLocalMetricSet = new MetricSet("topology.local", Map.of(
                "NodeId", new UuidGauge("NodeId", "", () -> UUID.randomUUID()),
                "NodeName", new StringGauge("NodeName", "", () -> "nodeName"),
                "NetworkAddress", new StringGauge("NetworkAddress", "", () -> "10.2.51.131"),
                "NetworkPort", new IntGauge("NetworkPort", "", () -> 3344)
        ));

        metricManager.registerSource(new TestMetricSource(topologyLocalMetricSet));
        metricManager.enable(topologyLocalMetricSet.name());

        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeEnabledMetrics("testSource", "topology.local")
                )
        );
    }

    @Test
    void testMetricUpdate() {
        var intMetric = new AtomicIntMetric(MTRC_NAME, "");
        var additionalMetricSet = new MetricSet(
                ADDITIONAL_SRC_NAME,
                Map.of(intMetric.name(), intMetric)
        );

        metricManager.registerSource(new TestMetricSource(additionalMetricSet));
        metricManager.enable(additionalMetricSet.name());
        metricManager.start(Map.of("logPush", exporter));

        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains(MTRC_NAME + "=1"),
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
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node"),
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
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node"),
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
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node"),
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void enabledMetricsTest(boolean oneLinePerMetricSource) {
        mutateConfiguration(metricConfiguration, change ->
                change.changeExporters().update("log", exporterChange ->
                        exporterChange.convert(LogPushExporterChange.class)
                                .changeOneLinePerMetricSource(oneLinePerMetricSource)
                )
        );

        metricManager.start(Map.of("logPush", exporter));

        var fullMetricInt = new AtomicBoolean();
        var fullMetricLong = new AtomicBoolean();
        var partialMetricIncluded = new AtomicBoolean();
        var partialMetricNotIncluded = new AtomicBoolean();
        var nameWithWildcardMetric = new AtomicBoolean();
        var similarSetOneMetricOne = new AtomicBoolean();
        var similarSetOneMetricTwo = new AtomicBoolean();
        var similarSetTwoMetricOne = new AtomicBoolean();
        var similarSetTwoMetricTwo = new AtomicBoolean();
        var ignoredMetric = new AtomicBoolean();

        var fullMetricSingleString = new AtomicBoolean();
        var fullMetricIntOneString = new AtomicBoolean();
        var fullMetricLongOneString = new AtomicBoolean();

        withLogInspector(
                evt -> evt.getMessage().getFormattedMessage().contains("Metrics for local node"),
                logInspector -> {
                    logInspector.addHandler(evt -> evtMatches(evt, "FullMetricInt"), () -> fullMetricInt.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "FullMetricLong"), () -> fullMetricLong.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "IncludedMetric"), () -> partialMetricIncluded.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "NotIncludedMetric"), () -> partialMetricNotIncluded.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "NameWithWildcardMetric"), () -> nameWithWildcardMetric.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "SimilarNameOneMetricOne"), () -> similarSetOneMetricOne.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "SimilarNameOneMetricTwo"), () -> similarSetOneMetricTwo.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "SimilarNameTwoMetricOne"), () -> similarSetTwoMetricOne.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "SimilarNameTwoMetricTwo"), () -> similarSetTwoMetricTwo.set(true));
                    logInspector.addHandler(evt -> evtMatches(evt, "IgnoredMetric"), () -> ignoredMetric.set(true));

                    logInspector.addHandler(
                            evt -> evtMatches(evt, ": full [FullMetricInt=42, FullMetricLong=42]"),
                            () -> fullMetricSingleString.set(true)
                    );

                    logInspector.addHandler(
                            evt -> evtMatches(evt, "  ^-- full [FullMetricInt=42, FullMetricLong=42]"),
                            () -> fullMetricIntOneString.set(true)
                    );
                    logInspector.addHandler(
                            evt -> evtMatches(evt, "  ^-- full [FullMetricInt=42, FullMetricLong=42]"),
                            () -> fullMetricLongOneString.set(true)
                    );

                    Awaitility.await()
                            .atMost(Duration.ofMillis(2000L))
                            .until(() -> logInspector.timesMatched().anyMatch(i -> i == 2));

                    assertTrue(fullMetricInt.get());
                    assertTrue(fullMetricLong.get());
                    assertTrue(partialMetricIncluded.get());
                    assertFalse(partialMetricNotIncluded.get());
                    assertTrue(nameWithWildcardMetric.get());
                    assertTrue(similarSetOneMetricOne.get());
                    assertTrue(similarSetOneMetricTwo.get());
                    assertTrue(similarSetTwoMetricOne.get());
                    assertTrue(similarSetTwoMetricTwo.get());
                    assertFalse(ignoredMetric.get());

                    if (oneLinePerMetricSource) {
                        assertFalse(fullMetricSingleString.get());
                        assertTrue(fullMetricIntOneString.get());
                        assertTrue(fullMetricLongOneString.get());
                    } else {
                        assertTrue(fullMetricSingleString.get());
                        assertFalse(fullMetricIntOneString.get());
                        assertFalse(fullMetricLongOneString.get());
                    }
                }
        );
    }

    private static boolean evtMatches(LogEvent evt, String s) {
        return evt.getMessage().getFormattedMessage().contains(s);
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
