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

package org.apache.ignite.internal.cli.commands.metric;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.metrics.MetricSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Tests for cluster metric commands. */
class ItClusterMetricCommandTest extends CliIntegrationTest {
    private static final String NL = System.lineSeparator();

    @Test
    void metricEnable() {
        // When disable cluster metric with valid url
        execute("cluster", "metric", "source", "disable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was disabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's only one disabled metric source on all nodes
        assertThat(metricSources(0).filter(source -> !source.enabled()))
                .singleElement().extracting(MetricSource::name).isEqualTo("jvm");
        assertThat(metricSources(1).filter(source -> !source.enabled()))
                .singleElement().extracting(MetricSource::name).isEqualTo("jvm");
        assertThat(metricSources(2).filter(source -> !source.enabled()))
                .singleElement().extracting(MetricSource::name).isEqualTo("jvm");

        // When enable cluster metric with valid url
        execute("cluster", "metric", "source", "enable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was enabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's no disabled metric sources on any of the nodes
        assertThat(metricSources(0)).allMatch(MetricSource::enabled);
        assertThat(metricSources(1)).allMatch(MetricSource::enabled);
        assertThat(metricSources(2)).allMatch(MetricSource::enabled);
    }

    private static Stream<MetricSource> metricSources(int nodeIndex) {
        return unwrapIgniteImpl(CLUSTER.node(nodeIndex)).metricManager().metricSources().stream();
    }

    @Test
    void metricList() {
        // When list cluster metrics with valid url
        execute("cluster", "metric", "source", "list", "--plain", "--url", NODE_URL);

        List<String> nodeNames = CLUSTER.nodes().stream()
                .map(Ignite::name)
                .sorted()
                .collect(toList());

        var assertions = new ArrayList<Executable>();

        assertions.add(this::assertExitCodeIsZero);
        assertions.add(this::assertErrOutputIsEmpty);
        assertions.add(() -> assertOutputContains("Node\tSource name\tAvailability"));
        // Let's check that the substrings are in the correct order.
        assertions.add(() -> assertOutputContainsSubsequence(nodeNames));

        for (Ignite node : CLUSTER.nodes()) {
            List<String> expectedMetrics = Arrays.stream(getExpectedNodeMetrics(node))
                            .map(org.apache.ignite.rest.client.model.MetricSource::getName)
                            .sorted()
                            .collect(toList());

            assertions.add(() -> assertOutputContainsSubsequence(expectedMetrics));
        }

        assertAll(assertions);
    }

    @Test
    void metricEnableNonexistent() {
        // When enable nonexistent cluster metric with valid url
        execute("cluster", "metric", "source", "enable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputIs("Metrics source with given name doesn't exist: no.such.metric" + NL),
                this::assertOutputIsEmpty
        );
    }

    @Test
    void metricDisableNonexistent() {
        // When disable nonexistent cluster metric with valid url
        execute("cluster", "metric", "source", "disable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputIs("Metrics source with given name doesn't exist: no.such.metric" + NL),
                this::assertOutputIsEmpty
        );
    }
}
