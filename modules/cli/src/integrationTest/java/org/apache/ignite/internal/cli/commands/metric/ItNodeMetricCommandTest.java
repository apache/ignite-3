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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.metrics.MetricSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Tests for node metric commands. */
class ItNodeMetricCommandTest extends CliIntegrationTest {
    private static final String NL = System.lineSeparator();

    @Test
    void metricEnable() {
        // When disable node metric with valid url
        execute("node", "metric", "source", "disable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was disabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's only one disabled metric source on node 0
        assertThat(metricSources(0).filter(source -> !source.enabled()))
                .singleElement().extracting(MetricSource::name).isEqualTo("jvm");
        // And there's no disabled metric sources on node 1 and 2
        assertThat(metricSources(1)).allMatch(MetricSource::enabled);
        assertThat(metricSources(2)).allMatch(MetricSource::enabled);

        // When enable cluster metric with valid url
        execute("cluster", "metric", "source", "enable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was enabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's no disabled metric sources on node 0
        assertThat(metricSources(0)).allMatch(MetricSource::enabled);
    }

    private static Stream<MetricSource> metricSources(int nodeIndex) {
        return unwrapIgniteImpl(CLUSTER.node(nodeIndex)).metricManager().metricSources().stream();
    }

    @Test
    void metricList() {
        // When list node metrics with valid url
        execute("node", "metric", "source", "list", "--plain", "--url", NODE_URL);

        // Then
        List<Executable> assertions = new ArrayList<>();
        assertions.add(this::assertExitCodeIsZero);
        assertions.add(this::assertErrOutputIsEmpty);
        for (org.apache.ignite.rest.client.model.MetricSource source : ALL_METRIC_SOURCES) {
            assertions.add(() -> assertOutputContains(source.getName() + "\tenabled" + NL));
        }
        assertAll(assertions);
    }

    @Test
    void metricEnableNonexistent() {
        // When enable nonexistent node metric with valid url
        execute("node", "metric", "source", "enable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("Metrics source with given name doesn't exist: no.such.metric"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    void metricDisableNonexistent() {
        // When disable nonexistent node metric with valid url
        execute("node", "metric", "source", "disable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("Metrics source with given name doesn't exist: no.such.metric"),
                this::assertOutputIsEmpty
        );
    }
}
