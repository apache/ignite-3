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
import org.apache.ignite.rest.client.model.MetricSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Tests for node metric commands. */
class ItNodeMetricCommandTest extends CliIntegrationTest {
    private static final String NL = System.lineSeparator();

    @Test
    void metricEnable() {
        // When disable cluster metric with valid url
        execute("node", "metric", "source", "disable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was disabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's only one disable metric source on node 0
        assertThat(metricSources(0).filter(source -> !source.enabled())).hasSize(1);
        // And there's no disabled metric sources on node 1 and 2
        assertThat(metricSources(1).filter(source -> !source.enabled())).isEmpty();
        assertThat(metricSources(2).filter(source -> !source.enabled())).isEmpty();

        // When enable cluster metric with valid url
        execute("cluster", "metric", "source", "enable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was enabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's no disabled metric sources on node 0
        assertThat(metricSources(0).filter(source -> !source.enabled())).isEmpty();
    }

    private static Stream<org.apache.ignite.internal.metrics.MetricSource> metricSources(int nodeIndex) {
        return unwrapIgniteImpl(CLUSTER.node(nodeIndex)).metricManager().metricSources().stream();
    }

    @Test
    void metricList() {
        // When list node metric with valid url
        execute("node", "metric", "source", "list", "--plain", "--url", NODE_URL);

        // Then
        List<Executable> assertions = new ArrayList<>();
        assertions.add(this::assertExitCodeIsZero);
        assertions.add(this::assertErrOutputIsEmpty);
        for (MetricSource source : ALL_METRIC_SOURCES) {
            assertions.add(() -> assertOutputContains(source.getName() + "\tenabled" + NL));
        }
        assertAll(assertions);
    }

    @Test
    void metricEnableNonexistent() {
        // When list node metric with valid url
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
        // When list node metric with valid url
        execute("node", "metric", "source", "disable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("Metrics source with given name doesn't exist: no.such.metric"),
                this::assertOutputIsEmpty
        );
    }
}
