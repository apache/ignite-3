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

import java.util.stream.Stream;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.metrics.MetricSource;
import org.junit.jupiter.api.Test;

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

        // And there's only one disable metric source on all nodes
        assertThat(metricSources(0).filter(source -> !source.enabled())).hasSize(1);
        assertThat(metricSources(1).filter(source -> !source.enabled())).hasSize(1);
        assertThat(metricSources(2).filter(source -> !source.enabled())).hasSize(1);

        // When enable cluster metric with valid url
        execute("cluster", "metric", "source", "enable", "jvm", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs("Metric source was enabled successfully" + NL),
                this::assertErrOutputIsEmpty
        );

        // And there's no disable metric sources on all nodes
        assertThat(metricSources(0).filter(source -> !source.enabled())).isEmpty();
        assertThat(metricSources(1).filter(source -> !source.enabled())).isEmpty();
        assertThat(metricSources(2).filter(source -> !source.enabled())).isEmpty();
    }

    private static Stream<MetricSource> metricSources(int nodeIndex) {
        return unwrapIgniteImpl(CLUSTER.node(nodeIndex)).metricManager().metricSources().stream();
    }

    @Test
    void metricEnableNonexistent() {
        // When enable non-existing cluster metric with valid url
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
        // When disable non-existing cluster metric with valid url
        execute("cluster", "metric", "source", "disable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputIs("Metrics source with given name doesn't exist: no.such.metric" + NL),
                this::assertOutputIsEmpty
        );
    }
}
