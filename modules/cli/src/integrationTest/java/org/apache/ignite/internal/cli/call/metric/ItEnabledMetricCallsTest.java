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

package org.apache.ignite.internal.cli.call.metric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSetListCall;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceEnableCall;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceEnableCallInput;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceListCall;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.rest.client.model.Metric;
import org.apache.ignite.rest.client.model.MetricSet;
import org.apache.ignite.rest.client.model.MetricSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for metrics calls with enabled "jvm" metrics source. */
class ItEnabledMetricCallsTest extends CliIntegrationTest {
    private final UrlCallInput urlInput = new UrlCallInput(NODE_URL);

    @Inject
    NodeMetricSourceEnableCall nodeMetricSourceEnableCall;

    @Inject
    NodeMetricSourceListCall nodeMetricSourceListCall;

    @Inject
    NodeMetricSetListCall nodeMetricSetListCall;

    @BeforeAll
    void beforeAll() {
        var inputEnable = NodeMetricSourceEnableCallInput.builder()
                .endpointUrl(NODE_URL)
                .srcName("jvm")
                .enable(true)
                .build();

        nodeMetricSourceEnableCall.execute(inputEnable);
    }

    @Test
    @DisplayName("Should display enabled jvm node metric source when cluster is up and running")
    void nodeMetricSourcesList() {
        // When
        CallOutput<List<MetricSource>> output = nodeMetricSourceListCall.execute(urlInput);

        // Then
        assertThat(output.hasError()).isFalse();

        MetricSource[] expectedMetricSources = {
                new MetricSource().name("jvm").enabled(true),
                new MetricSource().name("client.handler").enabled(false),
                new MetricSource().name("sql.client").enabled(false),
                new MetricSource().name("sql.plan.cache").enabled(false)
        };

        // And
        assertThat(output.body()).contains(expectedMetricSources);
    }

    @Test
    @DisplayName("Should display node metric sets list when cluster is up and running")
    void nodeMetricSetsListEnabled() {
        // When
        CallOutput<List<MetricSet>> output = nodeMetricSetListCall.execute(urlInput);

        // Then
        assertThat(output.hasError()).isFalse();

        // And
        Metric[] expectedMetrics = {
                new Metric().name("memory.heap.Init").desc("Initial amount of heap memory"),
                new Metric().name("memory.heap.Used").desc("Current used amount of heap memory"),
                new Metric().name("memory.heap.Committed").desc("Committed amount of heap memory"),
                new Metric().name("memory.heap.Max").desc("Maximum amount of heap memory"),
                new Metric().name("memory.non-heap.Init").desc("Initial amount of non-heap memory"),
                new Metric().name("memory.non-heap.Used").desc("Used amount of non-heap memory"),
                new Metric().name("memory.non-heap.Committed").desc("Committed amount of non-heap memory"),
                new Metric().name("memory.non-heap.Max").desc("Maximum amount of non-heap memory")
        };

        assertAll(
                () -> assertThat(output.body()).hasSize(1),
                () -> assertThat(output.body().get(0).getName()).isEqualTo("jvm"),
                () -> assertThat(output.body().get(0).getMetrics()).contains(expectedMetrics)
        );
    }
}
