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

package org.apache.ignite.internal.rest.metrics;

import static io.micronaut.core.type.Argument.listOf;
import static io.micronaut.http.HttpRequest.GET;
import static io.micronaut.http.HttpRequest.POST;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static io.micronaut.http.MediaType.TEXT_PLAIN_TYPE;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.THREAD_POOLS_METRICS_SOURCE_NAME;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.metric.MetricSource;
import org.apache.ignite.internal.rest.api.metric.NodeMetricSources;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

@MicronautTest
class ItMetricControllerTest extends ClusterPerClassIntegrationTest {
    private static final MetricSource[] ALL_METRIC_SOURCES = {
            new MetricSource("jvm", true),
            new MetricSource("os", true),
            new MetricSource("raft", true),
            new MetricSource("metastorage", true),
            new MetricSource("client.handler", true),
            new MetricSource("sql.client", true),
            new MetricSource("sql.plan.cache", true),
            new MetricSource("sql.queries", true),
            new MetricSource("storage.aipersist.default", true),
            new MetricSource("storage.aipersist.default_aipersist", true),
            new MetricSource("storage.aipersist.checkpoint", true),
            new MetricSource("storage.aipersist.io", true),
            new MetricSource("storage.aipersist", true),
            new MetricSource("topology.cluster", true),
            new MetricSource("topology.local", true),
            new MetricSource("thread.pools.partitions-executor", true),
            new MetricSource("thread.pools.sql-executor", true),
            new MetricSource("thread.pools.sql-planning-executor", true),
            new MetricSource("transactions", true),
            new MetricSource("resource.vacuum", true),
            new MetricSource("placement-driver", true),
            new MetricSource("clock.service", true),
            new MetricSource("index.builder", true),
            new MetricSource("raft.snapshots", true),
            new MetricSource("messaging", true),
            new MetricSource("log.storage", true),
            new MetricSource(THREAD_POOLS_METRICS_SOURCE_NAME + ".striped.messaging.inbound.default", true),
            new MetricSource(THREAD_POOLS_METRICS_SOURCE_NAME + ".striped.messaging.inbound.deploymentunits", true),
            new MetricSource(THREAD_POOLS_METRICS_SOURCE_NAME + ".striped.messaging.inbound.scalecube", true),
            new MetricSource(THREAD_POOLS_METRICS_SOURCE_NAME + ".messaging.outbound", true),
    };

    @Inject
    @Client("http://localhost:10300/management/v1/metric/node/")
    HttpClient node0Client;

    @Inject
    @Client("http://localhost:10301/management/v1/metric/node/")
    HttpClient node1Client;

    @Inject
    @Client("http://localhost:10302/management/v1/metric/node/")
    HttpClient node2Client;

    @Inject
    @Client("http://localhost:10300/management/v1/metric/cluster/")
    HttpClient clusterClient;

    @Test
    void listNodeMetrics() {
        HttpResponse<List<MetricSource>> response = node0Client.toBlocking().exchange(GET("source"), listOf(MetricSource.class));
        assertThat(response, hasStatus(HttpStatus.OK));
        assertThat(response.body(), containsInAnyOrder(ALL_METRIC_SOURCES));
    }

    private static Matcher<NodeMetricSources> hasNodeName(Matcher<String> nodeNameMatcher) {
        return new FeatureMatcher<>(nodeNameMatcher, "a node metric sources with node name", "node") {
            @Override
            protected String featureValueOf(NodeMetricSources actual) {
                return actual.node();
            }
        };
    }

    private static Matcher<NodeMetricSources> hasSources(Matcher<Iterable<? extends MetricSource>> sourcesMatcher) {
        return new FeatureMatcher<>(sourcesMatcher, "a node metric sources with sources list", "sources") {
            @Override
            protected Iterable<? extends MetricSource> featureValueOf(NodeMetricSources actual) {
                return actual.sources();
            }
        };
    }

    @Test
    void listClusterMetrics() {
        HttpResponse<List<NodeMetricSources>> response = clusterClient.toBlocking().exchange(
                GET("source"),
                listOf(NodeMetricSources.class)
        );
        assertThat(response, hasStatus(HttpStatus.OK));

        List<Matcher<? super NodeMetricSources>> matchers = CLUSTER.runningNodes()
                .map(ignite -> both(hasNodeName(is(ignite.name()))).and(hasSources(containsInAnyOrder(ALL_METRIC_SOURCES))))
                .collect(toList());

        assertThat(response.body(), containsInAnyOrder(matchers));
    }

    @Test
    void enableDisableNodeMetrics() {
        HttpResponse<Void> response = node0Client.toBlocking().exchange(POST("disable", "jvm").contentType(TEXT_PLAIN_TYPE));
        assertThat(response, hasStatus(HttpStatus.OK));

        assertJvmMetric(node0Client, false);
        assertJvmMetric(node1Client, true);
        assertJvmMetric(node2Client, true);

        response = node0Client.toBlocking().exchange(POST("enable", "jvm").contentType(TEXT_PLAIN_TYPE));
        assertThat(response, hasStatus(HttpStatus.OK));

        assertJvmMetric(node0Client, true);
        assertJvmMetric(node1Client, true);
        assertJvmMetric(node2Client, true);
    }

    @Test
    void enableDisableClusterMetrics() {
        HttpResponse<Void> response = clusterClient.toBlocking().exchange(POST("disable", "jvm").contentType(TEXT_PLAIN_TYPE));
        assertThat(response, hasStatus(HttpStatus.OK));

        assertJvmMetric(node0Client, false);
        assertJvmMetric(node1Client, false);
        assertJvmMetric(node2Client, false);

        response = clusterClient.toBlocking().exchange(POST("enable", "jvm").contentType(TEXT_PLAIN_TYPE));
        assertThat(response, hasStatus(HttpStatus.OK));

        assertJvmMetric(node0Client, true);
        assertJvmMetric(node1Client, true);
        assertJvmMetric(node2Client, true);
    }

    @Test
    void nodeNonExistingMetrics() {
        assertThrowsProblem(
                () -> node0Client.toBlocking().exchange(POST("disable", "no.such.metric").contentType(TEXT_PLAIN_TYPE)),
                isProblem().withStatus(NOT_FOUND).withDetail("Metrics source with given name doesn't exist: no.such.metric")
        );
    }

    @Test
    void clusterNonExistingMetrics() {
        assertThrowsProblem(
                () -> clusterClient.toBlocking().exchange(POST("disable", "no.such.metric").contentType(TEXT_PLAIN_TYPE)),
                isProblem().withStatus(NOT_FOUND).withDetail("Metrics source with given name doesn't exist: no.such.metric")
        );
    }

    private static void assertJvmMetric(HttpClient client, boolean enabled) {
        HttpResponse<List<MetricSource>> sources = client.toBlocking().exchange(GET("source"), listOf(MetricSource.class));
        assertThat(sources, hasStatus(HttpStatus.OK));
        assertThat(sources.body(), hasItem(new MetricSource("jvm", enabled)));
    }
}
