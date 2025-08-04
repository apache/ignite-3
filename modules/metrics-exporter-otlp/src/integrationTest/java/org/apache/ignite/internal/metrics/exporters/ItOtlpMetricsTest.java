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

package org.apache.ignite.internal.metrics.exporters;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.exporters.otlp.OtlpPushMetricExporter;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for OTLP metrics.
 */
public class ItOtlpMetricsTest extends ClusterPerTestIntegrationTest {
    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(
                "ignite.metrics.exporters.otlp = {"
                        + "exporterName = otlp,"
                        + "periodMillis = 1," // Small period is required to trigger possible races on node start.
                        + "endpoint = \"http://localhost:4317\","
                        + "protocol = \"http/protobuf\"}"
        );
    }

    /**
     * Mock HTTP server needed to reduce the amount of errors in logs (because we don't start an OTLP server).
     */
    private HttpServer server;

    private final LogInspector logInspector = new LogInspector(
            OtlpPushMetricExporter.class.getName(),
            evt -> evt.getMessage().getFormattedMessage().contains("Metrics export error")
    );

    @BeforeEach
    @Override
    public void startCluster(TestInfo testInfo) throws Exception {
        server = HttpServer.create(new InetSocketAddress(4317), 0);

        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        server.start();

        logInspector.start();

        super.startCluster(testInfo);
    }

    @AfterEach
    @Override
    public void stopCluster() {
        super.stopCluster();

        logInspector.stop();

        server.stop(0);
    }

    /**
     * Tests that OTLP scheduled metrics are started on node start without any errors.
     */
    @Test
    void otlpMetricsGetCorrectlyReported() {
        cluster.runningNodes().forEach(node -> {
            MetricManager metricManager = unwrapIgniteImpl(node).metricManager();

            assertThat(metricManager.enabledExporters(), containsInRelativeOrder(instanceOf(OtlpPushMetricExporter.class)));
        });

        assertFalse(logInspector.isMatched());
    }
}
