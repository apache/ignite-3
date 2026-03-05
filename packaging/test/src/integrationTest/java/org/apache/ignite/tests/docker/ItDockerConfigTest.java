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

package org.apache.ignite.tests.docker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * Basic tests for Docker configuration.
 */
@Testcontainers
public class ItDockerConfigTest {
    private static final Logger LOG = Logger.getLogger(ItDockerConfigTest.class.getName());
    private static final String DOCKER_IMAGE = "apacheignite/ignite:latest";
    private static final Network network = Network.newNetwork();

    @Container
    private static final GenericContainer<?> node1 = createNode(1);

    @Container
    private static final GenericContainer<?> node2 = createNode(2);

    @Container
    private static final GenericContainer<?> node3 = createNode(3);

    private static final List<GenericContainer<?>> igniteNodes = List.of(node1, node2, node3);

    private static GenericContainer<?> createNode(int index) {
        return new GenericContainer<>(DOCKER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("node" + index)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .withCopyToContainer(MountableFile.forClasspathResource("/org/apache/ignite/tests/docker/ignite-config.conf"),
                        "/opt/ignite/etc/ignite-config.conf")
                .withCommand("--node-name node" + index)
                .withLogConsumer(frame -> {
                    switch (frame.getType()) {
                        case STDOUT:
                        case END:
                            LOG.info(frame.getUtf8String());
                            break;
                        case STDERR:
                            LOG.severe(frame.getUtf8String());
                            break;
                    }
                })
                .withExposedPorts(10300, 10800)
                .waitingFor(Wait.forListeningPorts(10300, 10800))
                .waitingFor(Wait.forLogMessage(".*Joining the cluster.*", 1));
    }

    @AfterAll
    public static void tearDownNetwork() {
        if (network != null) {
            network.close();
        }
    }

    @Test
    public void testClusterNodesAreRunningAsCluster() throws IOException, InterruptedException {
        igniteNodes.forEach(node -> {
                    assertTrue(node.isRunning(), "Node should be running: " + node.getNetworkAliases());
                    assertThat(node.getExposedPorts()).contains(10300, 10800);
                }
        );

        int restPort = igniteNodes.get(0).getMappedPort(10300);
        String restUrl = "http://localhost:" + restPort;
        // Check physical topology via REST API
        String topologyUrlUrl = restUrl + "/management/v1/cluster/topology/physical";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest topologyRequest = HttpRequest.newBuilder()
                .uri(URI.create(topologyUrlUrl))
                .GET()
                .build();

        HttpResponse<String> topologyResponse = client.send(topologyRequest, HttpResponse.BodyHandlers.ofString());
        assertThat(topologyResponse.statusCode()).isEqualTo(200);

        assertThat(topologyResponse.body()).contains(igniteNodes.stream().map(n -> n.getNetworkAliases().get(1)).toArray(String[]::new));

        // Update node configuration via REST API
        String updateConfig = "ignite.nodeAttributes.nodeAttributes={test_attr.attribute=\"zone_value\"}";

        HttpRequest configUpdateRequest = HttpRequest.newBuilder()
                .uri(URI.create(restUrl + "/management/v1/configuration/node"))
                .header("Content-Type", "text/plain")
                .method("PATCH", BodyPublishers.ofString(updateConfig))
                .build();

        HttpResponse<String> configUpdateResponse = client.send(configUpdateRequest, HttpResponse.BodyHandlers.ofString());

        // Check status code
        assertThat(configUpdateResponse.statusCode())
                .withFailMessage("Node config update failed: " + configUpdateResponse.body())
                .isBetween(200, 299);

        // Check updated configuration via REST API
        String nodeConfig = restUrl + "/management/v1/configuration/node";

        HttpRequest nodeConfigRequest = HttpRequest.newBuilder()
                .uri(URI.create(nodeConfig))
                .GET()
                .build();
        HttpResponse<String> responseNodeConfig = client.send(nodeConfigRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(responseNodeConfig.statusCode()).isEqualTo(200);
        assertThat(responseNodeConfig.body()).contains("test_attr", "zone_value");
    }
}
