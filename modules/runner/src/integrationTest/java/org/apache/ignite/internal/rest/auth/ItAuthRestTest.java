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

package org.apache.ignite.internal.rest.auth;


import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.rest.RestNode;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;


/** Tests for the REST authentication configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItAuthRestTest {

    /** HTTP client that is expected to be defined in subclasses. */
    private HttpClient client;

    private List<RestNode> nodes;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        client = HttpClient.newBuilder()
                .build();

        nodes = IntStream.range(0, 3)
                .mapToObj(id -> {
                    return RestNode.builder()
                            .setWorkDir(workDir)
                            .setName(testNodeName(testInfo, id))
                            .setNetworkPort(3344 + id)
                            .setHttpPort(10300 + id)
                            .setHttpsPort(10400 + id)
                            .setSslEnabled(false)
                            .setDualProtocol(false)
                            .build();
                })
                .collect(Collectors.toList());

        nodes.forEach(RestNode::start);
    }

    @Test
    public void disabledAuthentication(TestInfo testInfo) throws IOException, InterruptedException {
        RestNode metaStorageNode = nodes.get(0);

        String initClusterBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + metaStorageNode.name() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authConfig\": {\n"
                + "        \"enabled\": false\n"
                + "    }\n"
                + "}";

        URI uri = URI.create(metaStorageNode.httpAddress() + "/management/v1/cluster/init");
        HttpRequest initRequest = HttpRequest.newBuilder(uri)
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(initClusterBody))
                .build();

        HttpResponse<String> initResponse = client.send(initRequest, BodyHandlers.ofString());
        assertThat(initResponse.statusCode(), is(200));
        checkAllNodesStarted(nodes);

        URI clusterConfigUri = URI.create(metaStorageNode.httpAddress() + "/management/v1/configuration/cluster/");
        HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                .build();

        HttpResponse<String> httpResponse = client.send(clusterConfigRequest, BodyHandlers.ofString());
        assertEquals(200, httpResponse.statusCode());
    }

    @Test
    public void basicAuthentication(TestInfo testInfo) throws IOException, InterruptedException {
        RestNode metaStorageNode = nodes.get(0);

        String msNodes = nodes.stream()
                .map(it -> it.name())
                .map(name -> "\"" + name + "\"")
                .collect(Collectors.joining(","));

        String initClusterBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + msNodes
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authConfig\": {\n"
                + "      \"enabled\": true,\n"
                + "      \"providers\": [\n"
                + "        {\n"
                + "          \"name\": \"basic\",\n"
                + "          \"type\": \"basic\",\n"
                + "          \"login\": \"admin\",\n"
                + "          \"password\": \"admin\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }";

        URI clusterInitUri = URI.create(metaStorageNode.httpAddress() + "/management/v1/cluster/init");
        HttpRequest initRequest = HttpRequest.newBuilder(clusterInitUri)
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(initClusterBody))
                .build();

        HttpResponse<String> initResponse = client.send(initRequest, BodyHandlers.ofString());
        assertThat(initResponse.statusCode(), is(200));
        checkAllNodesStarted(nodes);

        for (RestNode node : nodes) {
            URI clusterConfigUri = URI.create(node.httpAddress() + "/management/v1/configuration/cluster/");
            HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri).build();
            assertTrue(waitForCondition(() -> {
                try {
                    return client.send(clusterConfigRequest, BodyHandlers.ofString()).statusCode() == 401;
                } catch (Exception e) {
                    return false;
                }
            }, Duration.ofSeconds(5).toMillis()));
        }

        for (RestNode node : nodes) {
            URI clusterConfigUri = URI.create(node.httpAddress() + "/management/v1/configuration/cluster/");
            HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                    .header("Authorization", basicAuthenticationHeader("admin", "admin"))
                    .build();
            assertEquals(200, client.send(clusterConfigRequest, BodyHandlers.ofString()).statusCode());
        }

        for (RestNode node : nodes) {
            URI clusterConfigUri = URI.create(node.httpAddress() + "/management/v1/configuration/cluster/");
            HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                    .header("Authorization", basicAuthenticationHeader("admin", "password"))
                    .build();
            assertEquals(401, client.send(clusterConfigRequest, BodyHandlers.ofString()).statusCode());
        }

        String updateRestAuthConfigBody = "{\n"
                + "    \"rest\": {\n"
                + "        \"authConfiguration\": {\n"
                + "            \"enabled\": true,\n"
                + "            \"providers\": [\n"
                + "                {\n"
                + "                    \"name\": \"basic\",\n"
                + "                    \"type\": \"basic\",\n"
                + "                    \"login\": \"admin\",\n"
                + "                    \"password\": \"password\"\n"
                + "                }\n"
                + "            ]\n"
                + "        }\n"
                + "    }\n"
                + "}";

        URI updateClusterConfigUri = URI.create(metaStorageNode.httpAddress() + "/management/v1/configuration/cluster/");
        HttpRequest updateClusterConfigRequest = HttpRequest.newBuilder(updateClusterConfigUri)
                .header("content-type", "text/plain")
                .header("Authorization", basicAuthenticationHeader("admin", "admin"))
                .method("PATCH", BodyPublishers.ofString(updateRestAuthConfigBody))
                .build();

        HttpResponse<String> updateClusterConfigResponse = client.send(updateClusterConfigRequest, BodyHandlers.ofString());
        assertThat(updateClusterConfigResponse.statusCode(), is(200));

        for (RestNode node : nodes) {
            URI clusterConfigUri = URI.create(node.httpAddress() + "/management/v1/configuration/cluster/");
            HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                    .header("Authorization", basicAuthenticationHeader("admin", "admin"))
                    .build();

            assertTrue(waitForCondition(() -> {
                try {
                    return client.send(clusterConfigRequest, BodyHandlers.ofString()).statusCode() == 401;
                } catch (Exception e) {
                    return false;
                }
            }, Duration.ofSeconds(5).toMillis()));
        }

        for (RestNode node : nodes) {
            URI clusterConfigUri = URI.create(node.httpAddress() + "/management/v1/configuration/cluster/");
            HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                    .header("Authorization", basicAuthenticationHeader("admin", "password"))
                    .build();
            assertEquals(200, client.send(clusterConfigRequest, BodyHandlers.ofString()).statusCode());
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RestNode::stop);
    }

    private static void checkAllNodesStarted(List<RestNode> nodes) {
        nodes.stream()
                .map(RestNode::igniteNodeFuture)
                .forEach(future -> assertThat(future, willCompleteSuccessfully()));
    }

    private static String basicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }
}
