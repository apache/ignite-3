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

package org.apache.ignite.internal.rest.authentication;


import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.rest.RestNode;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;


/** Tests for the REST authentication configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItAuthenticationTest extends BaseIgniteAbstractTest {

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
                            .workDir(workDir)
                            .name(testNodeName(testInfo, id))
                            .networkPort(3344 + id)
                            .httpPort(10300 + id)
                            .httpsPort(10400 + id)
                            .sslEnabled(false)
                            .dualProtocol(false)
                            .build();
                })
                .collect(toList());

        nodes.stream().parallel().forEach(RestNode::start);
    }

    @Test
    public void disabledAuthentication(TestInfo testInfo) {
        RestNode metaStorageNode = nodes.get(0);

        // when
        String initClusterBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + metaStorageNode.name() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\"\n"
                + "}";

        initCluster(metaStorageNode.httpAddress(), initClusterBody);

        // then
        for (RestNode node : nodes) {
            assertTrue(isRestAvailable(node.httpAddress(), "", ""));
        }
    }

    @Test
    public void changeCredentials(TestInfo testInfo) throws InterruptedException {
        // when
        RestNode metaStorageNode = nodes.get(0);

        String initClusterBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + metaStorageNode.name() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"clusterConfiguration\": \"{"
                + "         security.authentication.enabled:true, "
                + "         security.authentication.providers:[{name:basic,password:password,type:basic,username:admin}]}\"\n"
                + "  }";

        initCluster(metaStorageNode.httpAddress(), initClusterBody);

        // then
        // authentication is enabled
        for (RestNode node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.httpAddress(), "", ""),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with valid credentials
        for (RestNode node : nodes) {
            assertTrue(isRestAvailable(node.httpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (RestNode node : nodes) {
            assertFalse(isRestAvailable(node.httpAddress(), "admin", "wrong-password"));
        }

        // change password
        String updateRestAuthConfigBody = "{\n"
                + "    \"security\": {\n"
                + "        \"authentication\": {\n"
                + "            \"enabled\": true,\n"
                + "            \"providers\": [\n"
                + "                {\n"
                + "                    \"name\": \"basic\",\n"
                + "                    \"type\": \"basic\",\n"
                + "                    \"username\": \"admin\",\n"
                + "                    \"password\": \"new-password\"\n"
                + "                }\n"
                + "            ]\n"
                + "        }\n"
                + "    }\n"
                + "}";

        updateClusterConfiguration(metaStorageNode.httpAddress(), "admin", "password", updateRestAuthConfigBody);

        // REST is not available with old credentials
        for (RestNode node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.httpAddress(), "admin", "password"),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with new credentials
        for (RestNode node : nodes) {
            assertTrue(isRestAvailable(node.httpAddress(), "admin", "new-password"));
        }
    }

    @Test
    public void enableAuthenticationAndRestartNode(TestInfo testInfo) throws InterruptedException {
        // when
        RestNode metaStorageNode = nodes.get(0);

        String initClusterBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + metaStorageNode.name() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"clusterConfiguration\": \"{"
                + "         security.authentication.enabled:true, "
                + "         security.authentication.providers:[{name:basic,password:password,type:basic,username:admin}]}\"\n"
                + "  }";

        initCluster(metaStorageNode.httpAddress(), initClusterBody);

        // then
        // authentication is enabled
        for (RestNode node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.httpAddress(), "", ""),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with valid credentials
        for (RestNode node : nodes) {
            assertTrue(isRestAvailable(node.httpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (RestNode node : nodes) {
            assertFalse(isRestAvailable(node.httpAddress(), "admin", "wrong-password"));
        }

        // restart one of the nodes
        RestNode nodeToRestart = nodes.get(2);
        nodeToRestart.restart();
        waitForAllNodesStarted(Collections.singletonList(nodeToRestart));

        // REST is available with valid credentials
        for (RestNode node : nodes) {
            assertTrue(isRestAvailable(node.httpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (RestNode node : nodes) {
            assertFalse(isRestAvailable(node.httpAddress(), "admin", "wrong-password"));
        }
    }

    private void initCluster(String baseUrl, String initClusterBody) {
        URI clusterInitUri = URI.create(baseUrl + "/management/v1/cluster/init");
        HttpRequest initRequest = HttpRequest.newBuilder(clusterInitUri)
                .header("content-type", "application/json")
                .POST(BodyPublishers.ofString(initClusterBody))
                .build();

        HttpResponse<String> response = sendRequest(client, initRequest);
        assertThat(response.statusCode(), is(200));
        waitForAllNodesStarted(nodes);
    }

    private void updateClusterConfiguration(String baseUrl, String username, String password, String configToApply) {
        URI updateClusterConfigUri = URI.create(baseUrl + "/management/v1/configuration/cluster/");
        HttpRequest updateClusterConfigRequest = HttpRequest.newBuilder(updateClusterConfigUri)
                .header("content-type", "text/plain")
                .header("Authorization", basicAuthenticationHeader(username, password))
                .method("PATCH", BodyPublishers.ofString(configToApply))
                .build();

        HttpResponse<String> updateClusterConfigResponse = sendRequest(client, updateClusterConfigRequest);
        assertThat(updateClusterConfigResponse.statusCode(), is(200));
    }

    private boolean isRestNotAvailable(String baseUrl, String username, String password) {
        return !isRestAvailable(baseUrl, username, password);
    }

    private boolean isRestAvailable(String baseUrl, String username, String password) {
        URI clusterConfigUri = URI.create(baseUrl + "/management/v1/configuration/cluster/");
        HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                .header("Authorization", basicAuthenticationHeader(username, password))
                .build();
        int code = sendRequest(client, clusterConfigRequest).statusCode();
        if (code == 200) {
            return true;
        } else if (code == 401) {
            return false;
        } else {
            throw new IllegalStateException("Unexpected response code: " + code);
        }
    }

    private static HttpResponse<String> sendRequest(HttpClient client, HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        nodes.stream().parallel().forEach(RestNode::stop);
    }

    private static void waitForAllNodesStarted(List<RestNode> nodes) {
        nodes.stream()
                .map(RestNode::igniteNodeFuture)
                .forEach(future -> assertThat(future, willCompleteSuccessfully()));
    }

    private static String basicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }
}
