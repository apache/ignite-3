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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Base64;
import java.util.Set;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the REST authentication configuration. */
@ExtendWith(WorkDirectoryExtension.class)
public class ItAuthenticationTest extends ClusterPerTestIntegrationTest {
    /** HTTP client that is expected to be defined in subclasses. */
    private HttpClient client;

    private String username = "admin";

    private String password = "password";

    private boolean enableAuth = true;

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) {
        client = HttpClient.newBuilder()
                .build();
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        String config = "security.enabled=" + enableAuth;

        if (username != null && password != null) {
            config += ",\n"
                    + "security.authentication.providers.default={"
                    + "type=basic,"
                    + "users=[{username=" + username
                    + ",password=" + password
                    + "}]}";
        }

        builder.clusterConfiguration("ignite {\n" + config + "\n}");
    }

    @Test
    public void disabledAuthentication(TestInfo testInfo) throws Exception {
        enableAuth = false;
        super.setup(testInfo);

        // Then.
        cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).forEach(node ->
                assertTrue(isRestAvailable(node.restHttpAddress(), "", ""))
        );
    }

    @Test
    public void defaultUser(TestInfo testInfo) throws Exception {
        username = null;
        password = null;
        super.setup(testInfo);

        // Authentication is enabled.
        Set<IgniteImpl> nodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet());
        for (IgniteImpl node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.restHttpAddress(), "", ""),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with valid credentials
        for (IgniteImpl node : nodes) {
            assertTrue(isRestAvailable(node.restHttpAddress(), "ignite", "ignite"));
        }
    }

    @Test
    public void changeCredentials(TestInfo testInfo) throws Exception {
        super.setup(testInfo);
        Set<IgniteImpl> nodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet());

        // Then.
        // Authentication is enabled.
        for (IgniteImpl node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.restHttpAddress(), "", ""),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with valid credentials
        for (IgniteImpl node : nodes) {
            assertTrue(isRestAvailable(node.restHttpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (IgniteImpl node : nodes) {
            assertFalse(isRestAvailable(node.restHttpAddress(), "admin", "wrong-password"));
        }

        // Change credentials.
        String updateRestAuthConfigBody = "ignite.security.authentication.providers.default.users.admin.password=new-password";

        updateClusterConfiguration(unwrapIgniteImpl(node(0)).restHttpAddress(), "admin", "password", updateRestAuthConfigBody);

        // REST is not available with old credentials
        for (IgniteImpl node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.restHttpAddress(), "admin", "password"),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with new credentials
        for (IgniteImpl node : nodes) {
            assertTrue(isRestAvailable(node.restHttpAddress(), "admin", "new-password"));
        }
    }

    @Test
    public void enableAuthenticationAndRestartNode(TestInfo testInfo) throws Exception {
        super.setup(testInfo);
        Set<IgniteImpl> nodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet());

        // Then.
        // Authentication is enabled.
        for (IgniteImpl node : nodes) {
            assertTrue(waitForCondition(() -> isRestNotAvailable(node.restHttpAddress(), "", ""),
                    Duration.ofSeconds(5).toMillis()));
        }

        // REST is available with valid credentials
        for (IgniteImpl node : nodes) {
            assertTrue(isRestAvailable(node.restHttpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (IgniteImpl node : nodes) {
            assertFalse(isRestAvailable(node.restHttpAddress(), "admin", "wrong-password"));
        }

        // Restart a node.
        restartNode(2);

        nodes = cluster.runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toSet());

        // REST is available with valid credentials
        for (IgniteImpl node : nodes) {
            assertTrue(isRestAvailable(node.restHttpAddress(), "admin", "password"));
        }

        // REST is not available with invalid credentials
        for (IgniteImpl node : nodes) {
            assertFalse(isRestAvailable(node.restHttpAddress(), "admin", "wrong-password"));
        }
    }

    private void updateClusterConfiguration(NetworkAddress address, String username, String password, String configToApply) {
        URI updateClusterConfigUri = URI.create("http://" + hostUrl(address) + "/management/v1/configuration/cluster/");
        HttpRequest updateClusterConfigRequest = HttpRequest.newBuilder(updateClusterConfigUri)
                .header("content-type", "text/plain")
                .header("Authorization", basicAuthenticationHeader(username, password))
                .method("PATCH", BodyPublishers.ofString(configToApply))
                .build();

        assertThat(sendRequest(client, updateClusterConfigRequest), hasStatusCode(200));
    }

    private boolean isRestNotAvailable(NetworkAddress baseUrl, String username, String password) {
        return !isRestAvailable(baseUrl, username, password);
    }

    private boolean isRestAvailable(NetworkAddress address, String username, String password) {
        URI clusterConfigUri = URI.create("http://" + hostUrl(address) + "/management/v1/configuration/cluster/");
        HttpRequest clusterConfigRequest = HttpRequest.newBuilder(clusterConfigUri)
                .header("Authorization", basicAuthenticationHeader(username, password))
                .build();
        HttpResponse<String> response = sendRequest(client, clusterConfigRequest);
        int code = response.statusCode();
        if (code == 200) {
            return true;
        } else if (code == 401) {
            return false;
        } else {
            throw new IllegalStateException("Unexpected response code: " + code + ", body: " + response.body());
        }
    }

    private static String hostUrl(NetworkAddress networkAddress) {
        try {
            InetAddress host = InetAddress.getByName(networkAddress.host());
            return host.getHostAddress() + ":" + networkAddress.port();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpResponse<String> sendRequest(HttpClient client, HttpRequest request) {
        try {
            return client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String basicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }
}
