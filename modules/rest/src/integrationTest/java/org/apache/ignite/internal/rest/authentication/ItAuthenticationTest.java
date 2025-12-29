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

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.apache.ignite.internal.rest.AbstractRestTestBase.get;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCode;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCodeAndBody;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** Tests for the REST authentication configuration. */
public class ItAuthenticationTest extends ClusterPerTestIntegrationTest {
    private final HttpClient client = HttpClient.newBuilder().build();

    private @Nullable String username = "admin";

    private @Nullable String password = "password";

    private boolean enableAuth = true;

    @BeforeEach
    @Override
    public void startCluster(TestInfo testInfo) {
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
        super.startCluster(testInfo);

        // Then.
        assertRestAvailable("", "");
    }

    @Test
    public void defaultUser(TestInfo testInfo) throws Exception {
        username = null;
        password = null;
        super.startCluster(testInfo);

        // Authentication is enabled.
        await().untilAsserted(() -> assertRestNotAvailable("", ""));

        // REST is available with valid credentials
        assertRestAvailable("ignite", "ignite");
    }

    @Test
    public void changeCredentials(TestInfo testInfo) throws Exception {
        super.startCluster(testInfo);

        // Then.
        // Authentication is enabled.
        await().untilAsserted(() -> assertRestNotAvailable("", ""));

        // REST is available with valid credentials
        assertRestAvailable("admin", "password");

        // REST is not available with invalid credentials
        assertRestNotAvailable("admin", "wrong-password");

        // Change credentials.
        String updateRestAuthConfigBody = "ignite.security.authentication.providers.default.users.admin.password=new-password";

        updateClusterConfiguration(node(0), "admin", "password", updateRestAuthConfigBody);

        // REST is not available with old credentials
        await().untilAsserted(() -> assertRestNotAvailable("admin", "password"));

        // REST is available with new credentials
        assertRestAvailable("admin", "new-password");
    }

    @Test
    public void enableAuthenticationAndRestartNode(TestInfo testInfo) throws Exception {
        super.startCluster(testInfo);

        // Then.
        // Authentication is enabled.
        await().untilAsserted(() -> assertRestNotAvailable("", ""));

        // REST is available with valid credentials
        assertRestAvailable("admin", "password");

        // REST is not available with invalid credentials
        assertRestNotAvailable("admin", "wrong-password");

        // Restart a node.
        restartNode(2);

        // REST is available with valid credentials
        assertRestAvailable("admin", "password");

        // REST is not available with invalid credentials
        assertRestNotAvailable("admin", "wrong-password");
    }

    @Test
    public void probes(TestInfo testInfo) throws Exception {
        super.startCluster(testInfo);

        // Authentication is enabled.
        await().untilAsserted(() -> assertRestNotAvailable("", ""));

        cluster.runningNodes().forEach(node -> {
            assertThat(
                    send(get(getHost(node), "/health/liveness")),
                    hasStatusCodeAndBody(200, hasJsonPath("$.status", is("UP")))
            );

            assertThat(
                    send(get(getHost(node), "/health/readiness")),
                    hasStatusCodeAndBody(200, hasJsonPath("$.status", is("UP")))
            );

            // Health probe is a combination of all indicators not qualified as "liveness".
            assertThat(
                    send(get(getHost(node), "/health")),
                    hasStatusCodeAndBody(200, hasJsonPath("$.status", is("UP")))
            );
        });
    }

    private void updateClusterConfiguration(Ignite node, String username, String password, String configToApply) {
        HttpRequest updateClusterConfigRequest = HttpRequest.newBuilder(getClusterConfigUri(node))
                .header("content-type", "text/plain")
                .header("Authorization", basicAuthenticationHeader(username, password))
                .method("PATCH", BodyPublishers.ofString(configToApply))
                .build();

        assertThat(send(updateClusterConfigRequest), hasStatusCode(200));
    }

    private void assertRestNotAvailable(String username, String password) {
        cluster.runningNodes().forEach(node -> assertClusterConfigStatusIs(node, username, password, 401));
    }

    private void assertRestAvailable(String username, String password) {
        cluster.runningNodes().forEach(node -> assertClusterConfigStatusIs(node, username, password, 200));
    }

    private void assertClusterConfigStatusIs(Ignite node, String username, String password, int expectedStatusCode) {
        HttpRequest clusterConfigRequest = HttpRequest.newBuilder(getClusterConfigUri(node))
                .header("Authorization", basicAuthenticationHeader(username, password))
                .build();
        assertThat(send(clusterConfigRequest), hasStatusCode(expectedStatusCode));
    }

    private URI getClusterConfigUri(Ignite node) {
        return URI.create(getHost(node) + "/management/v1/configuration/cluster/");
    }

    private String getHost(Ignite node) {
        int port = cluster.httpPort(cluster.nodeIndex(node.name()));
        return "http://localhost:" + port;
    }

    private HttpResponse<String> send(HttpRequest request) {
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
