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

package org.apache.ignite.internal.rest;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.apache.ignite.internal.testframework.matchers.HttpResponseMatcher.hasStatusCodeAndBody;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.http.HttpResponse;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test for the REST endpoints in case cluster is initialized.
 */
public class ItInitializedClusterRestTest extends AbstractRestTestBase {
    @BeforeEach
    @Override
    void setUp(TestInfo testInfo) throws IOException, InterruptedException {
        super.setUp(testInfo);

        // For each test case the cluster is already initialized
        String requestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + nodeNames.get(0) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authConfig\": {\n"
                + "        \"enabled\": false\n"
                + "    }\n"
                + "}";

        HttpResponse<String> response = send(post("/management/v1/cluster/init", requestBody));

        assertThat(response.statusCode(), is(200));
        checkAllNodesStarted();
    }

    @Test
    @DisplayName("Node configuration is available when the cluster is initialized")
    void nodeConfiguration() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        HttpResponse<String> response = send(get("/management/v1/configuration/node"));

        // Expect node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And has rest.port config value
        assertThat(config.getInt("ignite.rest.port"), is(equalTo(10300)));
    }

    @Test
    @DisplayName("Node configuration by path is available when the cluster is initialized")
    void nodeConfigurationByPath() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node and path selector is "rest"
        HttpResponse<String> response = send(get("/management/v1/configuration/node/ignite.rest"));

        // Expect node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And has rest.port config value
        assertThat(config.getInt("port"), is(equalTo(10300)));
    }

    @Test
    @DisplayName("Node configuration can be changed when the cluster is initialized")
    void nodeConfigurationUpdate() throws IOException, InterruptedException {
        // When PATCH /management/v1/configuration/node rest.port=10333
        HttpResponse<String> pathResponse = send(patch("/management/v1/configuration/node", "ignite.rest.port=10333"));
        // Then
        assertThat(pathResponse.statusCode(), is(200));

        // And GET /management/v1/configuration/node
        HttpResponse<String> getResponse = send(get("/management/v1/configuration/node"));

        // Then node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(getResponse.body());
        // And rest.port is updated
        assertThat(config.getInt("ignite.rest.port"), is(equalTo(10333)));
    }

    @Test
    @DisplayName("Cluster configuration is available when the cluster is initialized")
    void clusterConfiguration() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/cluster
        HttpResponse<String> response = send(get("/management/v1/configuration/cluster"));

        // Then cluster configuration is not available
        assertThat(response.statusCode(), is(200));
        // And configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And gc.batchSize can be read
        assertThat(config.getInt("ignite.gc.batchSize"), is(equalTo(5)));
    }

    @Test
    @DisplayName("Cluster configuration can be updated when the cluster is initialized")
    void clusterConfigurationUpdate() throws IOException, InterruptedException {
        // When PATCH /management/v1/configuration/cluster
        HttpResponse<String> patchRequest = send(patch("/management/v1/configuration/cluster", "ignite.gc.batchSize=1"));

        // Then
        assertThat(patchRequest.statusCode(), is(200));
        // And value was updated
        HttpResponse<String> getResponse = send(get("/management/v1/configuration/cluster"));
        assertThat(getResponse.statusCode(), is(200));
        // And
        Config config = ConfigFactory.parseString(getResponse.body());
        assertThat(config.getInt("ignite.gc.batchSize"), is(equalTo(1)));
    }

    @Test
    @DisplayName("Cluster configuration can not be updated if provided config did not pass the validation")
    void clusterConfigurationUpdateValidation() throws IOException, InterruptedException {
        // When PATCH /management/v1/configuration/cluster invalid with invalid value
        HttpResponse<String> patchRequest = send(patch("/management/v1/configuration/cluster", "ignite {\n"
                + "    security.enabled:true\n"
                + "    security.authentication.providers:null\n"
                + "}"));

        // Then
        assertThat(
                patchRequest,
                hasStatusCodeAndBody(
                        400,
                        containsString(
                                "Validation did not pass for keys: "
                                        + "[ignite.security.authentication.providers, At least one provider is required.]"
                        )
                )
        );
    }

    @Test
    @DisplayName("Cluster configuration by path is available when the cluster is initialized")
    void clusterConfigurationByPath() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/cluster and path selector is "gc"
        HttpResponse<String> response = send(get("/management/v1/configuration/cluster/ignite.gc"));

        // Then cluster configuration is not available
        assertThat(response.statusCode(), is(200));
        // And configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And gc.batchSize can be read
        assertThat(config.getInt("batchSize"), is(equalTo(5)));
    }

    @Test
    @DisplayName("Logical topology is available on initialized cluster")
    void logicalTopology() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/topology/logical
        HttpResponse<String> response = send(get("/management/v1/cluster/topology/logical"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$", hasSize(3))),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].name")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].id")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.host")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.port"))
        );
    }

    @Test
    @DisplayName("Physical topology is available on initialized cluster")
    void physicalTopology() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/topology/physical
        HttpResponse<String> response = send(get("/management/v1/cluster/topology/physical"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$", hasSize(3))),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].name")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].id")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.host")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.port"))
        );
    }

    @Test
    @DisplayName("Cluster state is available on initialized cluster")
    void clusterState() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/status
        HttpResponse<String> response = send(get("/management/v1/cluster/state"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$.clusterTag.clusterName", is(equalTo("cluster")))),
                () -> assertThat(response.body(), hasJsonPath("$.clusterTag.clusterId")),
                () -> assertThat(response.body(), hasJsonPath("$.igniteVersion")),
                () -> assertThat(response.body(), hasJsonPath("$.msNodes")),
                () -> assertThat(response.body(), hasJsonPath("$.cmgNodes"))
        );
    }

    @Test
    @DisplayName("Node state is available on initialized cluster")
    void nodeState() throws IOException, InterruptedException {
        // When GET /management/v1/node/status
        HttpResponse<String> response = send(get("/management/v1/node/state"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$.name")),
                () -> assertThat(response.body(), hasJsonPath("$.state", is(equalTo("STARTED"))))
        );
    }

    @Test
    @DisplayName("Node version is available on initialized cluster")
    void nodeVersion() throws IOException, InterruptedException {
        // When GET /management/v1/node/version/
        HttpResponse<String> response = send(get("/management/v1/node/version/"));

        // Then
        assertThat(response.statusCode(), is(200));
        // And version is a semver
        assertThat(response.body(), matchesRegex(IgniteProductVersion.VERSION_PATTERN));
    }
}
