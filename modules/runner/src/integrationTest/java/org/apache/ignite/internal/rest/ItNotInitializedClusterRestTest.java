/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for the REST endpoints in case cluster is not initialized.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItNotInitializedClusterRestTest extends AbstractRestTestBase {

    private ObjectMapper objectMapper;

    @BeforeEach
    @Override
    void setUp(TestInfo testInfo) throws IOException, InterruptedException {
        super.setUp(testInfo);
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Node configuration is available when the cluster in not initialized")
    void nodeConfiguration() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/node
        HttpResponse<String> response = client.send(
                get("/management/v1/configuration/node"),
                BodyHandlers.ofString()
        );

        // Expect node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(response.body());
        // And has rest.port config value
        assertThat(config.getInt("rest.port"), is(equalTo(10300)));
    }

    @Test
    @DisplayName("Node configuration can be changed when the cluster in not initialized")
    void nodeConfigurationUpdate() throws IOException, InterruptedException {
        // When PATCH /management/v1/configuration/node rest.port=10333
        HttpResponse<String> pathResponce = client.send(
                patch("/management/v1/configuration/node", "rest.port=10333"),
                BodyHandlers.ofString()
        );
        // Then
        assertThat(pathResponce.statusCode(), is(200));

        // And GET /management/v1/configuration/node
        HttpResponse<String> getResponse = client.send(
                get("/management/v1/configuration/node"),
                BodyHandlers.ofString()
        );

        // Then node configuration can be parsed to hocon format
        Config config = ConfigFactory.parseString(getResponse.body());
        // And rest.port is updated
        assertThat(config.getInt("rest.port"), is(equalTo(10333)));
    }

    @Test
    @DisplayName("Cluster configuration is not available on not initialized cluster")
    void clusterConfiguration() throws IOException, InterruptedException {
        // When GET /management/v1/configuration/cluster
        HttpResponse<String> response = client.send(get("/management/v1/configuration/cluster"), BodyHandlers.ofString());

        // Expect cluster configuration is not available
        Problem problem = objectMapper.readValue(response.body(), Problem.class);
        assertAll(
                () -> assertThat(problem.status(), is(404)),
                () -> assertThat(problem.title(), is("Not Found")),
                () -> assertThat(problem.detail(),
                        is("Cluster not initialized. Call /management/v1/cluster/init in order to initialize cluster"))
        );
    }

    @Test
    @DisplayName("Cluster configuration could not be updated on not initialized cluster")
    void clusterConfigurationUpdate() throws IOException, InterruptedException {
        // When PATCH /management/v1/configuration/cluster
        HttpResponse<String> response = client.send(
                patch("/management/v1/configuration/cluster", "any.key=any-value"),
                BodyHandlers.ofString()
        );

        // Expect cluster configuration could not be updated
        Problem problem = objectMapper.readValue(response.body(), Problem.class);
        assertAll(
                () -> assertThat(problem.status(), is(404)),
                () -> assertThat(problem.title(), is("Not Found")),
                () -> assertThat(problem.detail(),
                        is("Cluster not initialized. Call /management/v1/cluster/init in order to initialize cluster"))
        );
    }

    @Test
    @DisplayName("Logical topology is not available on not initialized cluster")
    void logicalTopology() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/topology/logical
        HttpResponse<String> response = client.send(get("/management/v1/cluster/topology/logical"), BodyHandlers.ofString());

        // Then
        Problem problem = objectMapper.readValue(response.body(), Problem.class);
        assertAll(
                () -> assertThat(problem.status(), is(404)),
                () -> assertThat(problem.title(), is("Not Found")),
                () -> assertThat(problem.detail(),
                        is("Cluster not initialized. Call /management/v1/cluster/init in order to initialize cluster"))
        );
    }

    @Test
    @DisplayName("Physical topology is available on not initialized cluster")
    void physicalTopology() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/topology/physical
        HttpResponse<String> response = client.send(get("/management/v1/cluster/topology/physical"), BodyHandlers.ofString());

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
    @DisplayName("Node state is available on not initialized cluster and it is STARTING")
    void nodeState() throws IOException, InterruptedException {
        // When GET /management/v1/node/state
        HttpResponse<String> response = client.send(get("/management/v1/node/state"), BodyHandlers.ofString());

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$.name")),
                () -> assertThat(response.body(), hasJsonPath("$.state", is(equalTo("STARTING"))))
        );
    }

    @Test
    @DisplayName("Cluster state is not available on not initialized cluster")
    void clusterState() throws IOException, InterruptedException {
        // When GET /management/v1/cluster/state
        HttpResponse<String> response = client.send(get("/management/v1/cluster/state"), BodyHandlers.ofString());

        // Then
        Problem problem = objectMapper.readValue(response.body(), Problem.class);
        assertAll(
                () -> assertThat(problem.status(), is(404)),
                () -> assertThat(problem.title(), is("Not Found")),
                () -> assertThat(problem.detail(),
                        is("Cluster not initialized. Call /management/v1/cluster/init in order to initialize cluster"))
        );
    }
}
