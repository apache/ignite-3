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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.http.HttpResponse;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for the REST endpoints in case cluster is not initialized.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItNotInitializedClusterRestTest extends AbstractRestTestBase {
    /** <a href="https://semver.org">semver</a> compatible regex. */
    private static final String IGNITE_SEMVER_REGEX =
            "(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<maintenance>\\d+)((?<snapshot>-SNAPSHOT)|-(?<alpha>alpha\\d+)|--(?<beta>beta\\d+))?";

    @Test
    @DisplayName("Node configuration is available when the cluster in not initialized")
    void nodeConfiguration() throws Exception {
        // When GET /management/v1/configuration/node.
        HttpResponse<String> response = send(get("/management/v1/configuration/node"));

        // Expect node configuration can be parsed to hocon format.
        Config config = ConfigFactory.parseString(response.body());
        // And has rest.port config value.
        assertThat(config.getInt("rest.port"), is(equalTo(10300)));
    }

    @Test
    @DisplayName("Node configuration can be changed when the cluster in not initialized")
    void nodeConfigurationUpdate() throws Exception {
        // When PATCH /management/v1/configuration/node rest.port=10333.
        HttpResponse<String> patchResponse = send(patch("/management/v1/configuration/node", "rest.port=10333"));
        // Then
        assertThat(patchResponse.statusCode(), is(200));

        // And GET /management/v1/configuration/node.
        HttpResponse<String> getResponse = send(get("/management/v1/configuration/node"));

        // Then node configuration can be parsed to hocon format.
        Config config = ConfigFactory.parseString(getResponse.body());
        // And rest.port is updated.
        assertThat(config.getInt("rest.port"), is(equalTo(10333)));
    }

    @Test
    @DisplayName("Physical topology is available on not initialized cluster")
    void physicalTopology() throws Exception {
        // When GET /management/v1/cluster/topology/physical.
        HttpResponse<String> response = send(get("/management/v1/cluster/topology/physical"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$", hasSize(3))),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].name")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].id")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.host")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].address.port")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].metadata.restPort")),
                () -> assertThat(response.body(), hasJsonPath("$[0:2].metadata.restHost"))
        );
    }

    @Test
    @DisplayName("Node state is available on not initialized cluster and it is STARTING")
    void nodeState() throws Exception {
        // When GET /management/v1/node/state.
        HttpResponse<String> response = send(get("/management/v1/node/state"));

        // Then
        assertThat(response.statusCode(), is(200));
        assertAll(
                () -> assertThat(response.body(), hasJsonPath("$.name")),
                () -> assertThat(response.body(), hasJsonPath("$.state", is(equalTo("STARTING"))))
        );
    }

    @Test
    @DisplayName("Node version is available on not initialized cluster")
    void nodeVersion() throws Exception {
        // When GET /management/v1/node/version/.
        HttpResponse<String> response = send(get("/management/v1/node/version/"));

        // Then.
        assertThat(response.statusCode(), is(200));
        // And version is a semver.
        assertThat(response.body(), matchesRegex(IGNITE_SEMVER_REGEX));
    }

    @Test
    @DisplayName("Cluster is not initialized, if config has a syntax error")
    void initClusterWithInvalidHoconConfig() throws Exception {
        // When POST /management/v1/cluster/init with invalid config.
        String requestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + nodeNames.get(0) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"clusterConfiguration\": \"{"
                + "         qwe123 "
                + "     }\"\n"
                + "  }";

        // Then.
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", requestBody));
        Problem initProblem = getProblem(initResponse);

        assertThat(initResponse.statusCode(), is(400));
        assertAll(
                () -> assertThat(initProblem.status(), is(400)),
                () -> assertThat(initProblem.title(), is("Bad Request")),
                () -> assertThat(
                        initProblem.detail(),
                        containsString("Key 'qwe123' may not be followed")
                )
        );

        // And cluster is not initialized.
        startingNodes.forEach(it -> assertThat(it, willTimeoutFast()));
    }

    @Test
    @DisplayName("Cluster is not initialized, if config has logic error")
    void initClusterWithInvalidConfig() throws Exception {
        // When POST /management/v1/cluster/init with invalid config.
        String requestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + nodeNames.get(0) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"clusterConfiguration\": \"{"
                + "         security.enabled:1 "
                + "     }\"\n"
                + "  }";

        // Then.
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", requestBody));
        Problem initProblem = getProblem(initResponse);

        assertThat(initResponse.statusCode(), is(400));
        assertAll(
                () -> assertThat(initProblem.status(), is(400)),
                () -> assertThat(initProblem.title(), is("Bad Request")),
                () -> assertThat(
                        initProblem.detail(),
                        containsString("'boolean' is expected as a type for the 'security.enabled' configuration value")
                )
        );

        // And cluster is not initialized.
        startingNodes.forEach(it -> assertThat(it, willTimeoutFast()));
    }
}
