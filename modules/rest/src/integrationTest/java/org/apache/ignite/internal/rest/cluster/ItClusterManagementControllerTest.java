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

package org.apache.ignite.internal.rest.cluster;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.micronaut.http.HttpStatus;
import java.net.http.HttpResponse;
import java.util.List;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.rest.AbstractRestTestBase;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Cluster management REST test.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItClusterManagementControllerTest extends AbstractRestTestBase {
    @Test
    void testInitNoSuchNode() throws Exception {
        // Given body with nodename that does not exist
        String givenInvalidBody = "{\"metaStorageNodes\": [\"nodename\"], \"cmgNodes\": [], \"clusterName\": \"cluster\"}";

        // When
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", givenInvalidBody));
        Problem initProblem = getProblem(initResponse);

        // Then
        assertThat(initResponse.statusCode(), is(HttpStatus.BAD_REQUEST.getCode()));
        assertThat(initProblem.status(), is(HttpStatus.BAD_REQUEST.getCode()));
        assertThat(initProblem.title(), is("Bad Request"));
        assertThat(initProblem.detail(), is("Node \"nodename\" is not present in the physical topology"));
    }

    @Test
    void testInitAlreadyInitializedWithAnotherNodes() throws Exception {
        // Given cluster is not initialized
        HttpResponse<String> stateResponseBeforeInit = send(get("/management/v1/cluster/state"));
        Problem beforeInitProblem = getProblem(stateResponseBeforeInit);

        // Then status is 409: Cluster is not initialized
        assertThat(stateResponseBeforeInit.statusCode(), is(HttpStatus.CONFLICT.getCode()));
        assertThat(beforeInitProblem.title(), is("Cluster is not initialized"));
        assertThat(
                beforeInitProblem.detail(),
                is("Cluster is not initialized. Call /management/v1/cluster/init in order to initialize cluster.")
        );

        // Given cluster initialized
        String givenFirstRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + nodeNames.get(0) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\"\n"
                + "}";

        // When
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", givenFirstRequestBody));

        // Then
        assertThat(initResponse.statusCode(), is(HttpStatus.OK.getCode()));
        // And
        assertThat(nodes.get(0).igniteAsync(), willCompleteSuccessfully());

        // When get cluster state
        HttpResponse<String> stateResponse = send(get("/management/v1/cluster/state"));
        ClusterState state = objectMapper.readValue(stateResponse.body(), ClusterState.class);

        // Then cluster state is valid
        assertThat(state.msNodes(), is(List.of(nodeNames.get(0))));
        assertThat(state.cmgNodes(), is(List.of(nodeNames.get(0))));
        assertThat(state.clusterTag().clusterName(), is("cluster"));

        // Given second request with different node name
        String givenSecondRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + nodeNames.get(1) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\"\n"
                + "}";

        // When
        HttpResponse<String> secondInitResponse = send(post("/management/v1/cluster/init", givenSecondRequestBody));
        Problem secondInitProblem = getProblem(secondInitResponse);

        // Then
        assertThat(secondInitResponse.statusCode(), is(HttpStatus.INTERNAL_SERVER_ERROR.getCode()));
        assertThat(secondInitProblem.status(), is(HttpStatus.INTERNAL_SERVER_ERROR.getCode()));
    }
}
