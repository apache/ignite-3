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

import static org.apache.ignite.internal.rest.matcher.ProblemHttpResponseMatcher.isProblemResponse;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.micronaut.http.HttpStatus;
import java.net.http.HttpResponse;
import org.apache.ignite.internal.rest.AbstractRestTestBase;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.junit.jupiter.api.Test;

/**
 * Cluster management REST test.
 */
public class ItClusterManagementControllerTest extends AbstractRestTestBase {
    @Test
    void testInitNoSuchNode() throws Exception {
        // Given body with nodename that does not exist
        String givenInvalidBody = "{\"metaStorageNodes\": [\"nodename\"], \"cmgNodes\": [], \"clusterName\": \"cluster\"}";

        // When
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", givenInvalidBody));

        // Then
        assertThat(initResponse, isProblemResponse()
                .withStatus(HttpStatus.BAD_REQUEST.getCode())
                .withTitle(HttpStatus.BAD_REQUEST.getReason())
                .withDetail("Node \"nodename\" is not present in the physical topology")
        );
    }

    @Test
    void testInitNoMsCmg() throws Exception {
        // Given body with just cluster name
        String givenBody = "{\"clusterName\": \"cluster\"}";

        // When
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", givenBody));

        // Then
        assertThat(initResponse.statusCode(), is(HttpStatus.OK.getCode()));
    }

    @Test
    void testInitAlreadyInitializedWithAnotherNodes() throws Exception {
        // Given cluster is not initialized
        HttpResponse<String> stateResponseBeforeInit = send(get("/management/v1/cluster/state"));

        // Then status is 409: Cluster is not initialized
        assertThat(stateResponseBeforeInit, isProblemResponse()
                .withStatus(HttpStatus.CONFLICT.getCode())
                .withTitle("Cluster is not initialized")
                .withDetail("Cluster is not initialized. Call /management/v1/cluster/init in order to initialize cluster.")
        );

        // Given cluster initialized
        String givenFirstRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + cluster.nodeName(0) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\"\n"
                + "}";

        // When
        HttpResponse<String> initResponse = send(post("/management/v1/cluster/init", givenFirstRequestBody));

        // Then
        assertThat(initResponse.statusCode(), is(HttpStatus.OK.getCode()));
        // And
        assertThat(cluster.server(0).waitForInitAsync(), willCompleteSuccessfully());

        // When get cluster state
        HttpResponse<String> stateResponse = send(get("/management/v1/cluster/state"));
        ClusterState state = objectMapper.readValue(stateResponse.body(), ClusterState.class);

        // Then cluster state is valid
        assertThat(state.msNodes(), contains(cluster.nodeName(0)));
        assertThat(state.cmgNodes(), contains(cluster.nodeName(0)));
        assertThat(state.clusterTag().clusterName(), is("cluster"));

        // Given second request with different node name
        String givenSecondRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + cluster.nodeName(1) + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\"\n"
                + "}";

        // When
        HttpResponse<String> secondInitResponse = send(post("/management/v1/cluster/init", givenSecondRequestBody));

        // Then
        assertThat(secondInitResponse, isProblemResponse()
                .withStatus(HttpStatus.INTERNAL_SERVER_ERROR.getCode())
                .withTitle(HttpStatus.INTERNAL_SERVER_ERROR.getReason())
                .withDetail(containsString("Init CMG request denied, reason: CMG node names do not match."))
        );
    }
}
