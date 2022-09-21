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

package org.apache.ignite.internal.cluster.management.rest;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.cluster.management.MockNode;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.ClusterStateDto;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Cluster management REST test.
 */
public class ItClusterManagementControllerTest extends RestTestBase {

    @Inject
    @Client("/management/v1/cluster")
    private HttpClient client;

    @BeforeAll
    static void setUp(TestInfo testInfo) throws IOException {
        var addr1 = new NetworkAddress("localhost", PORT_BASE);
        var addr2 = new NetworkAddress("localhost", PORT_BASE + 1);

        var nodeFinder = new StaticNodeFinder(List.of(addr1, addr2));

        cluster.add(new MockNode(testInfo, addr1, nodeFinder, workDir.resolve("node0")));
        cluster.add(new MockNode(testInfo, addr2, nodeFinder, workDir.resolve("node1")));

        for (MockNode node : cluster) {
            node.start();
        }

        clusterService = cluster.get(0).clusterService();
        clusterManager = cluster.get(0).clusterManager();
    }

    @AfterAll
    static void tearDown() {
        for (MockNode node : cluster) {
            node.beforeNodeStop();
        }

        for (MockNode node : cluster) {
            node.stop();
        }
    }

    @Test
    void testControllerLoaded() {
        assertNotNull(server.getApplicationContext().getBean(ClusterManagementApi.class));
    }

    @Test
    void testInitNoSuchNode() {
        // Given body with nodename that does not exist
        String givenInvalidBody = "{\"metaStorageNodes\": [\"nodename\"], \"cmgNodes\": [], \"clusterName\": \"cluster\"}";

        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        // And
        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertEquals("Node \"nodename\" is not present in the physical topology", problem.detail());
    }

    @Test
    void testInitAlreadyInitializedWithAnotherNodes() {
        // Given cluster is not initialized
        HttpClientResponseException thrownBeforeInit = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().retrieve("state", ClusterStateDto.class));

        // Then status is 404: there is no "state"
        assertThat(thrownBeforeInit.getStatus(), is(equalTo(HttpStatus.NOT_FOUND)));
        assertThat(
                getProblem(thrownBeforeInit).detail(),
                is(equalTo("Cluster not initialized. Call /management/v1/cluster/init in order to initialize cluster"))
        );

        // Given cluster initialized
        String givenFirstRequestBody =
                "{\"metaStorageNodes\": [\"" + cluster.get(0).clusterService().localConfiguration().getName() + "\"], \"cmgNodes\": [], "
                        + "\"clusterName\": \"cluster\"}";

        // When
        HttpResponse<Object> response = client.toBlocking().exchange(HttpRequest.POST("init", givenFirstRequestBody));

        // Then
        assertThat(response.getStatus(), is(equalTo((HttpStatus.OK))));
        // And
        assertThat(cluster.get(0).startFuture(), willCompleteSuccessfully());

        // When get cluster state
        ClusterStateDto state =
                client.toBlocking().retrieve("state", ClusterStateDto.class);

        // Then cluster state is valid
        assertThat(state.msNodes(), is(equalTo(List.of(cluster.get(0).clusterService().localConfiguration().getName()))));
        assertThat(state.cmgNodes(), is(equalTo(List.of(cluster.get(0).clusterService().localConfiguration().getName()))));
        assertThat(state.clusterTag().clusterName(), is(equalTo("cluster")));

        // Given second request with different node name
        String givenSecondRequestBody =
                "{\"metaStorageNodes\": [\"" + cluster.get(1).clusterService().localConfiguration().getName() + "\"], \"cmgNodes\": [], "
                        + "\"clusterName\": \"cluster\" }";

        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenSecondRequestBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.INTERNAL_SERVER_ERROR))));
        // And
        var problem = getProblem(thrown);
        assertEquals(500, problem.status());
    }

    @Factory
    @Bean
    @Replaces(ClusterManagementRestFactory.class)
    public ClusterManagementRestFactory clusterManagementRestFactory() {
        return new ClusterManagementRestFactory(clusterService, clusterManager);
    }
}
