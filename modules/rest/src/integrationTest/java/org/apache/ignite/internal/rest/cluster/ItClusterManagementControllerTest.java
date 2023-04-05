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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.ClusterStateDto;
import org.apache.ignite.internal.rest.authentication.AuthProviderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Cluster management REST test.
 */
@Property(name = "micronaut.security.enabled", value = "false")
@ExtendWith(ConfigurationExtension.class)
public class ItClusterManagementControllerTest extends RestTestBase {
    @Inject
    @Client("/management/v1/cluster")
    private HttpClient client;

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

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
    void testInitEnabledAuthEmptyProviders() {
        String givenInvalidBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "    \"" + cluster.get(0).clusterService().nodeName() + "\""
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "        \"enabled\": true\n"
                + "    }\n"
                + "}";
        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        assertThat(getProblem(thrown).detail(), containsString("Providers list must not be empty"));
    }

    @Test
    void testInitEnabledAuthEmptyLogin() {
        String givenInvalidBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "    \"" + cluster.get(0).clusterService().nodeName() + "\""
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "      \"enabled\": true,\n"
                + "      \"providers\": [\n"
                + "        {\n"
                + "          \"name\": \"basic\",\n"
                + "          \"type\": \"basic\",\n"
                + "          \"password\": \"admin\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }";
        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        assertThat(getProblem(thrown).detail(), containsString("Login must not be empty"));
    }

    @Test
    void testInitEnabledAuthEmptyPassword() {
        String givenInvalidBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "    \"" + cluster.get(0).clusterService().nodeName() + "\""
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "      \"enabled\": true,\n"
                + "      \"providers\": [\n"
                + "        {\n"
                + "          \"name\": \"basic\",\n"
                + "          \"type\": \"basic\",\n"
                + "          \"login\": \"admin\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }";
        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        assertThat(getProblem(thrown).detail(), containsString("Password must not be empty"));
    }

    @Test
    void testInitEnabledAuthEmptyType() {
        String givenInvalidBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "    \"" + cluster.get(0).clusterService().nodeName() + "\""
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "      \"enabled\": true,\n"
                + "      \"providers\": [\n"
                + "        {\n"
                + "          \"name\": \"basic\",\n"
                + "          \"login\": \"admin\",\n"
                + "          \"password\": \"admin\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }";
        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        assertThat(getProblem(thrown).detail(), containsString("missing type id property"));
    }

    @Test
    void testInitEnabledAuthEmptyName() {
        String givenInvalidBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "    \"" + cluster.get(0).clusterService().nodeName() + "\""
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "      \"enabled\": true,\n"
                + "      \"providers\": [\n"
                + "        {\n"
                + "          \"type\": \"basic\",\n"
                + "          \"login\": \"admin\",\n"
                + "          \"password\": \"admin\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  }";
        // When
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(HttpRequest.POST("init", givenInvalidBody))
        );

        // Then
        assertThat(thrown.getResponse().getStatus(), is(equalTo((HttpStatus.BAD_REQUEST))));
        assertThat(getProblem(thrown).detail(), containsString("Name must not be empty"));
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
        String givenFirstRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + cluster.get(0).clusterService().nodeName() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "        \"enabled\": false\n"
                + "    }\n"
                + "}";

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
        assertThat(state.msNodes(), is(equalTo(List.of(cluster.get(0).clusterService().nodeName()))));
        assertThat(state.cmgNodes(), is(equalTo(List.of(cluster.get(0).clusterService().nodeName()))));
        assertThat(state.clusterTag().clusterName(), is(equalTo("cluster")));

        // Given second request with different node name
        String givenSecondRequestBody = "{\n"
                + "    \"metaStorageNodes\": [\n"
                + "        \"" + cluster.get(1).clusterService().nodeName() + "\"\n"
                + "    ],\n"
                + "    \"cmgNodes\": [],\n"
                + "    \"clusterName\": \"cluster\",\n"
                + "    \"authenticationConfig\": {\n"
                + "        \"enabled\": false\n"
                + "    }\n"
                + "}";

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

    @Factory
    @Bean
    @Replaces(AuthProviderFactory.class)
    public AuthProviderFactory authProviderFactory() {
        return new AuthProviderFactory(authenticationConfiguration);
    }
}
