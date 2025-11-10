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

import static io.micronaut.http.HttpRequest.GET;
import static io.micronaut.http.HttpRequest.PATCH;
import static io.micronaut.http.HttpStatus.CONFLICT;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that before cluster is initialized, only a subset of endpoints are available. */
@MicronautTest(rebuildContext = true)
public class ItClusterStateHttpServerFilterNotInitializedTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    @Client(NODE_URL + "/management/v1")
    HttpClient client;

    private static Stream<Arguments> disabledEndpoints() {
        return Stream.of(
                Arguments.of(GET("deployment/units")),
                Arguments.of(GET("cluster/state")),
                Arguments.of(GET("configuration/cluster")),
                Arguments.of(PATCH("configuration/cluster", "any.key=any-value")),
                Arguments.of(GET("cluster/topology/logical"))
        );
    }

    private static Stream<Arguments> enabledEndpoints() {
        return Stream.of(
                Arguments.of("node/state"),
                Arguments.of("configuration/node"),
                Arguments.of("configuration/node/ignite.rest"),
                Arguments.of("cluster/topology/physical")
        );
    }

    @BeforeAll
    public void setup(TestInfo testInfo) {
        // Given non-initialized cluster.
        for (int i = 0; i < super.initialNodes(); i++) {
            CLUSTER.startEmbeddedNode(i);
        }
    }

    /**
     * This method is overridden to skip cluster initialization in the base class.
     */
    @Override
    protected int initialNodes() {
        return 0;
    }

    @ParameterizedTest
    @MethodSource("disabledEndpoints")
    void clusterEndpointsDisabledWhenNotInitialized(HttpRequest<String> request) {
        assertThrowsProblem(
                () -> client.toBlocking().exchange(request),
                isProblem()
                        .withStatus(CONFLICT)
                        .withTitle("Cluster is not initialized")
                        .withDetail("Cluster is not initialized. Call /management/v1/cluster/init in order to initialize cluster.")
        );
    }

    @ParameterizedTest
    @MethodSource("enabledEndpoints")
    void nodeConfigAndClusterInitAreEnabled(String path) {
        // But node config and cluster init endpoints are enabled
        assertDoesNotThrow(
                () -> client.toBlocking().retrieve(GET(path))
        );
    }
}
