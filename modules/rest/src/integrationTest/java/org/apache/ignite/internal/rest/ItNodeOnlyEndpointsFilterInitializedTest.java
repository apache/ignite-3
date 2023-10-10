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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import java.util.stream.Stream;
import org.apache.ignite.internal.testframework.IntegrationTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests that after cluster is initialized, all endpoints are available. */
public class ItNodeOnlyEndpointsFilterInitializedTest extends IntegrationTestBase {

    @Inject
    @Client(NODE_URL + "/management/v1")
    HttpClient client;

    private static Stream<Arguments> endpoints() {
        return Stream.of(
                Arguments.of("deployment/cluster/units"),
                Arguments.of("node/state"),
                Arguments.of("configuration/cluster"),
                Arguments.of("configuration/node"),
                Arguments.of("configuration/node/rest"),
                Arguments.of("cluster/topology/logical"),
                Arguments.of("cluster/topology/physical")
        );
    }

    @BeforeAll
    public void setup(TestInfo testInfo) {
        startNodes(testInfo);
        String metaStorageNodeName = testNodeName(testInfo, 0);
        initializeCluster(metaStorageNodeName);
    }

    @AfterAll
    public void cleanup(TestInfo testInfo) throws Exception {
        stopNodes(testInfo);
    }

    @ParameterizedTest
    @MethodSource("endpoints")
    void clusterEndpointsEnabled(String path) {
        assertDoesNotThrow(
                () -> client.toBlocking().retrieve(HttpRequest.GET(path))
        );
        System.out.println("ll");
    }
}
