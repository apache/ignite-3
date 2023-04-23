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

package org.apache.ignite.internal.cli.call.node.status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.rest.client.model.NodeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockserver.integration.ClientAndServer;

@MicronautTest(rebuildContext = true)
class NodeStatusCallTest {

    private ClientAndServer clientAndServer;

    private ObjectMapper objectMapper;

    private String url;

    @Inject
    private NodeStatusCall call;

    private String nodeName;

    private static Stream<Arguments> nodeStates() {
        return Arrays.stream(org.apache.ignite.rest.client.model.State.values()).map(Arguments::of);
    }

    @BeforeEach
    void setUp() {
        nodeName = "testnode";
        objectMapper = new ObjectMapper();

        clientAndServer = ClientAndServer.startClientAndServer(0);
        url = "http://localhost:" + clientAndServer.getPort();
    }

    @ParameterizedTest
    @MethodSource("nodeStates")
    @DisplayName("Should return node status")
    void nodeStatusStarting(org.apache.ignite.rest.client.model.State givenNodeState) {
        // Given
        nodeState(givenNodeState);

        // When call node status
        CallOutput<NodeStatus> output = call.execute(new UrlCallInput(url));

        // Then output is successful
        assertThat(output.hasError(), is(false));
        // And body has correct node name and status
        assertThat(output.body().name(), is(nodeName));
        assertThat(output.body().state().stateName(), is(equalToIgnoringCase(givenNodeState.getValue())));
    }

    private void nodeState(org.apache.ignite.rest.client.model.State state) {
        try {
            clientAndServer
                    .when(request()
                            .withMethod("GET")
                            .withPath("/management/v1/node/state"))
                    .respond(response(objectMapper.writeValueAsString(new NodeState().name(nodeName).state(state))));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
