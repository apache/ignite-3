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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
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

@WireMockTest
class NodeStatusCallTest {

    private ObjectMapper objectMapper;

    private String url;

    @Inject
    private NodeStatusCall call;

    private String nodeName;

    private static Stream<Arguments> nodeStates() {
        return Arrays.stream(org.apache.ignite.rest.client.model.State.values()).map(Arguments::of);
    }

    @BeforeEach
    void setUp(WireMockRuntimeInfo wmRuntimeInfo) {
        nodeName = "testnode";
        objectMapper = new ObjectMapper();

        url = wmRuntimeInfo.getHttpBaseUrl();
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
            stubFor(get("/management/v1/node/state")
                    .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                    .willReturn(ok(objectMapper.writeValueAsString(new NodeState().name(nodeName).state(state)))));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
