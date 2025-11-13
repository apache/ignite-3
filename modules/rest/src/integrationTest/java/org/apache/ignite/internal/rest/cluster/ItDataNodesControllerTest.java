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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.createZoneWithInfiniteTimers;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.waitForDataNodes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.rest.api.cluster.zone.datanodes.DataNodesRecalculationRequest;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for distributed zones data nodes controller. */
@MicronautTest
public class ItDataNodesControllerTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "test_zone";

    private static final String DATA_NODES_RECALCULATION_ENDPOINT = "/zone/datanodes/recalculate";

    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    @Inject
    @Client(NODE_URL + "/management/v1/cluster")
    private HttpClient client;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    public void setup() {
        createZoneWithInfiniteTimers(unwrapIgniteImpl(node(0)), ZONE_NAME);
    }

    @Test
    public void restDataNodeRecalculationIdempotencyTest() {
        HttpResponse<String> response = doRestDataNodesRecalculationCall(ZONE_NAME);

        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));
    }

    @Test
    public void restDataNodeRecalculationAfterNewNodeAddedTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        Ignite node1 = startNode(1);

        assertTrue(waitForCondition(() -> node0.logicalTopologyService().localLogicalTopology().nodes().size() == 2, 1000));

        waitForDataNodes(node0, ZONE_NAME, Set.of(node0.name()));

        HttpResponse<String> response = doRestDataNodesRecalculationCall(ZONE_NAME);

        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        waitForDataNodes(node0, ZONE_NAME, Set.of(node0.name(), node1.name()));
    }

    @Test
    public void restDataNodeRecalculationWithUnknownZoneTest() {
        String unknownZoneName = "unknown_zone";

        HttpClientResponseException ex = assertThrows(
                HttpClientResponseException.class,
                () -> doRestDataNodesRecalculationCall(unknownZoneName)
        );

        HttpResponse<?> response = ex.getResponse();
        assertThat(response.code(), is(HttpStatus.BAD_REQUEST.getCode()));
        assertThat(response.reason(), is(HttpStatus.BAD_REQUEST.getReason()));
        assertThat(ex.getMessage(), containsString("Some distribution zones are missing: [" + unknownZoneName + "]"));
    }

    @Test
    public void restDataNodeRecalculationWithEmptyZoneNamesThatTriggersAllZonesTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        String secondZoneName = ZONE_NAME + "_2";
        createZoneWithInfiniteTimers(node0, secondZoneName);

        Ignite node1 = startNode(1);
        assertTrue(waitForCondition(() -> node0.logicalTopologyService().localLogicalTopology().nodes().size() == 2, 1000));

        Set<String> expectedOneDataNode = Set.of(node0.name());

        waitForDataNodes(node0, ZONE_NAME, expectedOneDataNode);
        waitForDataNodes(node0, secondZoneName, expectedOneDataNode);

        HttpResponse<String> response = doRestDataNodesRecalculationCall();
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        Set<String> expectedTwoDataNodes = Set.of(node0.name(), node1.name());

        waitForDataNodes(node0, ZONE_NAME, expectedTwoDataNodes);
        waitForDataNodes(node0, secondZoneName, expectedTwoDataNodes);
    }

    private HttpResponse<String> doRestDataNodesRecalculationCall(String... zoneNames) {
        return client
                .toBlocking()
                .exchange(HttpRequest.POST(
                        DATA_NODES_RECALCULATION_ENDPOINT,
                        new DataNodesRecalculationRequest(Set.of(zoneNames))
                ));
    }
}
