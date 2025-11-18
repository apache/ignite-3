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
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.assertDistributionZoneScaleTimersAreNotScheduled;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.assertScaleDownScheduledOrDone;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.assertScaleUpScheduledOrDone;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.createZoneWithInfiniteTimers;
import static org.apache.ignite.internal.distributionzones.DataNodesTestUtil.waitForDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.alterZone;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.rest.constants.HttpCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for distributed zones data nodes controller. */
@MicronautTest
public class ItDataNodesControllerTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "test_zone";

    private static final String UNKNOWN_ZONE_NAME = "test_zone_unknown";

    private static final String DATA_NODES_ENDPOINT = "/datanodes";

    private static final String DATA_NODES_RESET_ENDPOINT = DATA_NODES_ENDPOINT + "/reset";

    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    @Inject
    @Client(NODE_URL + "/management/v1/zones")
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
    public void restDataNodesResetIdempotencyTest() {
        HttpResponse<String> response = doRestDataNodesResetForZonesCall(Set.of(ZONE_NAME));

        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));
    }

    @Test
    public void restDataNodesResetAfterNewNodeAddedTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        assertDistributionZoneScaleTimersAreNotScheduled(node0, ZONE_NAME);

        int veryLongTimer = 100_000_000;
        alterZone(node0.catalogManager(), ZONE_NAME, veryLongTimer, veryLongTimer, null);

        Ignite node1 = startNode(1);
        assertLogicalTopologySizeEqualsTo(node0, 2);

        waitForDataNodes(node0, ZONE_NAME, Set.of(node0.name()));

        assertScaleUpScheduledOrDone(node0, ZONE_NAME);

        HttpResponse<String> response = doRestDataNodesResetForZonesCall(Set.of(ZONE_NAME));
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        waitForDataNodes(node0, ZONE_NAME, Set.of(node0.name(), node1.name()));

        assertDistributionZoneScaleTimersAreNotScheduled(node0, ZONE_NAME);

        stopNode(1);

        assertScaleDownScheduledOrDone(node0, ZONE_NAME);

        response = doRestDataNodesResetForZonesCall(Set.of(ZONE_NAME));
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        waitForDataNodes(node0, ZONE_NAME, Set.of(node0.name()));

        assertDistributionZoneScaleTimersAreNotScheduled(node0, ZONE_NAME);
    }

    @Test
    public void restDataNodesResetWithUnknownZonesTest() {
        Set<String> unknownZoneNames = Set.of(UNKNOWN_ZONE_NAME);

        assertZonesNotFoundExceptionThrown(unknownZoneNames, this::doRestDataNodesResetForZonesCall);
    }

    @Test
    public void restDataNodesResetWithEmptyZoneNamesThatTriggersAllZonesTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        String secondZoneName = ZONE_NAME + "_2";
        createZoneWithInfiniteTimers(node0, secondZoneName);

        Ignite node1 = startNode(1);
        assertLogicalTopologySizeEqualsTo(node0, 2);

        Set<String> expectedOneDataNode = Set.of(node0.name());

        waitForDataNodes(node0, ZONE_NAME, expectedOneDataNode);
        waitForDataNodes(node0, secondZoneName, expectedOneDataNode);

        HttpResponse<String> response = doRestDataNodesResetForZonesCall(Set.of());
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        Set<String> expectedTwoDataNodes = Set.of(node0.name(), node1.name());

        waitForDataNodes(node0, ZONE_NAME, expectedTwoDataNodes);
        waitForDataNodes(node0, secondZoneName, expectedTwoDataNodes);
    }

    @Test
    public void restDataNodesResetForZoneTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        Ignite node1 = startNode(1);
        assertLogicalTopologySizeEqualsTo(node0, 2);

        Set<String> expectedOneDataNode = Set.of(node0.name());

        waitForDataNodes(node0, ZONE_NAME, expectedOneDataNode);

        HttpResponse<String> response = doRestDataNodesResetForZoneCall(ZONE_NAME);
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));

        Set<String> expectedTwoDataNodes = Set.of(node0.name(), node1.name());

        waitForDataNodes(node0, ZONE_NAME, expectedTwoDataNodes);
    }

    @Test
    public void restDataNodesResetForUnknownZoneTest() {
        assertZoneNotFoundResponse(UNKNOWN_ZONE_NAME, this::doRestDataNodesResetForZoneCall);
    }

    @Test
    public void restGetDataNodesForZoneTest() throws InterruptedException {
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        Set<String> expectedOneDataNode = Set.of(node0.name());

        waitForDataNodes(node0, ZONE_NAME, expectedOneDataNode);

        HttpResponse<Set<String>> response = doRestGetDataNodesForZoneCall(ZONE_NAME);
        assertThat(response.getStatus().getCode(), is(HttpCode.OK.code()));
        assertThat(response.body(), is(expectedOneDataNode));
    }

    @Test
    public void restGetDataNodesForUnknownZoneTest() {
        assertZoneNotFoundResponse(UNKNOWN_ZONE_NAME, this::doRestGetDataNodesForZoneCall);
    }

    private static void assertZoneNotFoundResponse(String zoneName, Function<String, HttpResponse<?>> httpRequestAction) {
        HttpClientResponseException ex = assertThrows(
                HttpClientResponseException.class,
                () -> httpRequestAction.apply(zoneName)
        );

        HttpResponse<?> response = ex.getResponse();
        assertThat(response.code(), is(HttpStatus.BAD_REQUEST.getCode()));
        assertThat(response.reason(), is(HttpStatus.BAD_REQUEST.getReason()));
        assertThat(ex.getMessage(), containsString("Distribution zone was not found [zoneName=" + zoneName + "]"));
    }

    private static void assertZonesNotFoundExceptionThrown(
            Set<String> zoneNames,
            Function<Set<String>, HttpResponse<?>> httpRequestAction
    ) {
        HttpClientResponseException ex = assertThrows(
                HttpClientResponseException.class,
                () -> httpRequestAction.apply(zoneNames)
        );

        HttpResponse<?> response = ex.getResponse();
        assertThat(response.code(), is(HttpStatus.BAD_REQUEST.getCode()));
        assertThat(response.reason(), is(HttpStatus.BAD_REQUEST.getReason()));
        assertThat(ex.getMessage(), containsString("Distribution zones were not found [zoneNames=" + zoneNames + "]"));
    }

    private static void assertLogicalTopologySizeEqualsTo(IgniteImpl node, int expectedTopologySize) throws InterruptedException {
        LogicalTopologyService logicalTopologyService = node.logicalTopologyService();

        assertTrue(waitForCondition(() -> logicalTopologyService.localLogicalTopology().nodes().size() == expectedTopologySize, 1000));
    }

    private HttpResponse<String> doRestDataNodesResetForZonesCall(Set<String> zoneNames) {
        return client
                .toBlocking()
                .exchange(HttpRequest.POST(
                        DATA_NODES_RESET_ENDPOINT,
                        zoneNames
                ));
    }

    private HttpResponse<String> doRestDataNodesResetForZoneCall(String zoneName) {
        return client
                .toBlocking()
                .exchange(HttpRequest.POST("/" + zoneName + DATA_NODES_RESET_ENDPOINT, ""));
    }

    private HttpResponse<Set<String>> doRestGetDataNodesForZoneCall(String zoneName) {
        return client
                .toBlocking()
                .exchange(HttpRequest.GET("/" + zoneName + DATA_NODES_ENDPOINT), Argument.setOf(String.class));
    }
}
