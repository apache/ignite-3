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

package org.apache.ignite.internal.rest.recovery;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.apache.ignite.internal.util.CollectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for disaster recovery REST commands.
 */
@MicronautTest
public class ItDisasterRecoveryControllerTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + Cluster.BASE_HTTP_PORT;

    private static final Set<String> FILLED_ZONES = Set.of("first_ZONE", "second_ZONE", "third_ZONE");

    private static final Set<String> MIXED_CASE_ZONES = Set.of("mixed_first_zone", "MIXED_FIRST_ZONE", "mixed_second_zone",
            "MIXED_SECOND_ZONE");

    private static final Set<String> ALL_ZONES = new HashSet<>(CollectionUtils.concat(FILLED_ZONES, MIXED_CASE_ZONES));

    private static final String EMPTY_ZONE = "empty_ZONE";

    private static final Set<String> TABLE_NAMES = ALL_ZONES.stream().map(it -> it + "_table").collect(toSet());

    private static final Set<String> STATES = Set.of("HEALTHY", "AVAILABLE");

    private static Set<String> nodeNames;

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public static void setUp() {
        ALL_ZONES.forEach(name -> {
            sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", name, DEFAULT_AIPERSIST_PROFILE_NAME));
            sql("CREATE TABLE \"" + name + "_table\" (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = '" + name + "'");
        });

        sql("CREATE ZONE \"" + EMPTY_ZONE + "\" WITH storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'");

        nodeNames = CLUSTER.runningNodes().map(IgniteImpl::name).collect(toSet());
    }

    @Test
    void testLocalPartitionStates() {
        var response = client.toBlocking().exchange("/state/local/", LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        List<LocalPartitionStateResponse> states = response.body().states();

        assertFalse(states.isEmpty());

        List<Integer> partitionIds = states.stream().map(LocalPartitionStateResponse::partitionId).distinct().collect(toList());
        assertEquals(range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toList()), partitionIds);

        checkLocalStates(states, ALL_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesNodeNotFound() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/local?nodeNames=no-such-node", LocalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Some nodes are missing: [no-such-node]"));
    }

    @Test
    void testLocalPartitionStatesZoneNotFound() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/local?zoneNames=no-such-zone", LocalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Some distribution zones are missing: [no-such-zone]"));
    }

    @Test
    void testLocalPartitionStatesPartitionNotFound() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/local?partitionIds=-1", LocalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
    }

    @Test
    void testLocalPartitionsEmptyResult() {
        HttpResponse<LocalPartitionStatesResponse> response = client.toBlocking().exchange(
                "/state/local?zoneNames=" + EMPTY_ZONE,
                LocalPartitionStatesResponse.class
        );

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(0, response.body().states().size());
    }

    @Test
    void testLocalPartitionStatesByZones() {
        String url = "state/local?zoneNames=" + String.join(",", FILLED_ZONES);

        var response = client.toBlocking().exchange(url, LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        checkLocalStates(response.body().states(), FILLED_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByZonesCheckCase() {
        String url = "state/local?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        checkLocalStates(response.body().states(), MIXED_CASE_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodes() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).node().name(), CLUSTER.node(1).node().name());

        String url = "state/local?nodeNames=" + String.join(",", nodeNames);

        var response = client.toBlocking().exchange(url, LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        checkLocalStates(response.body().states(), ALL_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodesIsCaseSensitive() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).node().name(), CLUSTER.node(1).node().name());

        String url = "state/local?nodeNames=" + String.join(",", nodeNames).toUpperCase();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(url, LocalPartitionStatesResponse.class)
        );

        nodeNames.forEach(nodeName -> assertThat(thrown.getMessage(), containsString(nodeName.toUpperCase())));
    }

    @Test
    void testLocalPartitionStatesByPartitions() {
        Set<String> partitionIds = Set.of("1", "2");

        String url = "state/local?partitionIds=" + String.join(",", partitionIds);

        var response = client.toBlocking().exchange(url, LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        List<LocalPartitionStateResponse> states = response.body().states();

        for (LocalPartitionStateResponse state : states) {
            assertTrue(partitionIds.contains((String.valueOf(state.partitionId()))));
        }

        checkLocalStates(states, ALL_ZONES, nodeNames);
    }

    @Test
    void testGlobalPartitionStates() {
        var response = client.toBlocking().exchange("/state/global/", GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        List<GlobalPartitionStateResponse> states = response.body().states();
        assertFalse(response.body().states().isEmpty());

        List<Integer> partitionIds = states.stream().map(GlobalPartitionStateResponse::partitionId).distinct().collect(toList());
        assertEquals(range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toList()), partitionIds);

        checkGlobalStates(states, ALL_ZONES);
    }

    @Test
    void testGlobalPartitionStatesZoneNotFound() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/global?zoneNames=no-such-zone", GlobalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Some distribution zones are missing: [no-such-zone]"));
    }

    @Test
    void testGlobalPartitionStatesPartitionNotFound() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/global?partitionIds=-1", GlobalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Some partitions are missing: [-1]"));
    }

    @Test
    void testGlobalPartitionsEmptyResult() {
        HttpResponse<GlobalPartitionStatesResponse> response = client.toBlocking().exchange(
                "/state/global?zoneNames=" + EMPTY_ZONE,
                GlobalPartitionStatesResponse.class
        );

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(0, response.body().states().size());
    }

    @Test
    void testGlobalPartitionStatesByZones() {
        String url = "state/global?zoneNames=" + String.join(",", FILLED_ZONES);

        var response = client.toBlocking().exchange(url, GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        checkGlobalStates(response.body().states(), FILLED_ZONES);
    }

    @Test
    void testGlobalPartitionStatesByZonesCheckCase() {
        String url = "state/global?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        checkGlobalStates(response.body().states(), MIXED_CASE_ZONES);
    }

    private static void checkLocalStates(List<LocalPartitionStateResponse> states, Set<String> zoneNames, Set<String> nodes) {
        assertFalse(states.isEmpty());

        states.forEach(state -> {
            assertThat(zoneNames, hasItem(state.zoneName()));
            assertThat(nodes, hasItem(state.nodeName()));
            assertThat(TABLE_NAMES, hasItem(state.tableName()));
            assertThat(STATES, hasItem(state.state()));
        });
    }

    private static void checkGlobalStates(List<GlobalPartitionStateResponse> states, Set<String> zoneNames) {
        assertFalse(states.isEmpty());

        states.forEach(state -> {
            assertThat(zoneNames, hasItem(state.zoneName()));
            assertThat(TABLE_NAMES, hasItem(state.tableName()));
            assertThat(STATES, hasItem(state.state()));
        });
    }
}
