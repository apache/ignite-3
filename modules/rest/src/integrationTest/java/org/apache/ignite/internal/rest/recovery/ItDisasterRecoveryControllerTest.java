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

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.ResetPartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.util.CollectionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * Test for disaster recovery REST commands.
 */
@MicronautTest
public class ItDisasterRecoveryControllerTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String QUALIFIED_TABLE_NAME = canonicalName("PUBLIC", "first_ZONE_table");

    private static final Set<String> ZONES = Set.of(FIRST_ZONE, "second_ZONE", "third_ZONE");

    private static final Set<String> MIXED_CASE_ZONES = Set.of("mixed_first_zone", "MIXED_FIRST_ZONE", "mixed_second_zone",
            "MIXED_SECOND_ZONE");

    private static final Set<String> ZONES_CONTAINING_TABLES = new HashSet<>(CollectionUtils.concat(ZONES, MIXED_CASE_ZONES));

    private static final String EMPTY_ZONE = "empty_ZONE";

    private static final Set<String> TABLE_NAMES = ZONES_CONTAINING_TABLES.stream().map(it -> it + "_table").collect(toSet());

    private static final Set<String> STATES = Set.of("HEALTHY", "AVAILABLE");

    public static final String RESET_PARTITIONS_ENDPOINT = "/partitions/reset";

    public static final String RESET_ZONE_PARTITIONS_ENDPOINT = "zone/partitions/reset";

    private static Set<String> nodeNames;

    private static Set<Integer> tableIds;

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public static void setUp() {
        ZONES_CONTAINING_TABLES.forEach(name -> {
            sql(String.format("CREATE ZONE \"%s\" storage profiles ['%s']", name, DEFAULT_AIPERSIST_PROFILE_NAME));
            sql(String.format("CREATE TABLE PUBLIC.\"%s_table\" (id INT PRIMARY KEY, val INT) ZONE \"%1$s\"", name));
        });

        sql(String.format("CREATE ZONE \"%s\" storage profiles ['%s']", EMPTY_ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));

        CatalogManager catalogManager = unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager();

        tableIds = catalogManager.catalog(catalogManager.latestCatalogVersion()).tables().stream()
                .map(CatalogObjectDescriptor::id)
                .collect(toSet());

        nodeNames = CLUSTER.runningNodes().map(Ignite::name).collect(toSet());
    }

    @Test
    void testLocalPartitionStates() {
        String path = localStatePath();

        var response = client.toBlocking().exchange(path + "/", localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkPartitionIds(response.body(), range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toSet()), true);

        if (colocationEnabled()) {
            // When colocation enabled, empty zones (without tables) still have partitions.
            Set<String> zoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));

            checkLocalStates(response.body(), zoneNames, nodeNames);
        } else {
            checkLocalStates(response.body(), ZONES_CONTAINING_TABLES, nodeNames);
        }
    }

    @Test
    void testLocalPartitionStatesNodeNotFound() {
        String path = localStatePath();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(path + "?nodeNames=no-such-node")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Some nodes are missing: [no-such-node]"));
    }

    @Test
    void testLocalPartitionStatesZoneNotFound() {
        String path = localStatePath();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(path + "?zoneNames=no-such-zone")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Distribution zones were not found [zoneNames=[no-such-zone]]"));
    }

    @Test
    void testLocalPartitionStatesNegativePartition() {
        String path = localStatePath();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(path + "?partitionIds=0,1,-1,-10")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Partition ID can't be negative, found: -10"));
    }

    @Test
    void testLocalPartitionStatesPartitionOutOfRange() {
        String zoneName = ZONES_CONTAINING_TABLES.stream().findAny().get();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(
                        String.format("%s?partitionIds=0,4,%d&zoneNames=%s", localStatePath(), DEFAULT_PARTITION_COUNT, zoneName)
                )
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString(
                        String.format(
                                "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                                DEFAULT_PARTITION_COUNT - 1,
                                zoneName,
                                DEFAULT_PARTITION_COUNT
                        )
                ));
    }

    @Test
    @DisabledIf("org.apache.ignite.internal.lang.IgniteSystemProperties#colocationEnabled")
    void testLocalPartitionsEmptyResult() {
        String path = localStatePath();
        HttpResponse<?> response = client.toBlocking().exchange(
                path + "?zoneNames=" + EMPTY_ZONE,
                localStateResponseType()
        );

        assertEquals(HttpStatus.OK, response.status());
        assertEmptyStates(response.body(), true);
    }

    @Test
    void testLocalPartitionStatesByZones() {
        String path = localStatePath();

        String url = path + "?zoneNames=" + String.join(",", ZONES);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkLocalStates(response.body(), ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByZonesCheckCase() {
        String path = localStatePath();

        String url = path + "?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkLocalStates(response.body(), MIXED_CASE_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodes() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

        String path = localStatePath();

        String url = path + "?nodeNames=" + String.join(",", nodeNames);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        Set<String> expectedZoneNames;

        if (colocationEnabled()) {
            // When colocation enabled, empty zones (without tables) still have partitions.
            expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));

        } else {
            expectedZoneNames = ZONES_CONTAINING_TABLES;
        }

        checkLocalStates(response.body(), expectedZoneNames, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodesIsCaseSensitive() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

        String path = localStatePath();

        String url = path + "?nodeNames=" + String.join(",", nodeNames).toUpperCase();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(url)
        );

        nodeNames.forEach(nodeName -> assertThat(thrown.getMessage(), containsString(nodeName.toUpperCase())));
    }

    @Test
    void testLocalPartitionStatesByPartitions() {
        Set<String> partitionIds = Set.of("1", "2");

        String path = localStatePath();

        String url = path + "?partitionIds=" + String.join(",", partitionIds);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkPartitionIds(response.body(), partitionIds.stream().map(Integer::parseInt).collect(toSet()), true);

        Set<String> expectedZoneNames;

        if (colocationEnabled()) {
            // When colocation enabled, empty zones (without tables) still have partitions.
            expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));
        } else {
            expectedZoneNames = ZONES_CONTAINING_TABLES;
        }

        checkLocalStates(response.body(), expectedZoneNames, nodeNames);
    }

    @Test
    void testGlobalPartitionStates() {
        String path = globalStatePath();

        var response = client.toBlocking().exchange(path + "/", globalStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkPartitionIds(response.body(), range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toSet()), false);

        Set<String> expectedZoneNames;

        if (colocationEnabled()) {
            // When colocation enabled, empty zones (without tables) still have partitions.
            expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(EMPTY_ZONE, DEFAULT_ZONE_NAME)));
        } else {
            expectedZoneNames = ZONES_CONTAINING_TABLES;
        }

        checkGlobalStates(response.body(), expectedZoneNames);
    }

    @Test
    void testGlobalPartitionStatesZoneNotFound() {
        String path = globalStatePath();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(path + "?zoneNames=no-such-zone", GlobalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Distribution zones were not found [zoneNames=[no-such-zone]]"));
    }

    @Test
    void testGlobalPartitionStatesIllegalPartitionNegative() {
        String path = globalStatePath();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(path + "?partitionIds=0,1,-1,-10")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString("Partition ID can't be negative, found: -10"));
    }

    @Test
    void testGlobalPartitionStatesPartitionsOutOfRange() {
        String zoneName = ZONES_CONTAINING_TABLES.stream().findAny().get();

        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(
                        String.format("%s?partitionIds=0,4,%d&zoneNames=%s", globalStatePath(), DEFAULT_PARTITION_COUNT, zoneName),
                        GlobalPartitionStatesResponse.class
                )
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());
        assertThat(thrown.getMessage(), containsString(
                        String.format(
                                "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                                DEFAULT_PARTITION_COUNT - 1,
                                zoneName,
                                DEFAULT_PARTITION_COUNT
                        )
                ));
    }

    @Test
    @DisabledIf("org.apache.ignite.internal.lang.IgniteSystemProperties#colocationEnabled")
    void testGlobalPartitionsEmptyResult() {
        String path = globalStatePath();

        HttpResponse<?> response = client.toBlocking().exchange(path + "?zoneNames=" + EMPTY_ZONE, globalStateResponseType());

        assertEquals(HttpStatus.OK, response.status());
        assertEmptyStates(response.body(), false);
    }

    @Test
    void testGlobalPartitionStatesByZones() {
        String path = globalStatePath();

        String url = path + "?zoneNames=" + String.join(",", ZONES);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkGlobalStates(response.body(), ZONES);
    }

    @Test
    void testGlobalPartitionStatesByZonesCheckCase() {
        String path = globalStatePath();

        String url = path + "?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkGlobalStates(response.body(), MIXED_CASE_ZONES);
    }

    @Test
    void testGlobalPartitionStatesByPartitions() {
        Set<String> partitionIds = Set.of("1", "2");

        String path = globalStatePath();

        String url = path + "?partitionIds=" + String.join(",", partitionIds);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        checkPartitionIds(response.body(), partitionIds.stream().map(Integer::parseInt).collect(toSet()), false);

        Set<String> expectedZoneNames;

        if (colocationEnabled()) {
            // When colocation enabled, empty zones (without tables) still have partitions.
            expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(EMPTY_ZONE, DEFAULT_ZONE_NAME)));
        } else {
            expectedZoneNames = ZONES_CONTAINING_TABLES;
        }

        checkGlobalStates(response.body(), expectedZoneNames);
    }

    @Test
    public void testResetPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        MutableHttpRequest<?> post = resetPartitionsRequest(unknownZone, QUALIFIED_TABLE_NAME, emptySet());

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("Distribution zone was not found [zoneName=" + unknownZone + "]"));
    }

    @Test
    // TODO: remove this test when colocation is enabled https://issues.apache.org/jira/browse/IGNITE-22522
    @DisabledIf("org.apache.ignite.internal.lang.IgniteSystemProperties#colocationEnabled")
    public void testResetPartitionTableNotFound() {
        String tableName = "PUBLIC.unknown_table";

        MutableHttpRequest<?> post = resetPartitionsRequest(FIRST_ZONE, tableName, emptySet());

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("The table does not exist [name=" + tableName.toUpperCase() + "]"));
    }

    @Test
    void testResetPartitionsIllegalPartitionNegative() {
        MutableHttpRequest<?> post = resetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 5, -1, -10));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("Partition ID can't be negative, found: -10"));
    }

    @Test
    void testResetPartitionsPartitionsOutOfRange() {
        MutableHttpRequest<?> post = resetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(DEFAULT_PARTITION_COUNT));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));
        assertThat(e.getMessage(), containsString(
                String.format(
                        "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                        DEFAULT_PARTITION_COUNT - 1,
                        FIRST_ZONE,
                        DEFAULT_PARTITION_COUNT
                )
        ));
    }

    @Test
    void testLocalPartitionStatesWithUpdatedEstimatedRows() {
        insertRowToAllTables(1, 1);

        HttpResponse<?> response = client.toBlocking().exchange(localStatePath() + "/", localStateResponseType());

        assertEquals(HttpStatus.OK, response.status());

        assertThat(collectEstimatedRows(response.body()), containsInAnyOrder(0L, 1L));
    }

    private static void insertRowToAllTables(int id, int val) {
        ZONES_CONTAINING_TABLES.forEach(name -> {
            sql(String.format("INSERT INTO PUBLIC.\"%s_table\" (id, val) values (%s, %s)", name, id, val));
        });
    }

    private static void assertEmptyStates(Object body, boolean local) {
        List<?> statesCasted = castStatesResponse(body, local);

        assertEquals(0, statesCasted.size());
    }

    private static Set<Long> collectEstimatedRows(Object body) {
        if (colocationEnabled()) {
            return ((LocalZonePartitionStatesResponse) body).states()
                    .stream()
                    .map(LocalZonePartitionStateResponse::estimatedRows)
                    .collect(toSet());
        } else {
            return ((LocalPartitionStatesResponse) body).states()
                    .stream()
                    .map(LocalPartitionStateResponse::estimatedRows)
                    .collect(toSet());
        }
    }

    private static void checkLocalStates(Object body, Set<String> zoneNames, Set<String> nodes) {
        if (colocationEnabled()) {
            List<LocalZonePartitionStateResponse> statesCasted = ((LocalZonePartitionStatesResponse) body).states();

            assertFalse(statesCasted.isEmpty());

            statesCasted.forEach(state -> {
                assertThat(zoneNames, hasItem(state.zoneName()));
                assertThat(nodes, hasItem(state.nodeName()));
                assertThat(STATES, hasItem(state.state()));
            });

        } else {
            List<LocalPartitionStateResponse> statesCasted = ((LocalPartitionStatesResponse) body).states();

            assertFalse(statesCasted.isEmpty());

            statesCasted.forEach(state -> {
                assertThat(zoneNames, hasItem(state.zoneName()));
                assertThat(nodes, hasItem(state.nodeName()));
                assertThat(tableIds, hasItem(state.tableId()));
                assertEquals(DEFAULT_SCHEMA_NAME, state.schemaName());
                assertThat(TABLE_NAMES, hasItem(state.tableName()));
                assertThat(STATES, hasItem(state.state()));
            });
        }
    }

    private static void checkPartitionIds(Object body, Set<Integer> partitionIds, boolean local) {
        List<?> statesCasted = castStatesResponse(body, local);

        for (Object state : statesCasted) {
            assertTrue(partitionIds.contains(getPartitionId(state)));
        }
    }

    private static int getPartitionId(Object state) {
        if (state instanceof LocalZonePartitionStateResponse) {
            return ((LocalZonePartitionStateResponse) state).partitionId();
        } else if (state instanceof LocalPartitionStateResponse) {
            return ((LocalPartitionStateResponse) state).partitionId();
        } else if (state instanceof GlobalPartitionStateResponse) {
            return ((GlobalPartitionStateResponse) state).partitionId();
        } else if (state instanceof GlobalZonePartitionStateResponse) {
            return ((GlobalZonePartitionStateResponse) state).partitionId();
        }

        throw new IllegalArgumentException("Unexpected state type: " + state.getClass().getName());
    }

    private static void checkGlobalStates(Object body, Set<String> zoneNames) {
        if (colocationEnabled()) {
            List<GlobalZonePartitionStateResponse> statesCasted = ((GlobalZonePartitionStatesResponse) body).states();

            assertFalse(statesCasted.isEmpty());

            statesCasted.forEach(state -> {
                assertThat(zoneNames, hasItem(state.zoneName()));
                assertThat(STATES, hasItem(state.state()));
            });
        } else {
            List<GlobalPartitionStateResponse> statesCasted = ((GlobalPartitionStatesResponse) body).states();

            assertFalse(statesCasted.isEmpty());

            statesCasted.forEach(state -> {
                assertThat(zoneNames, hasItem(state.zoneName()));
                assertThat(tableIds, hasItem(state.tableId()));
                assertEquals(DEFAULT_SCHEMA_NAME, state.schemaName());
                assertThat(TABLE_NAMES, hasItem(state.tableName()));
                assertThat(STATES, hasItem(state.state()));
            });
        }
    }

    static MutableHttpRequest<?> resetPartitionsRequest(String zoneName, String tableName, Collection<Integer> partitionIds) {
        if (colocationEnabled()) {
            return HttpRequest.POST(RESET_ZONE_PARTITIONS_ENDPOINT,
                    new ResetZonePartitionsRequest(zoneName, partitionIds));
        } else {
            return HttpRequest.POST(RESET_PARTITIONS_ENDPOINT,
                    new ResetPartitionsRequest(zoneName, tableName, partitionIds));
        }
    }

    static String localStatePath() {
        return colocationEnabled() ? "zone/state/local" : "state/local";
    }

    static String globalStatePath() {
        return colocationEnabled() ? "zone/state/global" : "state/global";
    }

    private static Class<?> localStateResponseType() {
        return colocationEnabled() ? LocalZonePartitionStatesResponse.class : LocalPartitionStatesResponse.class;
    }

    private static Class<?> globalStateResponseType() {
        return colocationEnabled() ? GlobalZonePartitionStatesResponse.class : GlobalPartitionStatesResponse.class;
    }

    private static List<?> castStatesResponse(Object body, boolean local) {
        List<?> statesCasted;

        if (colocationEnabled()) {
            if (local) {
                statesCasted = ((LocalZonePartitionStatesResponse) body).states();
            } else {
                statesCasted = ((GlobalZonePartitionStatesResponse) body).states();
            }
        } else {
            if (local) {
                statesCasted = ((LocalPartitionStatesResponse) body).states();
            } else {
                statesCasted = ((GlobalPartitionStatesResponse) body).states();
            }
        }
        return statesCasted;
    }
}
