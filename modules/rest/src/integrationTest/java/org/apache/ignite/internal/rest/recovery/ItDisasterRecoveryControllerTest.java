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

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static io.micronaut.http.HttpStatus.OK;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.util.CollectionUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for disaster recovery REST commands.
 */
@MicronautTest
public class ItDisasterRecoveryControllerTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private static final int PARTITIONS_COUNT = 10;

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String QUALIFIED_TABLE_NAME = canonicalName("PUBLIC", "first_ZONE_table");

    private static final Set<String> ZONES = Set.of(FIRST_ZONE, "second_ZONE", "third_ZONE");

    private static final Set<String> MIXED_CASE_ZONES = Set.of("mixed_first_zone", "MIXED_FIRST_ZONE", "mixed_second_zone",
            "MIXED_SECOND_ZONE");

    private static final Set<String> ZONES_CONTAINING_TABLES = new HashSet<>(CollectionUtils.concat(ZONES, MIXED_CASE_ZONES));

    private static final String EMPTY_ZONE = "empty_ZONE";

    private static final Set<String> STATES = Set.of("HEALTHY", "AVAILABLE");

    public static final String RESET_ZONE_PARTITIONS_ENDPOINT = "zone/partitions/reset";

    private static Set<String> nodeNames;

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public static void setUp() {
        ZONES_CONTAINING_TABLES.forEach(name -> {
            sql(String.format(
                    "CREATE ZONE \"%s\" (PARTITIONS %d) storage profiles ['%s']",
                    name,
                    PARTITIONS_COUNT,
                    DEFAULT_AIPERSIST_PROFILE_NAME
            ));
            sql(String.format("CREATE TABLE PUBLIC.\"%s_table\" (id INT PRIMARY KEY, val INT) ZONE \"%1$s\"", name));
        });

        sql(String.format(
                "CREATE ZONE \"%s\" (PARTITIONS %d) storage profiles ['%s']",
                EMPTY_ZONE,
                PARTITIONS_COUNT,
                DEFAULT_AIPERSIST_PROFILE_NAME
        ));

        nodeNames = CLUSTER.runningNodes().map(Ignite::name).collect(toSet());
    }

    @Test
    void testLocalPartitionStates() {
        String path = localStatePath();

        var response = client.toBlocking().exchange(path + "/", localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkPartitionIds(response.body(), range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toSet()), true);

        // When colocation enabled, empty zones (without tables) still have partitions.
        Set<String> zoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));

        checkLocalStates(response.body(), zoneNames, nodeNames);
    }

    @Test
    void testLocalPartitionStatesNodeNotFound() {
        String path = localStatePath();

        assertThrowsProblem(
                () -> client.toBlocking().exchange(path + "?nodeNames=no-such-node"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Some nodes are missing: [no-such-node]")
        );
    }

    @Test
    void testLocalPartitionStatesZoneNotFound() {
        String path = localStatePath();

        assertThrowsProblem(
                () -> client.toBlocking().exchange(path + "?zoneNames=no-such-zone"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Distribution zones were not found [zoneNames=[no-such-zone]]")
        );
    }

    @Test
    void testLocalPartitionStatesNegativePartition() {
        String path = localStatePath();

        assertThrowsProblem(
                () -> client.toBlocking().exchange(path + "?partitionIds=0,1,-1,-10"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Partition ID can't be negative, found: -10")
        );
    }

    @Test
    void testLocalPartitionStatesPartitionOutOfRange() {
        String zoneName = ZONES_CONTAINING_TABLES.stream().findAny().get();

        int partitionCount = partitionsCount(zoneName);

        assertThrowsProblem(
                () -> client.toBlocking().exchange(
                        String.format("%s?partitionIds=0,4,%d&zoneNames=%s", localStatePath(), partitionCount, zoneName)
                ),
                isProblem().withStatus(BAD_REQUEST).withDetail(String.format(
                        "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                        partitionCount - 1,
                        zoneName,
                        partitionCount
                ))
        );
    }

    @Test
    void testLocalPartitionStatesByZones() {
        String path = localStatePath();

        String url = path + "?zoneNames=" + String.join(",", ZONES);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkLocalStates(response.body(), ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByZonesCheckCase() {
        String path = localStatePath();

        String url = path + "?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkLocalStates(response.body(), MIXED_CASE_ZONES, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodes() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

        String path = localStatePath();

        String url = path + "?nodeNames=" + String.join(",", nodeNames);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        Set<String> expectedZoneNames;

        // When colocation enabled, empty zones (without tables) still have partitions.
        expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));

        checkLocalStates(response.body(), expectedZoneNames, nodeNames);
    }

    @Test
    void testLocalPartitionStatesByNodesIsCaseSensitive() {
        Set<String> nodeNames = Set.of(CLUSTER.node(0).name(), CLUSTER.node(1).name());

        String path = localStatePath();

        String url = path + "?nodeNames=" + String.join(",", nodeNames).toUpperCase();

        List<Matcher<? super String>> detailMatchers = nodeNames.stream()
                .map(String::toUpperCase)
                .map(Matchers::containsString)
                .collect(Collectors.toList());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(url),
                isProblem().withStatus(BAD_REQUEST).withDetail(allOf(detailMatchers)));
    }

    @Test
    void testLocalPartitionStatesByPartitions() {
        Set<String> partitionIds = Set.of("1", "2");

        String path = localStatePath();

        String url = path + "?partitionIds=" + String.join(",", partitionIds);

        var response = client.toBlocking().exchange(url, localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkPartitionIds(response.body(), partitionIds.stream().map(Integer::parseInt).collect(toSet()), true);

        Set<String> expectedZoneNames;

        // When colocation enabled, empty zones (without tables) still have partitions.
        expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(DEFAULT_ZONE_NAME, EMPTY_ZONE)));

        checkLocalStates(response.body(), expectedZoneNames, nodeNames);
    }

    @Test
    void testGlobalPartitionStates() {
        String path = globalStatePath();

        var response = client.toBlocking().exchange(path + "/", globalStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkPartitionIds(response.body(), range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toSet()), false);

        Set<String> expectedZoneNames;

        // When colocation enabled, empty zones (without tables) still have partitions.
        expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(EMPTY_ZONE, DEFAULT_ZONE_NAME)));

        checkGlobalStates(response.body(), expectedZoneNames);
    }

    @Test
    void testGlobalPartitionStatesZoneNotFound() {
        String path = globalStatePath();

        assertThrowsProblem(
                () -> client.toBlocking().exchange(path + "?zoneNames=no-such-zone", GlobalZonePartitionStatesResponse.class),
                isProblem().withStatus(BAD_REQUEST).withDetail("Distribution zones were not found [zoneNames=[no-such-zone]]")
        );
    }

    @Test
    void testGlobalPartitionStatesIllegalPartitionNegative() {
        String path = globalStatePath();

        assertThrowsProblem(
                () -> client.toBlocking().exchange(path + "?partitionIds=0,1,-1,-10"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Partition ID can't be negative, found: -10")
        );
    }

    @Test
    void testGlobalPartitionStatesPartitionsOutOfRange() {
        String zoneName = ZONES_CONTAINING_TABLES.stream().findAny().get();

        int partitionCount = partitionsCount(zoneName);

        assertThrowsProblem(
                () -> client.toBlocking().exchange(
                        String.format("%s?partitionIds=0,4,%d&zoneNames=%s", globalStatePath(), partitionCount, zoneName),
                        GlobalZonePartitionStatesResponse.class
                ),
                isProblem().withStatus(BAD_REQUEST)
                        .withDetail(String.format(
                                "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                                partitionCount - 1,
                                zoneName,
                                partitionCount
                        ))
        );
    }

    @Test
    void testGlobalPartitionStatesByZones() {
        String path = globalStatePath();

        String url = path + "?zoneNames=" + String.join(",", ZONES);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkGlobalStates(response.body(), ZONES);
    }

    @Test
    void testGlobalPartitionStatesByZonesCheckCase() {
        String path = globalStatePath();

        String url = path + "?zoneNames=" + String.join(",", MIXED_CASE_ZONES);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkGlobalStates(response.body(), MIXED_CASE_ZONES);
    }

    @Test
    void testGlobalPartitionStatesByPartitions() {
        Set<String> partitionIds = Set.of("1", "2");

        String path = globalStatePath();

        String url = path + "?partitionIds=" + String.join(",", partitionIds);

        var response = client.toBlocking().exchange(url, globalStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        checkPartitionIds(response.body(), partitionIds.stream().map(Integer::parseInt).collect(toSet()), false);

        Set<String> expectedZoneNames;

        // When colocation enabled, empty zones (without tables) still have partitions.
        expectedZoneNames = new HashSet<>(CollectionUtils.concat(ZONES_CONTAINING_TABLES, Set.of(EMPTY_ZONE, DEFAULT_ZONE_NAME)));

        checkGlobalStates(response.body(), expectedZoneNames);
    }

    @Test
    public void testResetPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        MutableHttpRequest<?> post = resetPartitionsRequest(unknownZone, QUALIFIED_TABLE_NAME, emptySet());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Distribution zone was not found [zoneName=" + unknownZone + "]")
        );
    }

    @Test
    void testResetPartitionsIllegalPartitionNegative() {
        MutableHttpRequest<?> post = resetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 5, -1, -10));

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Partition ID can't be negative, found: -10")
        );
    }

    @Test
    void testResetPartitionsPartitionsOutOfRange() {
        int partitionCount = partitionsCount(FIRST_ZONE);

        MutableHttpRequest<?> post = resetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(partitionCount));

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail(String.format(
                        "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                        partitionCount - 1,
                        FIRST_ZONE,
                        partitionCount
                ))
        );
    }

    @Test
    void testLocalPartitionStatesWithUpdatedEstimatedRows() {
        insertRowToAllTables(1, 1);

        HttpResponse<?> response = client.toBlocking().exchange(localStatePath() + "/", localStateResponseType());

        assertThat(response, hasStatus(OK));
        assertThat(response.body(), notNullValue());

        assertThat(collectEstimatedRows(response.body()), containsInAnyOrder(0L, 1L));
    }

    private static void insertRowToAllTables(int id, int val) {
        ZONES_CONTAINING_TABLES.forEach(name -> {
            sql(String.format("INSERT INTO PUBLIC.\"%s_table\" (id, val) values (%s, %s)", name, id, val));
        });
    }

    private static Set<Long> collectEstimatedRows(Object body) {
        return ((LocalZonePartitionStatesResponse) body).states()
                .stream()
                .map(LocalZonePartitionStateResponse::estimatedRows)
                .collect(toSet());
    }

    private static void checkLocalStates(Object body, Set<String> zoneNames, Set<String> nodes) {
        List<LocalZonePartitionStateResponse> statesCasted = ((LocalZonePartitionStatesResponse) body).states();

        assertThat(statesCasted, is(not(empty())));

        statesCasted.forEach(state -> {
            assertThat(state.zoneName(), in(zoneNames));
            assertThat(state.nodeName(), in(nodes));
            assertThat(state.state(), in(STATES));
        });
    }

    private static void checkPartitionIds(Object body, Set<Integer> partitionIds, boolean local) {
        List<?> statesCasted = castStatesResponse(body, local);

        for (Object state : statesCasted) {
            assertThat(getPartitionId(state), in(partitionIds));
        }
    }

    private static int getPartitionId(Object state) {
        if (state instanceof LocalZonePartitionStateResponse) {
            return ((LocalZonePartitionStateResponse) state).partitionId();
        } else if (state instanceof GlobalZonePartitionStateResponse) {
            return ((GlobalZonePartitionStateResponse) state).partitionId();
        }

        throw new IllegalArgumentException("Unexpected state type: " + state.getClass().getName());
    }

    private static void checkGlobalStates(Object body, Set<String> zoneNames) {
        List<GlobalZonePartitionStateResponse> statesCasted = ((GlobalZonePartitionStatesResponse) body).states();

        assertThat(statesCasted, is(not(empty())));

        statesCasted.forEach(state -> {
            assertThat(state.zoneName(), in(zoneNames));
            assertThat(state.state(), in(STATES));
        });
    }

    static MutableHttpRequest<?> resetPartitionsRequest(String zoneName, String tableName, Collection<Integer> partitionIds) {
        return HttpRequest.POST(RESET_ZONE_PARTITIONS_ENDPOINT,
                new ResetZonePartitionsRequest(zoneName, partitionIds));
    }

    private static String localStatePath() {
        return "zone/state/local";
    }

    private static String globalStatePath() {
        return "zone/state/global";
    }

    private static Class<?> localStateResponseType() {
        return LocalZonePartitionStatesResponse.class;
    }

    private static Class<?> globalStateResponseType() {
        return GlobalZonePartitionStatesResponse.class;
    }

    private static List<?> castStatesResponse(Object body, boolean local) {
        if (local) {
            return ((LocalZonePartitionStatesResponse) body).states();
        } else {
            return ((GlobalZonePartitionStatesResponse) body).states();
        }
    }
}
