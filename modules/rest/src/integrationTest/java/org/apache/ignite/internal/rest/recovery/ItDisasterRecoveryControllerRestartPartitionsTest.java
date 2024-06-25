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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.rest.api.recovery.RestartPartitionsRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test for disaster recovery restart partitions command. */
@MicronautTest
public class ItDisasterRecoveryControllerRestartPartitionsTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + Cluster.BASE_HTTP_PORT;

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String TABLE_NAME = "first_ZONE_table";

    private static final String QUALIFIED_TABLE_NAME = "PUBLIC." + TABLE_NAME;

    public static final String RESTART_PARTITIONS_ENDPOINT = "/partitions/restart";

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public void setUp() {
        sql(String.format("CREATE ZONE \"%s\" WITH storage_profiles='%s'", FIRST_ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));
        sql(String.format("CREATE TABLE PUBLIC.\"%s\" (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = '%s'", TABLE_NAME,
                FIRST_ZONE));
    }

    @Test
    public void testRestartPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), unknownZone, QUALIFIED_TABLE_NAME, Set.of()));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("Distribution zone was not found [zoneName=" + unknownZone + "]"));
    }

    @Test
    public void testRestartPartitionTableNotFound() {
        String tableName = "unknown_table";

        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), FIRST_ZONE, tableName, Set.of()));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("The table does not exist [name=" + tableName + "]"));
    }

    @Test
    void testRestartPartitionsIllegalPartitionNegative() {
        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 5, -1, -10)));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(BAD_REQUEST.code()));

        assertThat(e.getMessage(), containsString("Partition ID can't be negative, found: -10"));
    }

    @Test
    void testRestartPartitionsPartitionsOutOfRange() {
        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(DEFAULT_PARTITION_COUNT)));

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
    void testRestartPartitionsNodesAreCaseSensitive() {
        Set<String> uppercaseNodeNames = nodeNames(initialNodes() - 1).stream()
                .map(String::toUpperCase)
                .collect(toSet());

        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(uppercaseNodeNames, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getStatus(), equalTo(HttpStatus.BAD_REQUEST));
        uppercaseNodeNames.forEach(nodeName -> assertThat(e.getMessage(), containsString(nodeName)));
    }

    @Test
    public void testRestartAllPartitions() {
        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
    }

    @Test
    public void testRestartSpecifiedPartitions() {
        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 1)));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
    }

    @Test
    public void testRestartPartitionsByNodes() {
        Set<String> nodeNames = nodeNames(initialNodes() - 1);

        MutableHttpRequest<RestartPartitionsRequest> post = HttpRequest.POST(RESTART_PARTITIONS_ENDPOINT,
                new RestartPartitionsRequest(nodeNames, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
    }

    private static Set<String> nodeNames(int count) {
        return CLUSTER.runningNodes()
                .map(IgniteImpl::name)
                .limit(count)
                .collect(toSet());
    }
}
