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
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.junit.jupiter.api.Test;

/**
 * Test for disaster recovery REST commands.
 */
@MicronautTest
public class ItDisasterRecoveryControllerTest extends ClusterPerTestIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + Cluster.BASE_HTTP_PORT;

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testLocalPartitionStates() {
        executeSql("CREATE TABLE foo (id INT PRIMARY KEY, val INT)");
        var response = client.toBlocking().exchange("/state/local/", LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        LocalPartitionStatesResponse body = response.body();
        assertEquals(DEFAULT_PARTITION_COUNT, body.states().size());

        List<Integer> partitionIds = body.states().stream().map(LocalPartitionStateResponse::partitionId).collect(toList());
        assertEquals(range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toList()), partitionIds);
    }

    @Test
    void testLocalPartitionStatesByZoneMissingZone() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/local/no-such-zone/", LocalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.NOT_FOUND, thrown.getResponse().status());
    }

    @Test
    void testLocalPartitionStatesByZone() {
        executeSql("CREATE TABLE def (id INT PRIMARY KEY, val INT)");

        executeSql("CREATE ZONE foo WITH partitions=1, storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'");
        executeSql("CREATE TABLE foo (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = 'FOO'");

        var response = client.toBlocking().exchange("/state/local/Default/", LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(DEFAULT_PARTITION_COUNT, response.body().states().size());

        response = client.toBlocking().exchange("/state/local/FOO/", LocalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        List<LocalPartitionStateResponse> states = response.body().states();
        assertEquals(1, states.size());

        LocalPartitionStateResponse state = states.get(0);
        assertEquals(0, state.partitionId());
        assertEquals("idrct_tlpsbz_0", state.nodeName());
        assertEquals("FOO", state.tableName());
        assertEquals("HEALTHY", state.state());
    }

    @Test
    void testGlobalPartitionStates() {
        executeSql("CREATE TABLE foo (id INT PRIMARY KEY, val INT)");
        var response = client.toBlocking().exchange("/state/global/", GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        GlobalPartitionStatesResponse body = response.body();
        assertEquals(DEFAULT_PARTITION_COUNT, body.states().size());

        List<Integer> partitionIds = body.states().stream().map(GlobalPartitionStateResponse::partitionId).collect(toList());
        assertEquals(range(0, DEFAULT_PARTITION_COUNT).boxed().collect(toList()), partitionIds);
    }

    @Test
    void testGlobalPartitionStatesByZoneMissingZone() {
        HttpClientResponseException thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/state/global/no-such-zone/", GlobalPartitionStatesResponse.class)
        );

        assertEquals(HttpStatus.NOT_FOUND, thrown.getResponse().status());
    }

    @Test
    void testGlobalPartitionStatesByZone() {
        executeSql("CREATE TABLE def (id INT PRIMARY KEY, val INT)");

        executeSql("CREATE ZONE foo WITH partitions=1, storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'");
        executeSql("CREATE TABLE foo (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE = 'FOO'");

        var response = client.toBlocking().exchange("/state/global/Default/", GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(DEFAULT_PARTITION_COUNT, response.body().states().size());

        response = client.toBlocking().exchange("/state/global/FOO/", GlobalPartitionStatesResponse.class);

        assertEquals(HttpStatus.OK, response.status());

        List<GlobalPartitionStateResponse> states = response.body().states();
        assertEquals(1, states.size());

        GlobalPartitionStateResponse state = states.get(0);
        assertEquals(0, state.partitionId());
        assertEquals("FOO", state.tableName());
        assertEquals("AVAILABLE", state.state());
    }
}
