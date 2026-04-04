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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Test for disaster recovery restart partitions command. */
@MicronautTest
public class ItDisasterRecoveryControllerRestartPartitionsTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String TABLE_NAME = "first_ZONE_table";

    private static final String RESTART_ZONE_PARTITIONS_ENDPOINT = "zone/partitions/restart";

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public void setUp() {
        sql(String.format("CREATE ZONE \"%s\" storage profiles ['%s']", FIRST_ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));
        sql(String.format("CREATE TABLE PUBLIC.\"%s\" (id INT PRIMARY KEY, val INT) ZONE \"%s\"", TABLE_NAME,
                FIRST_ZONE));
    }

    @Test
    public void testRestartPartitionZoneNotFound() {
        String unknownZone = "unknown_zone";

        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), unknownZone, Set.of());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Distribution zone was not found [zoneName=" + unknownZone + "]")
        );
    }

    @Test
    void testRestartPartitionsIllegalPartitionNegative() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, Set.of(0, 5, -1, -10));

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Partition ID can't be negative, found: -10")
        );
    }

    @Test
    void testRestartPartitionsPartitionsOutOfRange() {
        int partitionCount = partitionsCount(FIRST_ZONE);

        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, Set.of(partitionCount));

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
    void testRestartPartitionsNodesAreCaseSensitive() {
        Set<String> uppercaseNodeNames = nodeNames(initialNodes() - 1).stream()
                .map(String::toUpperCase)
                .collect(toSet());

        MutableHttpRequest<?> post = restartPartitionsRequest(uppercaseNodeNames, FIRST_ZONE, Set.of());

        List<Matcher<? super String>> detailMatchers = uppercaseNodeNames.stream()
                .map(Matchers::containsString)
                .collect(Collectors.toList());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail(allOf(detailMatchers))
        );
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26377")
    public void testRestartAllPartitions() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, Set.of());

        assertThat(client.toBlocking().exchange(post), hasStatus(OK));
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26377")
    public void testRestartSpecifiedPartitions() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, Set.of(0, 1));

        assertThat(client.toBlocking().exchange(post), hasStatus(OK));
    }

    @Test
    public void testRestartPartitionsByNodes() {
        Set<String> nodeNames = nodeNames(initialNodes() - 1);

        MutableHttpRequest<?> post = restartPartitionsRequest(nodeNames, FIRST_ZONE, Set.of());

        assertThat(client.toBlocking().exchange(post), hasStatus(OK));
    }

    private static Set<String> nodeNames(int count) {
        return CLUSTER.runningNodes()
                .map(Ignite::name)
                .limit(count)
                .collect(toSet());
    }

    private static MutableHttpRequest<?> restartPartitionsRequest(
            Set<String> nodeNames,
            String zoneName,
            Collection<Integer> partitionIds
    ) {
        return HttpRequest.POST(RESTART_ZONE_PARTITIONS_ENDPOINT,
                new RestartZonePartitionsRequest(nodeNames, zoneName, partitionIds));
    }
}
