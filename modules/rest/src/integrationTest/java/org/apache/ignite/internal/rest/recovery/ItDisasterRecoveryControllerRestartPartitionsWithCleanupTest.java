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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.partition.replicator.network.disaster.DisasterRecoveryRequestMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.DisasterRecoveryResponseMessage;
import org.apache.ignite.internal.rest.api.recovery.RestartPartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;


/** Test for disaster recovery restart partitions with cleanup command. */
@MicronautTest
public class ItDisasterRecoveryControllerRestartPartitionsWithCleanupTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_1_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private static final String NODE_2_URL = "http://localhost:" + (ClusterConfiguration.DEFAULT_BASE_HTTP_PORT + 1);

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String TABLE_NAME = "first_ZONE_table";

    private static final String QUALIFIED_TABLE_NAME = canonicalName("PUBLIC", TABLE_NAME);

    public static final String RESTART_PARTITIONS_WITH_CLEANUP_ENDPOINT = "/partitions/restartWithCleanup";

    public static final String RESTART_ZONE_PARTITIONS_WITH_CLEANUP_ENDPOINT = "zone/partitions/restartWithCleanup";

    @Inject
    @Client(NODE_1_URL + "/management/v1/recovery/")
    HttpClient client1;

    @Inject
    @Client(NODE_2_URL + "/management/v1/recovery/")
    HttpClient client2;

    @BeforeAll
    public void setUp() {
        sql(String.format(
                "CREATE ZONE \"%s\" (REPLICAS %s) storage profiles ['%s']",
                FIRST_ZONE,
                initialNodes(),
                DEFAULT_AIPERSIST_PROFILE_NAME
        ));
        sql(String.format("CREATE TABLE PUBLIC.\"%s\" (id INT PRIMARY KEY, val INT) ZONE \"%s\"", TABLE_NAME,
                FIRST_ZONE));

        sql(String.format("INSERT INTO PUBLIC.\"%s\" VALUES (1, 1)", TABLE_NAME));
    }

    @Test
    public void testRestartPartitionWithCleanupZoneNotFound() {
        String unknownZone = "unknown_zone";

        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), unknownZone, QUALIFIED_TABLE_NAME, Set.of());

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Distribution zone was not found [zoneName=" + unknownZone + "]")
        );
    }

    @Test
    // TODO: remove this test when colocation is enabled https://issues.apache.org/jira/browse/IGNITE-22522
    @DisabledIf("org.apache.ignite.internal.lang.IgniteSystemProperties#colocationEnabled")
    public void testRestartPartitionWithCleanupTableNotFound() {
        String tableName = "PUBLIC.unknown_table";

        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, tableName, Set.of());

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("The table does not exist [name=" + tableName.toUpperCase() + "]")
        );
    }

    @Test
    void testRestartPartitionsWithCleanupIllegalPartitionNegative() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 5, -1, -10));

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Partition ID can't be negative, found: -10")
        );
    }

    @Test
    void testRestartPartitionsWithCleanupPartitionsOutOfRange() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(DEFAULT_PARTITION_COUNT));

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail(String.format(
                        "Partition IDs should be in range [0, %d] for zone %s, found: %d",
                        DEFAULT_PARTITION_COUNT - 1,
                        FIRST_ZONE,
                        DEFAULT_PARTITION_COUNT
                ))
        );
    }

    @Test
    void testRestartPartitionsWithCleanupNodesAreCaseSensitive() {
        Set<String> uppercaseNodeNames = Set.of(CLUSTER.aliveNode().name().toUpperCase());

        MutableHttpRequest<?> post = restartPartitionsRequest(uppercaseNodeNames, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of());

        List<Matcher<? super String>> detailMatchers = uppercaseNodeNames.stream()
                .map(Matchers::containsString)
                .collect(Collectors.toList());

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail(allOf(detailMatchers))
        );
    }

    @Test
    public void testPartitionsWithCleanupEmptySetOfNodes() {
        MutableHttpRequest<?> post = restartPartitionsRequest(Set.of(), FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of());

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Only one node name should be specified for the operation.")
        );
    }

    @Test
    public void testRestartSpecifiedPartitionsWithCleanup() throws InterruptedException {
        awaitPartitionsToBeHealthy(FIRST_ZONE, Set.of(0, 1));

        Set<String> nodeName = Set.of(CLUSTER.nodes().get(0).name());

        MutableHttpRequest<?> post = restartPartitionsRequest(nodeName, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0, 1));

        assertThat(client1.toBlocking().exchange(post), hasStatus(OK));
    }

    @Test
    public void testRestartPartitionsWithCleanupByNodes() {
        Set<String> nodeNames = nodeNames(initialNodes());

        MutableHttpRequest<?> post = restartPartitionsRequest(nodeNames, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of());

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Only one node name should be specified for the operation.")
        );
    }

    @Test
    public void testRestartTablePartitionsWithCleanupByNodes() {
        Set<String> nodeNames = nodeNames(initialNodes() - 1);

        MutableHttpRequest<?> post = HttpRequest.POST(RESTART_PARTITIONS_WITH_CLEANUP_ENDPOINT,
                new RestartPartitionsRequest(nodeNames, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        assertThrowsProblem(
                () -> client1.toBlocking().exchange(post),
                isProblem().withStatus(BAD_REQUEST).withDetail("Only one node name should be specified for the operation.")
        );
    }

    @Test
    public void testRestartPartitionsWithCleanupAllPartitions() throws InterruptedException {
        awaitPartitionsToBeHealthy(FIRST_ZONE, Set.of());

        Set<String> nodeName = Set.of(CLUSTER.nodes().get(0).name());

        MutableHttpRequest<?> post = restartPartitionsRequest(nodeName, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of());

        assertThat(client1.toBlocking().exchange(post), hasStatus(OK));
    }

    @Test
    public void testRestartTablePartitionsWithCleanupAllPartitions() throws InterruptedException {
        awaitPartitionsToBeHealthy(FIRST_ZONE, Set.of());

        Set<String> nodeName = Set.of(CLUSTER.nodes().get(0).name());

        MutableHttpRequest<?> post = HttpRequest.POST(RESTART_PARTITIONS_WITH_CLEANUP_ENDPOINT,
                new RestartPartitionsRequest(nodeName, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        assertThat(client1.toBlocking().exchange(post), hasStatus(OK));
    }

    @Test
    public void testRestartTablePartitionsWithCleanupAllPartitionsOnDifferentNode() throws InterruptedException {
        awaitPartitionsToBeHealthy(FIRST_ZONE, Set.of());
        IgniteImpl calledNode = unwrapIgniteImpl(CLUSTER.nodes().get(1));
        IgniteImpl targetNode = unwrapIgniteImpl(CLUSTER.nodes().get(0));
        AtomicBoolean targetIsCalled = new AtomicBoolean(false);
        AtomicBoolean calledRepliedSuccessfully = new AtomicBoolean(false);

        // Record that the called node sent the request.
        calledNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof DisasterRecoveryRequestMessage) {
                targetIsCalled.set(true);
            }
            return false; // do not drop the message.
        });

        // Record that the target node received the request responded successfully.
        targetNode.dropMessages((nodeName, msg) -> {
            if (msg instanceof DisasterRecoveryResponseMessage) {
                DisasterRecoveryResponseMessage responseMessage = (DisasterRecoveryResponseMessage) msg;
                if (responseMessage.errorMessage() == null) {
                    calledRepliedSuccessfully.set(true);
                }
            }
            return false; // do not drop the message.
        });

        Set<String> nodeName = Set.of(CLUSTER.nodes().get(0).name());

        MutableHttpRequest<?> post = restartPartitionsRequest(nodeName, FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of());

        // Send the request to the second node, which should forward it to the first node.
        HttpResponse<Void> response = client2.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.getCode()));
        assertTrue(targetIsCalled.get());
        assertTrue(calledRepliedSuccessfully.get());
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
            String tableName,
            Collection<Integer> partitionIds
    ) {
        if (colocationEnabled()) {
            return HttpRequest.POST(RESTART_ZONE_PARTITIONS_WITH_CLEANUP_ENDPOINT,
                    new RestartZonePartitionsRequest(nodeNames, zoneName, partitionIds));
        } else {
            return HttpRequest.POST(RESTART_PARTITIONS_WITH_CLEANUP_ENDPOINT,
                    new RestartPartitionsRequest(nodeNames, zoneName, tableName, partitionIds));
        }
    }
}
