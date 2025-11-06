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

import static io.micronaut.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static java.util.Collections.emptySet;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerRestartPartitionsTest.RESTART_ZONE_PARTITIONS_ENDPOINT;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerRestartPartitionsWithCleanupTest.RESTART_ZONE_PARTITIONS_WITH_CLEANUP_ENDPOINT;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerTest.RESET_ZONE_PARTITIONS_ENDPOINT;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
import org.apache.ignite.internal.rest.matcher.ProblemMatcher;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test that zone partitions endpoints return unsupported error when colocation is disabled.
 */
@MicronautTest
// TODO: Remove this class when colocation is enabled. https://issues.apache.org/jira/browse/IGNITE-22522
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
public class ItDisasterRecoveryColocationDisabledTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    private static final String FIRST_ZONE = "first_ZONE";

    private final ProblemMatcher colocationDisabledProblem = isProblem().withStatus(INTERNAL_SERVER_ERROR)
            .withDetail("This method is unsupported when colocation is disabled.");

    @Inject
    @Client(NODE_URL + "/management/v1/recovery/")
    HttpClient client;

    @BeforeAll
    public void setUp() {
        sql(String.format("CREATE ZONE \"%s\" storage profiles ['%s']", FIRST_ZONE, DEFAULT_AIPERSIST_PROFILE_NAME));
    }

    @Test
    public void testResetPartitions() {
        MutableHttpRequest<?> post = HttpRequest.POST(RESET_ZONE_PARTITIONS_ENDPOINT,
                new ResetZonePartitionsRequest(FIRST_ZONE, emptySet()));

        assertThrowsProblem(() -> client.toBlocking().exchange(post), colocationDisabledProblem);
    }

    @Test
    public void testRestartPartitions() {
        MutableHttpRequest<?> post = HttpRequest.POST(RESTART_ZONE_PARTITIONS_ENDPOINT,
                new RestartZonePartitionsRequest(emptySet(), FIRST_ZONE, emptySet()));

        assertThrowsProblem(() -> client.toBlocking().exchange(post), colocationDisabledProblem);
    }

    @Test
    public void testRestartPartitionsWithCleanup() {
        MutableHttpRequest<?> post = HttpRequest.POST(RESTART_ZONE_PARTITIONS_WITH_CLEANUP_ENDPOINT,
                new RestartZonePartitionsRequest(emptySet(), FIRST_ZONE, emptySet()));

        assertThrowsProblem(() -> client.toBlocking().exchange(post), colocationDisabledProblem);
    }

    @Test
    public void testGetLocalPartitions() {
        assertThrowsProblem(() -> client.toBlocking().exchange("zone/state/local/"), colocationDisabledProblem);
    }

    @Test
    public void testGetGlobalPartitions() {
        assertThrowsProblem(() -> client.toBlocking().exchange("zone/state/global/"), colocationDisabledProblem);
    }
}
