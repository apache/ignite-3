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
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.rest.constants.HttpCode.INTERNAL_SERVER_ERROR;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerRestartPartitionsTest.RESTART_ZONE_PARTITIONS_ENDPOINT;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerTest.RESET_ZONE_PARTITIONS_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
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

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(INTERNAL_SERVER_ERROR.code()));

        assertThat(e.getMessage(), containsString("This method is unsupported when colocation is disabled."));
    }

    @Test
    public void testRestartPartitions() {
        MutableHttpRequest<?> post = HttpRequest.POST(RESTART_ZONE_PARTITIONS_ENDPOINT,
                new RestartZonePartitionsRequest(emptySet(), FIRST_ZONE, emptySet()));

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange(post));

        assertThat(e.getResponse().code(), is(INTERNAL_SERVER_ERROR.code()));

        assertThat(e.getMessage(), containsString("This method is unsupported when colocation is disabled."));
    }

    @Test
    public void testGetLocalPartitions() {
        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange("zone/state/local/"));

        assertThat(e.getResponse().code(), is(INTERNAL_SERVER_ERROR.code()));

        assertThat(e.getMessage(), containsString("This method is unsupported when colocation is disabled."));
    }

    @Test
    public void testGetGlobalPartitions() {
        HttpClientResponseException e = assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().exchange("zone/state/global/"));

        assertThat(e.getResponse().code(), is(INTERNAL_SERVER_ERROR.code()));

        assertThat(e.getMessage(), containsString("This method is unsupported when colocation is disabled."));
    }
}
