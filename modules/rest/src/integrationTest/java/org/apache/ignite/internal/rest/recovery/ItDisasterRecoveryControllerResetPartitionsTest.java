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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.rest.constants.HttpCode.OK;
import static org.apache.ignite.internal.rest.recovery.ItDisasterRecoveryControllerTest.RESET_PARTITIONS_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.rest.api.recovery.ResetPartitionsRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test for disaster recovery reset partitions command, positive cases. */
@MicronautTest
public class ItDisasterRecoveryControllerResetPartitionsTest extends ClusterPerClassIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + Cluster.BASE_HTTP_PORT;

    private static final String FIRST_ZONE = "first_ZONE";

    private static final String TABLE_NAME = "first_ZONE_table";

    private static final String QUALIFIED_TABLE_NAME = "PUBLIC." + TABLE_NAME;

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
    public void testResetAllPartitions() {
        MutableHttpRequest<ResetPartitionsRequest> post = HttpRequest.POST(RESET_PARTITIONS_ENDPOINT,
                new ResetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of()));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
    }

    @Test
    public void testResetSpecifiedPartitions() {
        MutableHttpRequest<ResetPartitionsRequest> post = HttpRequest.POST(RESET_PARTITIONS_ENDPOINT,
                new ResetPartitionsRequest(FIRST_ZONE, QUALIFIED_TABLE_NAME, Set.of(0)));

        HttpResponse<Void> response = client.toBlocking().exchange(post);

        assertThat(response.getStatus().getCode(), is(OK.code()));
    }
}
