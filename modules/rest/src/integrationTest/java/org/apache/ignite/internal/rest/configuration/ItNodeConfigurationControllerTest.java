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

package org.apache.ignite.internal.rest.configuration;

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static io.micronaut.http.HttpStatus.OK;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.hasStatus;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link NodeConfigurationController}.
 */
@MicronautTest
public class ItNodeConfigurationControllerTest extends ClusterPerTestIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

    @Inject
    @Client(NODE_URL + "/management/v1/configuration/node")
    HttpClient client;

    @Test
    void testGetConfig() {
        String config = readConfig(null);

        assertThat(config, not(blankOrNullString()));
    }

    @Test
    void testGetConfigByPath() {
        String config = readConfig("ignite");

        assertThat(config, not(blankOrNullString()));
    }

    @Test
    void testUpdateConfig() {
        String givenChangedConfig = "ignite.deployment.location=deployment-location";

        updateConfig(givenChangedConfig);

        String s = readConfig("ignite.deployment.location");
        assertThat(s, is("\"deployment-location\""));
    }

    @Test
    void testUnrecognizedConfigPath() {
        assertThrowsProblem(
                () -> readConfig("ignite.no-such-path"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Configuration value 'ignite.no-such-path' has not been found")
        );
    }

    @Test
    void testUnrecognizedConfigPathForUpdate() {
        String givenBrokenConfig = "ignite.no-such-path=value";

        assertThrowsProblem(
                () -> updateConfig(givenBrokenConfig),
                isProblem().withStatus(BAD_REQUEST).withDetail("'ignite' configuration doesn't have the 'no-such-path' sub-configuration")
        );
    }

    @Test
    void testValidationForUpdate() {
        String givenConfigWithError = "ignite.rest.port=0";

        assertThrowsProblem(
                () -> updateConfig(givenConfigWithError),
                isProblem().withStatus(BAD_REQUEST)
                        .withDetail("Validation did not pass for keys: "
                                + "[ignite.rest.port, Configuration value 'ignite.rest.port' must not be less than 1024]")
                        .withInvalidParams(List.of(new InvalidParam(
                                "ignite.rest.port",
                                "Configuration value 'ignite.rest.port' must not be less than 1024"
                        )))
        );
    }

    private void updateConfig(String newConfig) {
        assertThat(
                client.toBlocking().exchange(HttpRequest.PATCH("", newConfig).contentType(MediaType.TEXT_PLAIN)),
                hasStatus(OK)
        );
    }

    private String readConfig(@Nullable String subTree) {
        HttpResponse<String> response = client.toBlocking()
                .exchange(subTree != null ? HttpRequest.GET("/" + subTree) : HttpRequest.GET(""), String.class);

        assertThat(response, hasStatus(OK));

        return response.body();
    }
}
