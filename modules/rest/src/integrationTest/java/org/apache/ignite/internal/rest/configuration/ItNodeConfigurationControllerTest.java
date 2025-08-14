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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link NodeConfigurationController}.
 */
@MicronautTest(rebuildContext = true)
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
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> readConfig("ignite.no-such-path")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("Configuration value 'ignite.no-such-path' has not been found"));
    }

    @Test
    void testUnrecognizedConfigPathForUpdate() {
        String givenBrokenConfig = "ignite.no-such-path=value";

        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> updateConfig(givenBrokenConfig)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("'ignite' configuration doesn't have the 'no-such-path' sub-configuration"));
    }

    @Test
    void testValidationForUpdate() {
        String givenConfigWithError = "ignite.rest.port=0";

        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> updateConfig(givenConfigWithError)
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("Validation did not pass for keys: "
                + "[ignite.rest.port, Configuration value 'ignite.rest.port' must not be less than 1024]"));
        assertThat(problem.invalidParams(), hasSize(1));

        InvalidParam invalidParam = problem.invalidParams().stream().findFirst().get();
        assertEquals("ignite.rest.port", invalidParam.name());
        assertEquals("Configuration value 'ignite.rest.port' must not be less than 1024", invalidParam.reason());
    }

    private void updateConfig(String newConfig) {
        HttpResponse<Object> response = client.toBlocking().exchange(HttpRequest.PATCH("", newConfig).contentType(MediaType.TEXT_PLAIN));

        assertEquals(HttpStatus.OK, response.status());
    }

    private String readConfig(@Nullable String subTree) {
        HttpResponse<String> response = client.toBlocking()
                .exchange(subTree != null ? HttpRequest.GET("/" + subTree) : HttpRequest.GET(""), String.class);

        assertEquals(HttpStatus.OK, response.status());

        return response.body();
    }

    private Problem getProblem(HttpClientResponseException exception) {
        return exception.getResponse().getBody(Problem.class).orElseThrow();
    }
}
