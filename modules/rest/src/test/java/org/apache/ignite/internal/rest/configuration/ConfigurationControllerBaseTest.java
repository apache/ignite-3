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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The base test for configuration controllers.
 */
@MicronautTest
public abstract class ConfigurationControllerBaseTest {

    @Inject
    private ConfigurationPresentation<String> cfgPresentation;

    @Inject
    private ConfigurationRegistry configurationRegistry;

    abstract HttpClient client();

    @BeforeEach
    void beforeEach() throws Exception {
        var cfg = configurationRegistry.getConfiguration(TestRootConfiguration.KEY);
        cfg.change(c -> c.changeFoo("foo").changeSubCfg(subCfg -> subCfg.changeBar("bar"))).get(1, SECONDS);
        cfg.change(c -> c.changeSensitive().changePassword("password")).get(1, SECONDS);
    }

    @Test
    void testGetConfig() {
        var response = client().toBlocking().exchange("", String.class);
        var expectedBody = cfgPresentation.represent();

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(expectedBody, response.body());
        assertEquals(
                "{\"root\":{\"foo\":\"foo\",\"sensitive\":{\"password\":\"********\"},\"subCfg\":{\"bar\":\"bar\"}}}",
                response.body()
        );
    }

    @Test
    void testGetConfigByPath() {
        var response = client().toBlocking().exchange("/root.subCfg", String.class);

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(cfgPresentation.representByPath("root.subCfg"), response.body());
        assertNotNull(response.body());
    }

    @Test
    void testGetSensitiveInformationByPath() {
        var response = client().toBlocking().exchange("/root.sensitive.password", String.class);
        var expectedBody = cfgPresentation.representByPath("root.sensitive.password");

        assertEquals(HttpStatus.OK, response.status());
        assertEquals(expectedBody, response.body());
        assertThat(response.body(), matchesPattern("^\\\"[\\*]+\\\"$"));
    }

    @Test
    void testUpdateConfig() {
        String givenChangedConfig = "{root:{foo:foo,subCfg:{bar:changed}}}";

        var response = client().toBlocking().exchange(
                HttpRequest.PATCH("", givenChangedConfig).contentType(MediaType.TEXT_PLAIN)
        );
        assertEquals(response.status(), HttpStatus.OK);

        String changedConfigValue = client().toBlocking().exchange("/root.subCfg.bar", String.class).body();
        assertEquals("\"changed\"", changedConfigValue);
    }

    @Test
    void testUnrecognizedConfigPath() {
        assertThrowsProblem(
                () -> client().toBlocking().exchange("/no-such-root.some-value"),
                isProblem().withStatus(BAD_REQUEST).withDetail("Configuration value 'no-such-root' has not been found")
        );
    }

    @Test
    void testUnrecognizedConfigPathForUpdate() {
        String givenBrokenConfig = "{root:{foo:foo,subCfg:{no-such-bar:bar}}}";

        assertThrowsProblem(
                () -> client().toBlocking().exchange(HttpRequest.PATCH("", givenBrokenConfig).contentType(MediaType.TEXT_PLAIN)),
                isProblem().withStatus(BAD_REQUEST)
                        .withDetail("'root.subCfg' configuration doesn't have the 'no-such-bar' sub-configuration")
        );
    }

    @Test
    void testValidationForUpdate() {
        String givenConfigWithError = "{root:{foo:error,subCfg:{bar:bar}}}";

        assertThrowsProblem(
                () -> client().toBlocking().exchange(HttpRequest.PATCH("", givenConfigWithError).contentType(MediaType.TEXT_PLAIN)),
                isProblem().withStatus(BAD_REQUEST)
                        .withDetail("Validation did not pass for keys: [root.foo, Error word]")
                        .withInvalidParams(List.of(new InvalidParam("root.foo", "Error word")))
        );
    }
}
