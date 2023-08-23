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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The base test for configuration controllers.
 */
@MicronautTest
@Property(name = "micronaut.security.enabled", value = "false")
public abstract class ConfigurationControllerBaseTest {

    private final Set<String> secretKeys = Set.of("password");

    @Inject
    private EmbeddedServer server;

    @Inject
    private ConfigurationPresentation<String> cfgPresentation;

    @Inject
    private ConfigurationRegistry configurationRegistry;

    @Inject
    private ApplicationContext context;

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
        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client().toBlocking().exchange("/no-such-root.some-value")
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("Configuration value 'no-such-root' has not been found"));
    }

    @Test
    void testUnrecognizedConfigPathForUpdate() {
        String givenBrokenConfig = "{root:{foo:foo,subCfg:{no-such-bar:bar}}}";

        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client().toBlocking().exchange(HttpRequest.PATCH("", givenBrokenConfig).contentType(MediaType.TEXT_PLAIN))
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("'root.subCfg' configuration doesn't have the 'no-such-bar' sub-configuration"));
    }

    @Test
    void testValidationForUpdate() {
        String givenConfigWithError = "{root:{foo:error,subCfg:{bar:bar}}}";

        var thrown = assertThrows(
                HttpClientResponseException.class,
                () -> client().toBlocking().exchange(HttpRequest.PATCH("", givenConfigWithError).contentType(MediaType.TEXT_PLAIN))
        );

        assertEquals(HttpStatus.BAD_REQUEST, thrown.getResponse().status());

        var problem = getProblem(thrown);
        assertEquals(400, problem.status());
        assertThat(problem.detail(), containsString("Validation did not pass for keys: [root.foo, Error word]"));
        assertThat(problem.invalidParams(), hasSize(1));

        InvalidParam invalidParam = problem.invalidParams().stream().findFirst().get();
        assertEquals("root.foo", invalidParam.name());
        assertEquals("Error word", invalidParam.reason());
    }

    private Problem getProblem(HttpClientResponseException exception) {
        return exception.getResponse().getBody(Problem.class).orElseThrow();
    }
}
