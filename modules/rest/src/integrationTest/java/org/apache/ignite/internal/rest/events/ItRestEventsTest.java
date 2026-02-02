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

package org.apache.ignite.internal.rest.events;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.rest.events.RestEvents.FieldNames;
import org.apache.ignite.internal.testframework.log4j2.EventLogInspector;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration test for REST events.
 */
@MicronautTest(rebuildContext = true)
@Property(name = "ignite.endpoints.rest-events", value = "true")
class ItRestEventsTest extends ClusterPerTestIntegrationTest {
    private static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;
    private static final @Nullable String username = "admin";
    private static final @Nullable String password = "password";

    private EventLogInspector logInspector = new EventLogInspector();
    private boolean securityEnabled = true;

    @Inject
    @Client(NODE_URL)
    HttpClient client;

    @BeforeEach
    @Override
    public void startCluster(TestInfo testInfo) {
    }

    @BeforeEach
    void setUp() {
        logInspector.start();
    }

    @AfterEach
    void tearDown() {
        logInspector.stop();
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        String allEvents = Arrays.stream(IgniteEventType.values())
                .map(IgniteEventType::name)
                .filter(name -> name.startsWith("REST"))
                .collect(Collectors.joining(", ", "[", "]"));

        String confEvents = "ignite.eventlog {"
                + " sinks.logSink.channel: testChannel,"
                + " channels.testChannel.events: " + allEvents
                + "}";

        String confSecurity = "ignite.security.enabled=" + securityEnabled + ",\n"
                + " ignite.security.authentication.providers.default={"
                + " type=basic,"
                + " users=[{username=" + username + ",password=" + password + "}]"
                + "}";

        builder.clusterConfiguration(confEvents + "," + confSecurity);
    }

    @ParameterizedTest(name = "securityEnabled={0}")
    @ValueSource(booleans = {false, true})
    void eventsTest(boolean securityEnabled, TestInfo testInfo) throws Exception {
        this.securityEnabled = securityEnabled;

        EventUser user = securityEnabled
                ? EventUser.of(username, "basic")
                : EventUser.of("anonymous", "anonymous");

        super.startCluster(testInfo);

        // any couple endpoints are enough for this test
        for (String uri : List.of(
                "/health",
                "/management/v1/node/state"
        )) {
            MutableHttpRequest<Object> request = HttpRequest.GET(uri);
            if (securityEnabled) {
                request.header("Authorization", basicAuthenticationHeader(username, password));
            }
            assertDoesNotThrow(() -> client.toBlocking().retrieve(request));
        }

        await().until(logInspector::events, hasSize(greaterThanOrEqualTo(4)));

        assertThat(logInspector.events(), containsInRelativeOrder(
                eventEqualTo(IgniteEventType.REST_API_REQUEST_STARTED, user, Map.of(
                        FieldNames.REQUEST_ID, notNullValue(),
                        FieldNames.TIMESTAMP, notNullValue(),
                        FieldNames.METHOD, "GET",
                        FieldNames.ENDPOINT, "/health",
                        FieldNames.NODE_NAME, notNullValue()
                )),
                eventEqualTo(IgniteEventType.REST_API_REQUEST_FINISHED, user, Map.of(
                        FieldNames.REQUEST_ID, notNullValue(),
                        FieldNames.TIMESTAMP, notNullValue(),
                        FieldNames.METHOD, "GET",
                        FieldNames.ENDPOINT, "/health",
                        FieldNames.NODE_NAME, notNullValue(),
                        FieldNames.STATUS, equalTo(200),
                        FieldNames.DURATION_MS, notNullValue()
                )),
                eventEqualTo(IgniteEventType.REST_API_REQUEST_STARTED, user, Map.of(
                        FieldNames.REQUEST_ID, notNullValue(),
                        FieldNames.TIMESTAMP, notNullValue(),
                        FieldNames.METHOD, "GET",
                        FieldNames.ENDPOINT, "/management/v1/node/state",
                        FieldNames.NODE_NAME, notNullValue()
                )),
                eventEqualTo(IgniteEventType.REST_API_REQUEST_FINISHED, user, Map.of(
                        FieldNames.REQUEST_ID, notNullValue(),
                        FieldNames.TIMESTAMP, notNullValue(),
                        FieldNames.METHOD, "GET",
                        FieldNames.ENDPOINT, "/management/v1/node/state",
                        FieldNames.NODE_NAME, notNullValue(),
                        FieldNames.STATUS, equalTo(200),
                        FieldNames.DURATION_MS, notNullValue()
                ))
        ));
    }

    private static String basicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }

    private static Matcher<? super String> eventEqualTo(
            IgniteEventType type,
            @Nullable EventUser user,
            @Nullable Map<String, Object> fields
    ) {
        List<Matcher<? super String>> matchers = new ArrayList<>();

        matchers.add(hasJsonPath("$.type", equalToIgnoringCase(type.name())));

        if (user != null) {
            matchers.add(hasJsonPath("$.user.username", equalToIgnoringCase(user.username())));
            matchers.add(hasJsonPath("$.user.authenticationProvider", equalToIgnoringCase(user.authenticationProvider())));
        }

        if (fields != null) {
            for (Entry<String, Object> entry : fields.entrySet()) {
                if (entry.getValue() instanceof String) {
                    matchers.add(hasJsonPath("$.fields." + entry.getKey(), equalToIgnoringCase((String) entry.getValue())));
                } else if (entry.getValue() instanceof Matcher) {
                    matchers.add(hasJsonPath("$.fields." + entry.getKey(), (Matcher<String>) entry.getValue()));
                }

            }
        }

        return allOf(matchers);
    }
}
