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

package org.apache.ignite.internal.eventlog.impl;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.mockserver.matchers.MatchType.ONLY_MATCHING_FIELDS;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;
import static org.mockserver.model.MediaType.APPLICATION_JSON;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.WebhookSinkChange;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.verify.VerificationTimes;

@ExtendWith({MockServerExtension.class, ConfigurationExtension.class})
class WebhookSinkTest extends BaseIgniteAbstractTest {
    private static ClientAndServer clientAndServer;

    private static final UUID CLUSTER_ID = UUID.randomUUID();

    @InjectConfiguration("mock{sinks.webhookSink{type=webhook,endpoint=\"http://localhost\"}}")
    private EventLogConfiguration cfg;

    @Test
    void shouldSendEventsInBatches() {
        clientAndServer
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                )
                .respond(response(null));

        WebhookSink sink = createSink(c -> c.changeBatchSize(2));

        Stream<Event> events = Stream.of(
                IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")),
                IgniteEventType.USER_AUTHENTICATION_FAILURE.create(EventUser.of("user2", "basicProvider")),
                IgniteEventType.CLIENT_CONNECTION_ESTABLISHED.create(EventUser.of("user3", "basicProvider"))
        );

        events.forEach(sink::write);

        Awaitility.await()
                .atMost(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() > 0L);

        assertThat(sink.getEvents(), hasSize(1));

        var expectedSentContent = "[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}, {\"type\" : \"USER_AUTHENTICATION_FAILURE\"}]";

        clientAndServer
                .verify(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                                .withContentType(APPLICATION_JSON)
                                .withHeader("X-SINK-CLUSTER-ID", CLUSTER_ID.toString())
                                .withHeader("X-SINK-NODE-NAME", "default")
                                .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS)),
                        VerificationTimes.exactly(1));
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-24226")
    @Test
    void shouldSendEventsByTimeout() {
        clientAndServer
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                )
                .respond(response(null));

        WebhookSink sink = createSink(c -> c.changeBatchSendFrequencyMillis(200L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        Awaitility.await()
                .atMost(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() > 0L);

        var expectedSentContent = "[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]";

        clientAndServer
                .verify(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                                .withContentType(APPLICATION_JSON)
                                .withHeader("X-SINK-CLUSTER-ID", CLUSTER_ID.toString())
                                .withHeader("X-SINK-NODE-NAME", "default")
                                .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS)),
                        VerificationTimes.exactly(1));
    }

    @Test
    void shouldSkipSendingEvents() {
        clientAndServer
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                )
                .respond(response(null));

        WebhookSink sink = createSink(c -> c.changeBatchSize(2));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        Awaitility.await()
                .timeout(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() == 0L);

        assertThat(sink.getEvents(), hasSize(1));

        clientAndServer
                .verify(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                                .withContentType(APPLICATION_JSON)
                                .withHeader("X-SINK-CLUSTER-ID", CLUSTER_ID.toString())
                                .withHeader("X-SINK-NODE-NAME", "default"),
                        VerificationTimes.never());
    }

    @Test
    void shouldRemoveEventsIfQueueIsFull() {
        var user1Event = IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider"));
        var user2Event = IgniteEventType.USER_AUTHENTICATION_FAILURE.create(EventUser.of("user2", "basicProvider"));
        var user3Event = IgniteEventType.CLIENT_CONNECTION_ESTABLISHED.create(EventUser.of("user3", "basicProvider"));

        Stream<Event> events = Stream.of(user1Event, user2Event, user3Event);

        WebhookSink sink = createSink(c -> c.changeQueueSize(2));
        events.forEach(sink::write);

        assertThat(sink.getEvents(), hasItems(user2Event, user3Event));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 502, 503, 504})
    void shouldTryToResendEvents(int statusCode) {
        clientAndServer
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                )
                .respond(response().withStatusCode(statusCode));

        WebhookSink sink = createSink(c -> c.changeBatchSize(1).changeRetryPolicy().changeInitBackoffMillis(100L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        Awaitility.await()
                .atMost(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() > 0L);

        assertThat(sink.getEvents(), hasSize(0));

        clientAndServer
                .verify(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                                .withContentType(APPLICATION_JSON)
                                .withHeader("X-SINK-CLUSTER-ID", CLUSTER_ID.toString())
                                .withHeader("X-SINK-NODE-NAME", "default")
                                .withBody(json("[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]", ONLY_MATCHING_FIELDS)),
                        VerificationTimes.atLeast(2));
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 400, 404, 301})
    void shouldIgnoreErrorOnSendEvents(int statusCode) {
        clientAndServer
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                )
                .respond(response().withStatusCode(statusCode));

        WebhookSink sink = createSink(c -> c.changeBatchSize(1).changeRetryPolicy().changeInitBackoffMillis(100L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        Awaitility.await()
                .atMost(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() > 0L);

        assertThat(sink.getEvents(), hasSize(0));

        clientAndServer
                .verify(
                        request()
                                .withMethod("POST")
                                .withPath("/api/v1/events")
                                .withContentType(APPLICATION_JSON)
                                .withHeader("X-SINK-CLUSTER-ID", CLUSTER_ID.toString())
                                .withHeader("X-SINK-NODE-NAME", "default")
                                .withBody(json("[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]", ONLY_MATCHING_FIELDS)),
                        VerificationTimes.atLeast(1));
    }

    @BeforeAll
    static void initMockServer(ClientAndServer clientAndServer) {
        WebhookSinkTest.clientAndServer = clientAndServer;
    }

    @BeforeEach
    void resetMockServer() {
        clientAndServer.reset();

        assertThat(
                cfg.sinks().get("webhookSink").change(c -> c.convert(WebhookSinkChange.class)
                        .changeEndpoint("http://localhost:" + clientAndServer.getPort() + "/api/v1/events")
                ),
                willCompleteSuccessfully()
        );
    }

    private static void mutateSinkConfiguration(EventLogConfiguration cfg, Consumer<WebhookSinkChange> consumer) {
        assertThat(
                cfg.sinks().get("webhookSink").change(c -> consumer.accept(c.convert(WebhookSinkChange.class))),
                willCompleteSuccessfully()
        );
    }

    private WebhookSink createSink(Consumer<WebhookSinkChange> consumer) {
        mutateSinkConfiguration(cfg, consumer);

        return (WebhookSink) new SinkFactoryImpl(new EventSerializerFactory().createEventSerializer(), () -> CLUSTER_ID, "default")
                .createSink(cfg.sinks().get("webhookSink").value());
    }
}
