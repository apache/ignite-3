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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.moreThanOrExactly;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(ConfigurationExtension.class)
@WireMockTest
class WebhookSinkTest extends BaseIgniteAbstractTest {
    private static final UUID CLUSTER_ID = UUID.randomUUID();

    @InjectConfiguration("mock{sinks.webhookSink{type=webhook,endpoint=\"http://localhost\"}}")
    private EventLogConfiguration cfg;

    private WebhookSink sink;

    @Test
    void shouldSendEventsInBatches() {
        stubFor(post("/api/v1/events").willReturn(ok()));

        createSink(c -> c.changeBatchSize(2));

        Stream<Event> events = Stream.of(
                IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")),
                IgniteEventType.USER_AUTHENTICATION_FAILURE.create(EventUser.of("user2", "basicProvider")),
                IgniteEventType.CLIENT_CONNECTION_ESTABLISHED.create(EventUser.of("user3", "basicProvider"))
        );

        events.forEach(sink::write);

        awaitSend();

        assertThat(sink.getEvents(), hasSize(1));

        var expectedSentContent = "[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}, {\"type\" : \"USER_AUTHENTICATION_FAILURE\"}]";

        verify(exactly(1), postRequestedFor(urlEqualTo("/api/v1/events"))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                .withHeader("X-SINK-CLUSTER-ID", equalTo(CLUSTER_ID.toString()))
                .withHeader("X-SINK-NODE-NAME", equalTo("default"))
                .withRequestBody(equalToJson(expectedSentContent, true, true))
        );
    }

    @Test
    void shouldSendEventsByTimeout() {
        stubFor(post("/api/v1/events").willReturn(ok()));

        createSink(c -> c.changeBatchSendFrequencyMillis(200L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        awaitSend();

        var expectedSentContent = "[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]";

        verify(exactly(1), postRequestedFor(urlEqualTo("/api/v1/events"))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                .withHeader("X-SINK-CLUSTER-ID", equalTo(CLUSTER_ID.toString()))
                .withHeader("X-SINK-NODE-NAME", equalTo("default"))
                .withRequestBody(equalToJson(expectedSentContent, true, true))
        );
    }

    @Test
    void shouldSkipSendingEvents() {
        stubFor(post("/api/v1/events").willReturn(ok()));

        createSink(c -> c.changeBatchSize(2));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        await()
                .during(Duration.ofMillis(500L))
                .until(() -> sink.getLastSendMillis() == 0L);

        assertThat(sink.getEvents(), hasSize(1));

        verify(exactly(0), postRequestedFor(urlEqualTo("/api/v1/events"))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                .withHeader("X-SINK-CLUSTER-ID", equalTo(CLUSTER_ID.toString()))
                .withHeader("X-SINK-NODE-NAME", equalTo("default"))
        );
    }

    @Test
    void shouldRemoveEventsIfQueueIsFull() {
        var user1Event = IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider"));
        var user2Event = IgniteEventType.USER_AUTHENTICATION_FAILURE.create(EventUser.of("user2", "basicProvider"));
        var user3Event = IgniteEventType.CLIENT_CONNECTION_ESTABLISHED.create(EventUser.of("user3", "basicProvider"));

        Stream<Event> events = Stream.of(user1Event, user2Event, user3Event);

        createSink(c -> c.changeQueueSize(2));
        events.forEach(sink::write);

        assertThat(sink.getEvents(), hasItems(user2Event, user3Event));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 502, 503, 504})
    void shouldTryToResendEvents(int statusCode) {
        stubFor(post("/api/v1/events").willReturn(aResponse().withStatus(statusCode)));

        createSink(c -> c.changeBatchSize(1).changeRetryPolicy().changeInitBackoffMillis(100L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        awaitSend();

        assertThat(sink.getEvents(), hasSize(0));

        verify(moreThanOrExactly(2), postRequestedFor(urlEqualTo("/api/v1/events"))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                .withHeader("X-SINK-CLUSTER-ID", equalTo(CLUSTER_ID.toString()))
                .withHeader("X-SINK-NODE-NAME", equalTo("default"))
                .withRequestBody(equalToJson("[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]", true, true))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 400, 404, 301})
    void shouldIgnoreErrorOnSendEvents(int statusCode) {
        stubFor(post("/api/v1/events").willReturn(aResponse().withStatus(statusCode)));

        createSink(c -> c.changeBatchSize(1).changeRetryPolicy().changeInitBackoffMillis(100L));

        sink.write(IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.of("user1", "basicProvider")));

        awaitSend();

        assertThat(sink.getEvents(), hasSize(0));

        verify(moreThanOrExactly(1), postRequestedFor(urlEqualTo("/api/v1/events"))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON))
                .withHeader("X-SINK-CLUSTER-ID", equalTo(CLUSTER_ID.toString()))
                .withHeader("X-SINK-NODE-NAME", equalTo("default"))
                .withRequestBody(equalToJson("[{\"type\" : \"USER_AUTHENTICATION_SUCCESS\"}]", true, true))
        );
    }

    @BeforeEach
    void resetMockServer(WireMockRuntimeInfo wmRuntimeInfo) {
        assertThat(
                cfg.sinks().get("webhookSink").change(c -> c.convert(WebhookSinkChange.class)
                        .changeEndpoint(wmRuntimeInfo.getHttpBaseUrl() + "/api/v1/events")
                ),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    void stopSink() {
        if (sink != null) {
            sink.stop();
            sink = null;
        }
    }

    private static void mutateSinkConfiguration(EventLogConfiguration cfg, Consumer<WebhookSinkChange> consumer) {
        assertThat(
                cfg.sinks().get("webhookSink").change(c -> consumer.accept(c.convert(WebhookSinkChange.class))),
                willCompleteSuccessfully()
        );
    }

    private void createSink(Consumer<WebhookSinkChange> consumer) {
        mutateSinkConfiguration(cfg, consumer);

        sink = (WebhookSink) new SinkFactoryImpl(new EventSerializerFactory().createEventSerializer(), () -> CLUSTER_ID, "default")
                .createSink(cfg.sinks().get("webhookSink").value());
    }

    private void awaitSend() {
        await().until(() -> sink.getLastSendMillis() > 0L);
    }
}
