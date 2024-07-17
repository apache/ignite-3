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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.api.Sink;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventLogTest {
    private static final EventUser TEST_USER = EventUser.of("testuser", "basicAuthenticator");
    private static final Event TEST_EVENT = IgniteEvents.USER_AUTHENTICATION_SUCCESS.create(TEST_USER);
    private static final String TEST_CHANNEL_NAME = "testChannel";

    private EventLog eventLog;

    private TestChannelRegistry channelRegistry;
    private TestSinkRegistry sinkRegistry;
    private ChannelFactory channelFactory;

    @BeforeEach
    void setUp() {
        channelRegistry = new TestChannelRegistry();
        sinkRegistry = new TestSinkRegistry();
        channelFactory = new ChannelFactory(sinkRegistry);
        eventLog = new EventLogImpl(channelRegistry);
    }

    @Test
    void logsEventCorrectly() {
        // Given no channels and sinks.

        // Then nothing thrown.
        assertDoesNotThrow(() -> eventLog.log(() -> TEST_EVENT));

        // When add a channel but there is no sink.
        channelRegistry.register(TEST_CHANNEL_NAME, () -> channelFactory.createChannel(
                TEST_CHANNEL_NAME, Set.of(TEST_EVENT.getType()))
        );

        // Then nothing thrown.
        assertDoesNotThrow(() -> eventLog.log(() -> TEST_EVENT));

        // When add a sink for the channel.
        List<Event> container = new ArrayList<>();
        sinkRegistry.register(TEST_CHANNEL_NAME, container::add);

        // And log event.
        eventLog.log(() -> TEST_EVENT);

        // Then event is logged.
        assertThat(container, hasItem(TEST_EVENT));

        // When log event with a type that is not supported by the channel.
        Event event = IgniteEvents.CLIENT_CONNECTION_CLOSED.create(TEST_USER);

        // Then nothing thrown.
        assertDoesNotThrow(() -> eventLog.log(() -> event));
        // And the event is not logged.
        assertThat(container, not(hasItem(event)));
    }

    private static class TestChannelRegistry implements ChannelRegistry {
        private final Map<String, Supplier<EventChannel>> channels;

        private TestChannelRegistry() {
            channels = new HashMap<>();
        }

        void register(String name, Supplier<EventChannel> channel) {
            channels.put(name, channel);
        }

        @Override
        public EventChannel getByName(String name) {
            return channels.get(name).get();
        }

        @Override
        public Set<EventChannel> findAllChannelsByEventType(String igniteEventType) {
            return channels.values().stream()
                    .map(Supplier::get)
                    .filter(channel -> channel.types().contains(igniteEventType))
                    .collect(HashSet::new, Set::add, Set::addAll);
        }
    }

    private static class TestSinkRegistry implements SinkRegistry {
        private final Map<String, Sink> sinks;

        private TestSinkRegistry() {
            sinks = new HashMap<>();
        }

        void register(String name, Sink sink) {
            sinks.put(name, sink);
        }

        @Override
        public Sink getByName(String name) {
            return sinks.get(name);
        }

        @Override
        public Set<Sink> findAllByChannel(String channel) {
            if (!sinks.containsKey(channel)) {
                return Set.of();
            }
            return Set.of(sinks.get(channel));
        }
    }
}
