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

package org.apache.ignite.internal.eventlog.event;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.ChannelFactory;
import org.apache.ignite.internal.eventlog.api.ChannelRegistry;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.EventLogImpl;
import org.apache.ignite.internal.eventlog.api.SinkRegistry;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.sink.Sink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventLogTest {
    private static final EventUser TEST_USER = EventUser.of("testuser", "basicAuthenticator");
    private static final Event TEST_EVENT = IgniteEvents.USER_AUTHENTICATED.create(TEST_USER);
    private static final String TEST_CHANNEL = "testChannel";

    @InjectConfiguration
    private EventLogConfiguration cfg;

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
    void noop() {
        // Given no channels and sinks.

        // Then nothing thrown.
        assertDoesNotThrow(() -> eventLog.log(() -> TEST_EVENT));

        // When add a channel but there is no sink.
        channelRegistry.register(TEST_CHANNEL, channelFactory.createChannel(
                TEST_CHANNEL, Set.of(IgniteEventType.USER_AUTHENTICATED))
        );

        // Then nothing thrown.
        assertDoesNotThrow(() -> eventLog.log(() -> TEST_EVENT));

        // When add a sink for the channel.
        List<Event> container = new ArrayList<>();
        sinkRegistry.register(TEST_CHANNEL, container::add);

        // And log event.
        eventLog.log(() -> TEST_EVENT);

        // Then event is logged.
        assertThat(container, hasItem(TEST_EVENT));
    }

    private static class TestChannelRegistry implements ChannelRegistry {
        private final Map<String, EventChannel> channels;

        private TestChannelRegistry() {
            channels = new HashMap<>();
        }

        void register(String name, EventChannel channel) {
            channels.put(name, channel);
        }

        @Override
        public EventChannel getByName(String name) {
            return channels.get(name);
        }

        @Override
        public Set<EventChannel> findAllChannelsByEventType(IgniteEventType igniteEventType) {
            return channels.values().stream()
                .filter(channel -> channel.types().contains(igniteEventType))
                .collect(Set::of, Set::add, Set::addAll);
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
