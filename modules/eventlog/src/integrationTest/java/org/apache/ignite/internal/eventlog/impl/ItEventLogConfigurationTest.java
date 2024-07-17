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

import static org.apache.ignite.internal.eventlog.impl.TestEventTypes.TEST_EVENT_TYPE_1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ItEventLogConfigurationTest extends BaseIgniteAbstractTest {
    private static final String TEST_CHANNEL_NAME = "testChannel";
    private static final String IN_MEMORY_SINK_TYPE = "inMemory";
    private static final String TEST_SINK_NAME = "testSink";

    @InjectConfiguration(polymorphicExtensions = InMemoryCollectionSinkConfigurationSchema.class)
    private EventLogConfiguration eventLogConfiguration;

    private EventLog eventLog;

    private InMemoryCollectionSink inMemoryCollectionSink;

    @BeforeEach
    void setUp() {
        inMemoryCollectionSink = new InMemoryCollectionSink();
        SinkFactory sinkFactory = new TestSinkFactory(inMemoryCollectionSink);
        eventLog = new EventLogImpl(eventLogConfiguration, sinkFactory);
    }

    @Test
    void configureChannelAndTestSink() throws Exception {
        // Given channel for EVENT_TYPE_1.
        eventLogConfiguration.change(c -> c.changeChannels().create(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEvents(TEST_EVENT_TYPE_1.name())
        )).get();

        // And in memory sink piped to the channel.
        eventLogConfiguration.change(c -> c.changeSinks().create(
                TEST_SINK_NAME,
                createTestSink -> createTestSink.convert(IN_MEMORY_SINK_TYPE).changeChannel(TEST_CHANNEL_NAME)
        )).get();

        // And in memory sink is empty.
        assertThat(inMemoryCollectionSink.events(), empty());

        // When publishing event of type EVENT_TYPE_1.
        var event = Event.builder()
                .type(TEST_EVENT_TYPE_1.name())
                .user(EventUser.system())
                .build();

        eventLog.log(() -> event);

        // Then event is written into the sink.
        assertThat(inMemoryCollectionSink.events(), contains(event));
    }

    @Test
    void configureTestSinkAndChannel() throws Exception {
        // Given in memory sink piped to the channel first. The order of configuration changes matters.
        eventLogConfiguration.change(c -> c.changeSinks().create(
                TEST_SINK_NAME,
                createTestSink -> createTestSink.convert(IN_MEMORY_SINK_TYPE).changeChannel(TEST_CHANNEL_NAME)
        )).get();

        // And channel for EVENT_TYPE_1.
        eventLogConfiguration.change(c -> c.changeChannels().create(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEvents(TEST_EVENT_TYPE_1.name())
        )).get();

        // And in memory sink is empty.
        assertThat(inMemoryCollectionSink.events(), empty());

        // When publishing event of type EVENT_TYPE_1.
        var event = Event.builder()
                .type(TEST_EVENT_TYPE_1.name())
                .user(EventUser.system())
                .build();

        eventLog.log(() -> event);

        // Then event is written into the sink.
        assertThat(inMemoryCollectionSink.events(), contains(event));
    }

    @Test
    void channelConfigurationChanges() throws Exception {
        // Given channel for EVENT_TYPE_1.
        eventLogConfiguration.change(c -> c.changeChannels().create(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEvents(TEST_EVENT_TYPE_1.name())
        )).get();

        // And in memory sink piped to the channel.
        eventLogConfiguration.change(c -> c.changeSinks().create(
                TEST_SINK_NAME,
                createTestSink -> createTestSink.convert(IN_MEMORY_SINK_TYPE).changeChannel(TEST_CHANNEL_NAME)
        )).get();

        // And in memory sink is empty.
        assertThat(inMemoryCollectionSink.events(), empty());

        // When publishing event of type EVENT_TYPE_1.
        var event = Event.builder()
                .type(TEST_EVENT_TYPE_1.name())
                .user(EventUser.system())
                .build();

        eventLog.log(() -> event);

        // Then event is written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(1));

        // When log event with the type that is not configured for test channel.
        var event2 = Event.builder()
                .type(TestEventTypes.TEST_EVENT_TYPE_2.name())
                .user(EventUser.system())
                .build();

        eventLog.log(() -> event2);

        // Then the event is not written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(1));

        // When change channel configuration to be associated with all test event types.
        eventLogConfiguration.change(c -> c.changeChannels().update(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEvents(
                        TEST_EVENT_TYPE_1.name(),
                        TestEventTypes.TEST_EVENT_TYPE_2.name()
                )
        )).get();

        // Then the previous event is not written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(1));

        // When log event2 again.
        eventLog.log(() -> event2);

        // Then the event2 is written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(2));
    }

    @Test
    void enableDisableChannel() throws Exception {
        // Given channel for EVENT_TYPE_1.
        eventLogConfiguration.change(c -> c.changeChannels().create(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEvents(TEST_EVENT_TYPE_1.name())
        )).get();

        // And in memory sink piped to the channel.
        eventLogConfiguration.change(c -> c.changeSinks().create(
                TEST_SINK_NAME,
                createTestSink -> createTestSink.convert(IN_MEMORY_SINK_TYPE).changeChannel(TEST_CHANNEL_NAME)
        )).get();

        // And in memory sink is empty.
        assertThat(inMemoryCollectionSink.events(), empty());

        // When publishing event of type EVENT_TYPE_1.
        var event = Event.builder()
                .type(TEST_EVENT_TYPE_1.name())
                .user(EventUser.system())
                .build();

        eventLog.log(() -> event);

        // Then event is written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(1));

        // When disable the channel.
        eventLogConfiguration.change(c -> c.changeChannels().update(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEnabled(false)
        )).get();

        // When log event again.
        eventLog.log(() -> event);

        // Then the event is not written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(1));

        // When enable the channel.
        eventLogConfiguration.change(c -> c.changeChannels().update(
                TEST_CHANNEL_NAME,
                channelChange -> channelChange.changeEnabled(true)
        )).get();

        // When log event again.
        eventLog.log(() -> event);

        // Then the event is written into the sink.
        assertThat(inMemoryCollectionSink.events(), hasSize(2));
    }
}
