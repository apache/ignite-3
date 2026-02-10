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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ConfigurationBasedSinkRegistryTest extends BaseIgniteAbstractTest {
    private static final String TEST_CHANNEL = "testChannel";
    private static final String TEST_SINK = "testSink";

    @InjectConfiguration(polymorphicExtensions = InMemoryCollectionSinkConfigurationSchema.class)
    private EventLogConfiguration cfg;

    private InMemoryCollectionSink inMemoryCollectionSink;

    private ConfigurationBasedSinkRegistry registry;

    @BeforeEach
    void setUp() {
        inMemoryCollectionSink = new InMemoryCollectionSink();
        SinkFactoryImpl defaultFactory = new SinkFactoryImpl(new EventSerializerFactory().createEventSerializer(),
                UUID::randomUUID, "default");

        registry = new ConfigurationBasedSinkRegistry(cfg, new TestSinkFactory(defaultFactory, inMemoryCollectionSink));
        registry.start();
    }

    @AfterEach
    void tearDown() {
        registry.stop();
    }

    @Test
    void noSuchSink() {
        assertNull(registry.getByName("noSuchSink"));
    }

    @Test
    void addNewConfigurationEntry() {
        // Given configuration with a sink.
        assertThat(
                cfg.sinks().change(c -> c.create(TEST_SINK, s -> s.changeChannel(TEST_CHANNEL))),
                willCompleteSuccessfully()
        );

        // Then configuration is updated.
        assertThat(registry.getByName(TEST_SINK), not(nullValue()));
        assertThat(inMemoryCollectionSink.getStopCounter(), equalTo(0));
    }

    @Test
    void removeConfigurationEntry() throws Exception {
        // Given configuration with a sink.
        assertThat(
                cfg.sinks().change(c -> c.create(TEST_SINK, s -> s.convert(InMemoryCollectionSinkChange.class)
                        .changeChannel(TEST_CHANNEL))),
                willCompleteSuccessfully()
        );

        assertThat(registry.getByName(TEST_SINK), not(nullValue()));

        // When configuration is removed.
        assertThat(cfg.sinks().change(c -> c.delete(TEST_SINK)), willCompleteSuccessfully());

        // Then sink is removed from registry .
        assertThat(registry.getByName(TEST_SINK), nullValue());
        assertThat(inMemoryCollectionSink.getStopCounter(), equalTo(1));
    }

    @Test
    void updateConfigurationEntry() throws Exception  {
        // Given configuration with a sink.
        assertThat(
                cfg.sinks().change(c -> c.create(TEST_SINK, s -> s.changeChannel("some"))),
                willCompleteSuccessfully()
        );

        assertThat(registry.getByName(TEST_SINK), not(nullValue()));

        // And sink can be found by channel.
        assertThat(registry.findAllByChannel("some"), hasSize(1));

        // When the channel is updated.
        assertThat(
                cfg.sinks().change(c -> c.update(TEST_SINK, s -> s.changeChannel(TEST_CHANNEL))),
                willCompleteSuccessfully()
        );

        // Then then the sink can not be found by previous channel.
        assertThat(registry.findAllByChannel("some"), nullValue());
        // And the sink can be found by new channel.
        assertThat(registry.findAllByChannel(TEST_CHANNEL), hasSize(1));

        // When add one more sink with the same channel.
        assertThat(
                cfg.sinks().change(c -> c.create("newSink", s -> s.changeChannel(TEST_CHANNEL))),
                willCompleteSuccessfully()
        );

        // Then the sink can be found by new channel.
        assertThat(registry.findAllByChannel(TEST_CHANNEL), hasSize(2));
    }
}
