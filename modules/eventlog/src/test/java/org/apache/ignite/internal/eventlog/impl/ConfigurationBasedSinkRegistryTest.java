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

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ConfigurationBasedSinkRegistryTest extends BaseIgniteAbstractTest {
    private static final String TEST_CHANNEL = "testChannel";
    private static final String TEST_SINK = "testSink";

    @InjectConfiguration
    private EventLogConfiguration cfg;

    private ConfigurationBasedSinkRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ConfigurationBasedSinkRegistry(cfg, new LogSinkFactory(new EventSerializerFactory().createEventSerializer()));
    }

    @Test
    void noSuchSink() {
        assertNull(registry.getByName("noSuchSink"));
    }

    @Test
    void addNewConfigurationEntry() throws Exception {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_SINK, s -> {
            s.changeChannel(TEST_CHANNEL);
        })).get();

        // Then configuration is updated.
        assertThat(registry.getByName(TEST_SINK), not(nullValue()));
    }

    @Test
    void removeConfigurationEntry() throws Exception {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_SINK, s -> {
            s.changeChannel(TEST_CHANNEL);
        })).get();

        assertThat(registry.getByName(TEST_SINK), not(nullValue()));

        // When configuration is removed.
        cfg.sinks().change(c -> c.delete(TEST_SINK)).get();

        // Then sink is removed from registry .
        assertThat(registry.getByName(TEST_SINK), nullValue());
    }

    @Test
    void updateConfigurationEntry() throws Exception  {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_SINK, s -> {
            s.changeChannel("some");
        })).get();

        assertThat(registry.getByName(TEST_SINK), not(nullValue()));

        // And sink can be found by channel.
        assertThat(registry.findAllByChannel("some"), hasSize(1));

        // When the channel is updated.
        cfg.sinks().change(c -> c.update(TEST_SINK, s -> {
            s.changeChannel(TEST_CHANNEL);
        })).get();

        // Then then the sink can not be found by previous channel.
        assertThat(registry.findAllByChannel("some"), hasSize(0));
        // And the sink can be found by new channel.
        assertThat(registry.findAllByChannel(TEST_CHANNEL), hasSize(1));

        // When add one more sink with the same channel.
        cfg.sinks().change(c -> c.create("newSink", s -> {
            s.changeChannel(TEST_CHANNEL);
        })).get();

        // Then the sink can be found by new channel.
        assertThat(registry.findAllByChannel(TEST_CHANNEL), hasSize(2));
    }
}
