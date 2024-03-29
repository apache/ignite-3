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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ConfigurationBasedSinkRegistryTest {
    private static final String TEST_CHANNEL = "testChannel";

    @InjectConfiguration
    private EventLogConfiguration cfg;

    private ConfigurationBasedSinkRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ConfigurationBasedSinkRegistry(cfg);
    }

    @Test
    void noSuchSink() {
        assertNull(registry.getByName("noSuchSink"));
    }

    @Test
    void addNewConfigurationEntry() {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeChannel(TEST_CHANNEL);
        }));

        // Then configuration is updated.
        await().until(() -> registry.getByName(TEST_CHANNEL) != null);
    }

    @Test
    void removeConfigurationEntry() {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeChannel(TEST_CHANNEL);
        }));

        // Then configuration is updated.
        await().until(() -> registry.getByName(TEST_CHANNEL) != null);

        // When configuration is removed.
        cfg.sinks().change(c -> c.delete(TEST_CHANNEL));

        // Then sink is removed from registry .
        await().until(() -> registry.getByName(TEST_CHANNEL) == null);
    }

    @Test
    void updateConfigurationEntry() {
        // Given configuration with a sink.
        cfg.sinks().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeChannel("some");
        }));

        // Then configuration is updated.
        await().until(() -> registry.getByName(TEST_CHANNEL) != null);
        // And sink can be found by channel.
        await().until(() -> registry.findAllByChannel("some").size() == 1);

        // When the channel is updated.
        cfg.sinks().change(c -> c.update(TEST_CHANNEL, s -> {
            s.changeChannel(TEST_CHANNEL);
        }));

        // Then then the sink can not be found by previous channel.
        await().until(() -> registry.findAllByChannel("some").isEmpty());
        // And the sink can be found by new channel.
        await().until(() -> registry.findAllByChannel(TEST_CHANNEL).size() == 1);

        // When add one more sink with the same channel.
        cfg.sinks().change(c -> c.create("newSink", s -> {
            s.changeChannel(TEST_CHANNEL);
        }));

        // Then the sink can be found by new channel.
        await().until(() -> registry.findAllByChannel(TEST_CHANNEL).size() == 2);
    }
}
