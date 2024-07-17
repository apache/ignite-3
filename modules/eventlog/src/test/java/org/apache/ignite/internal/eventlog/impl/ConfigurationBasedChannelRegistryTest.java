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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class ConfigurationBasedChannelRegistryTest extends BaseIgniteAbstractTest {
    private static final String TEST_CHANNEL = "testChannel";

    @InjectConfiguration
    private EventLogConfiguration cfg;

    private ConfigurationBasedChannelRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ConfigurationBasedChannelRegistry(cfg, new ConfigurationBasedSinkRegistry(
                cfg,
                new LogSinkFactory(new EventSerializerFactory().createEventSerializer()))
        );
    }

    @Test
    void noSuchChannel() {
        assertNull(registry.getByName("noSuchChannel"));
    }

    @Test
    void addNewConfigurationEntry() throws Exception {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        // When get channel from registry.
        EventChannel channel = registry.getByName(TEST_CHANNEL);

        // Then it is configured correctly.
        assertThat(channel.types(), hasItem(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name()));
    }

    @Test
    void removeConfigurationEntry() throws Exception {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        // When remove configuration entry.
        cfg.change(c -> c.changeChannels().delete(TEST_CHANNEL)).get();

        // Then channel is removed from registry.
        assertThat(registry.getByName(TEST_CHANNEL), nullValue());
    }

    @Test
    void updateConfigurationEntry() throws Exception {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        assertThat(registry.getByName(TEST_CHANNEL).types(), hasSize(1));

        // When update configuration entry.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name(), IgniteEventType.CLIENT_CONNECTION_CLOSED.name());
        })).get();

        // Then channel is updated in registry and types are not the same as the were before the update.
        assertThat(registry.getByName(TEST_CHANNEL).types(), hasSize(2));
    }

    @Test
    void findAllChannelsByEventType() throws Exception {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        // Then registry returns the channel by type.
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name()), hasSize(1));
        // But for another type it returns empty set.
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.CLIENT_CONNECTION_CLOSED.name()), hasSize(0));

        // When update configuration entry.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name(), IgniteEventType.CLIENT_CONNECTION_CLOSED.name());
        })).get();

        // Then registry returns the channel by type.
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name()), hasSize(1));
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.CLIENT_CONNECTION_CLOSED.name()), hasSize(1));

        // When add new channel.
        cfg.channels().change(c -> c.create("newChannel", s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        // Then.
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name()), hasSize(2));
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.CLIENT_CONNECTION_CLOSED.name()), hasSize(1));
    }

    @Test
    void enableDisable() throws Exception {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEvents(IgniteEventType.USER_AUTHENTICATION_SUCCESS.name());
        })).get();

        assertThat(registry.getByName(TEST_CHANNEL), not(nullValue()));

        // When disable channel.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> s.changeEnabled(false))).get();

        // Then channel is removed from registry.
        assertThat(registry.getByName(TEST_CHANNEL), nullValue());
    }
}
