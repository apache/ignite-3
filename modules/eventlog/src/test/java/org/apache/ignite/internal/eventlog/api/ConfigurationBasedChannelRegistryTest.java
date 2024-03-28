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

package org.apache.ignite.internal.eventlog.api;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Objects;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.event.IgniteEventType;
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
        registry = new ConfigurationBasedChannelRegistry(cfg, new ConfigurationBasedSinkRegistry(cfg));
    }

    @Test
    void noSuchChannel() {
        assertNull(registry.getByName("noSuchChannel"));
    }

    @Test
    void addNewConfigurationEntry() {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));

        // When get channel from registry.
        EventChannel channel = await().until(
                () -> registry.getByName(TEST_CHANNEL),
                Objects::nonNull
        );

        // Then it is configured correctly.
        assertThat(channel.types(), hasItem(IgniteEventType.USER_AUTHENTICATED));
    }

    @Test
    void removeConfigurationEntry() {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));
        // And registry returns not null.
        await().until(
                () -> registry.getByName(TEST_CHANNEL),
                Objects::nonNull
        );

        // When remove configuration entry.
        cfg.change(c -> c.changeChannels().delete(TEST_CHANNEL));

        // Then channel is removed from registry.
        await().until(
                () -> registry.getByName(TEST_CHANNEL) == null
        );
    }

    @Test
    void updateConfigurationEntry() {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));
        // And registry returns not null.
        EventChannel channel = await().until(
                () -> registry.getByName(TEST_CHANNEL),
                Objects::nonNull
        );

        // When update configuration entry.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name(), IgniteEventType.CONNECTION_CLOSED.name());
        }));

        // Then channel is updated in registry and types are not the same as the were before the update.
        await().until(() -> !registry.getByName(TEST_CHANNEL).types().equals(channel.types()));
    }

    @Test
    void findAllChannelsByEventType() {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));

        // Then registry returns the channel by type.
        await().untilAsserted(
                () -> assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATED).size(), is(1))
        );
        // But for another type it returns empty set.
        assertThat(registry.findAllChannelsByEventType(IgniteEventType.CONNECTION_CLOSED).size(), is(0));

        // When update configuration entry.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name(), IgniteEventType.CONNECTION_CLOSED.name());
        }));

        // Then registry returns the channel by type.
        await().untilAsserted(
                () -> assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATED).size(), is(1))
        );
        await().untilAsserted(
                () -> assertThat(registry.findAllChannelsByEventType(IgniteEventType.CONNECTION_CLOSED).size(), is(1))
        );

        // When add new channel.
        cfg.channels().change(c -> c.create("newChannel", s -> {
            s.changeEnabled(true);
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));

        // Then.
        await().untilAsserted(
                () -> assertThat(registry.findAllChannelsByEventType(IgniteEventType.USER_AUTHENTICATED).size(), is(2))
        );
        await().untilAsserted(
                () -> assertThat(registry.findAllChannelsByEventType(IgniteEventType.CONNECTION_CLOSED).size(), is(1))
        );
    }

    @Test
    void enableDisable() {
        // Given configuration with a channel.
        cfg.channels().change(c -> c.create(TEST_CHANNEL, s -> {
            s.changeEvents(IgniteEventType.USER_AUTHENTICATED.name());
        }));

        // Then can get channel from registry.
        await().until(() -> registry.getByName(TEST_CHANNEL) != null);

        // When disable channel.
        cfg.channels().change(c -> c.update(TEST_CHANNEL, s -> s.changeEnabled(false)));

        // Then channel is removed from registry.
        await().until(() -> registry.getByName(TEST_CHANNEL) == null);
    }
}
