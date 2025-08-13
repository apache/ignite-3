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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Implementation of the {@link EventLog} interface.
 */
public class EventLogImpl implements EventLog, IgniteComponent {
    private final ChannelRegistry channelRegistry;

    /**
     * Creates an instance of EventLogImpl.
     *
     * @param channelRegistry the channel registry.
     */
    EventLogImpl(ChannelRegistry channelRegistry) {
        this.channelRegistry = channelRegistry;
    }

    /**
     * Creates an instance of EventLogImpl that is configured via cluster configuration.
     *
     * @param cfg the configuration.
     */
    public EventLogImpl(EventLogConfiguration cfg, Supplier<UUID> clusterIdSupplier, String nodeName) {
        this(cfg, new SinkFactoryImpl(new EventSerializerFactory().createEventSerializer(), clusterIdSupplier, nodeName));
    }

    EventLogImpl(EventLogConfiguration cfg, SinkFactory sinkFactory) {
        this(new ConfigurationBasedChannelRegistry(cfg, new ConfigurationBasedSinkRegistry(cfg, sinkFactory)));
    }

    @Override
    public void log(Event event) {
        Set<EventChannel> channel = channelRegistry.findAllChannelsByEventType(event.getType());
        if (channel == null) {
            return;
        }

        channel.forEach(c -> c.log(event));
    }

    @Override
    public void log(String type, Supplier<Event> eventProvider) {
        Set<EventChannel> channels = channelRegistry.findAllChannelsByEventType(type);
        if (channels == null) {
            return;
        }

        Event event = eventProvider.get();
        if (!event.getType().equals(type)) {
            throw new IllegalArgumentException("Event type mismatch: " + event.getType() + " != " + type);
        }

        channels.forEach(c -> c.log(event));
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        channelRegistry.start();
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        channelRegistry.stop();
        return nullCompletedFuture();
    }
}
