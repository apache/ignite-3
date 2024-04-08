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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.config.schema.ChannelView;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;

class ConfigurationBasedChannelRegistry implements ChannelRegistry {
    private final ReadWriteLock guard;

    private final Map<String, EventChannel> cache;

    private final Map<String, Set<EventChannel>> typeCache;

    private final SinkRegistry sinkRegistry;

    ConfigurationBasedChannelRegistry(EventLogConfiguration cfg, SinkRegistry sinkRegistry) {
        this.guard = new ReentrantReadWriteLock();
        this.cache = new HashMap<>();
        this.typeCache = new HashMap<>();
        this.sinkRegistry = sinkRegistry;

        cfg.channels().listen(new CacheUpdater());
    }

    @Override
    public EventChannel getByName(String name) {
        guard.readLock().lock();
        try {
            return cache.get(name);
        } finally {
            guard.readLock().unlock();
        }
    }

    @Override
    public Set<EventChannel> findAllChannelsByEventType(String igniteEventType) {
        guard.readLock().lock();
        try {
            Set<EventChannel> result = typeCache.get(igniteEventType);
            return result == null ? Set.of() : new HashSet<>(result);
        } finally {
            guard.readLock().unlock();
        }
    }

    private class CacheUpdater implements ConfigurationListener<NamedListView<ChannelView>> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<NamedListView<ChannelView>> ctx) {
            NamedListView<ChannelView> newListValue = ctx.newValue();

            guard.writeLock().lock();

            try {
                cache.clear();
                typeCache.clear();

                newListValue.forEach(view -> {
                    if (view.enabled()) {
                        EventChannel channel = createChannel(view);
                        cache.put(view.name(), channel);
                        for (String eventType : view.events()) {
                            typeCache.computeIfAbsent(
                                    eventType.trim(),
                                    t -> new HashSet<>()
                            ).add(channel);
                        }
                    }
                });

                return completedFuture(null);
            } finally {
                guard.writeLock().unlock();
            }
        }

        private EventChannel createChannel(ChannelView view) {
            Set<String> types = Arrays.stream(view.events())
                    .map(String::trim)
                    .collect(Collectors.toSet());

            return new EventChannelImpl(view.name(), types, sinkRegistry);
        }
    }
}
