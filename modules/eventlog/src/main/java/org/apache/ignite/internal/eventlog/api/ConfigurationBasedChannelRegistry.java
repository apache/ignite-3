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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.config.schema.ChannelView;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.event.IgniteEventType;

public class ConfigurationBasedChannelRegistry implements ChannelRegistry {
    private final EventLogConfiguration cfg;

    private final Map<String, EventChannel> cache;
    private final Map<IgniteEventType, Set<EventChannel>> typeCache;

    public ConfigurationBasedChannelRegistry(EventLogConfiguration cfg) {
        this.cfg = cfg;
        this.cache = new ConcurrentHashMap<>();
        this.typeCache = new ConcurrentHashMap<>();
        this.cfg.channels().listen(new CacheUpdater());
    }

    @Override
    public EventChannel getByName(String name) {
        return cache.get(name);
    }

    @Override
    public Set<EventChannel> findAllChannelsByEventType(IgniteEventType igniteEventType) {
        Set<EventChannel> result = typeCache.get(igniteEventType);

        return result == null ? Set.of() : result;
    }

    private class CacheUpdater implements ConfigurationListener<NamedListView<ChannelView>> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<NamedListView<ChannelView>> ctx) {
            NamedListView<ChannelView> newListValue = ctx.newValue();

            cache.clear();
            typeCache.clear();

            newListValue.forEach(view -> {
                cache.put(view.name(), createChannel(view));
                for (String eventType : view.events()) {
                    IgniteEventType type = IgniteEventType.valueOf(eventType.trim());
                    typeCache.computeIfAbsent(type, t -> ConcurrentHashMap.newKeySet())
                            .add(cache.get(view.name()));
                }
            });

            return CompletableFuture.completedFuture(null);
        }

        private EventChannel createChannel(ChannelView view) {
            Set<IgniteEventType> types = Arrays.stream(view.events()).map(e -> IgniteEventType.valueOf(e.trim())).collect(Collectors.toSet());
            return new EventChannelImpl(types);
        }
    }
}
