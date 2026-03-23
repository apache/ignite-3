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

import static org.apache.ignite.configuration.notifications.ConfigurationListener.fromNewValueConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.internal.eventlog.api.Sink;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.SinkView;
import org.jetbrains.annotations.Nullable;

class ConfigurationBasedSinkRegistry implements SinkRegistry {
    private final EventLogConfiguration cfg;

    private final ConfigurationListener<NamedListView<SinkView>> listener = fromNewValueConsumer(this::updateCache);

    private volatile Map<String, Sink<?>> cache;

    private volatile Map<String, Set<Sink<?>>> cacheByChannel;

    private final SinkFactory sinkFactory;

    ConfigurationBasedSinkRegistry(EventLogConfiguration cfg, SinkFactory sinkFactory) {
        this.cfg = cfg;
        this.cache = new HashMap<>();
        this.cacheByChannel = new HashMap<>();
        this.sinkFactory = sinkFactory;
    }

    @Override
    public Sink<?> getByName(String name) {
        return cache.get(name);
    }

    @Override
    public Set<Sink<?>> findAllByChannel(String channel) {
        return cacheByChannel.get(channel);
    }

    @Override
    public void start() {
        updateCache(cfg.sinks().value());

        cfg.sinks().listen(listener);
    }

    @Override
    public void stop() {
        cfg.sinks().stopListen(listener);

        clearCache();
    }

    private void updateCache(@Nullable NamedListView<SinkView> newListValue) {
        Map<String, Sink<?>> newCache = new HashMap<>();
        Map<String, Set<Sink<?>>> newCacheByChannel = new HashMap<>();

        clearCache();

        if (newListValue != null) {
            for (SinkView sinkView : newListValue) {
                Sink<?> sink = sinkFactory.createSink(sinkView);
                newCache.put(sinkView.name(), sink);
                newCacheByChannel.computeIfAbsent(sinkView.channel(), k -> new HashSet<>()).add(sink);
            }
        }

        cache = newCache;
        cacheByChannel = newCacheByChannel;
    }

    private void clearCache() {
        cache.values().forEach(Sink::stop);
    }
}
