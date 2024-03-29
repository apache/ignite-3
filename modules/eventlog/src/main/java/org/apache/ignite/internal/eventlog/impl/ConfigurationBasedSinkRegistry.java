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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.api.Sink;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.SinkView;

class ConfigurationBasedSinkRegistry implements SinkRegistry {
    private final ReadWriteLock guard;

    private final Map<String, Sink> cache;

    private final Map<String, Set<Sink>> cacheByChannel;

    private final SinkFactory sinkFactory;

    ConfigurationBasedSinkRegistry(EventLogConfiguration cfg) {
        this.guard = new ReentrantReadWriteLock();
        this.cache = new HashMap<>();
        this.cacheByChannel = new HashMap<>();
        this.sinkFactory = new SinkFactory();

        cfg.sinks().listen(new CacheUpdater());
    }

    @Override
    public Sink getByName(String name) {
        guard.readLock().lock();
        try {
            return cache.get(name);
        } finally {
            guard.readLock().unlock();
        }
    }

    @Override
    public Set<Sink> findAllByChannel(String channel) {
        guard.readLock().lock();
        try {
            Set<Sink> sinks = cacheByChannel.get(channel);
            return new HashSet<>(sinks != null ? sinks : Set.of());
        } finally {
            guard.readLock().unlock();
        }
    }

    private class CacheUpdater implements ConfigurationListener<NamedListView<SinkView>> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<NamedListView<SinkView>> ctx) {
            NamedListView<SinkView> newListValue = ctx.newValue();

            guard.writeLock().lock();
            try {
                cache.clear();
                cacheByChannel.clear();
                for (SinkView sinkView : newListValue) {
                    cache.put(sinkView.name(), sinkFactory.createSink(sinkView));
                    cacheByChannel.computeIfAbsent(sinkView.channel(), k -> new HashSet<>()).add(cache.get(sinkView.name()));
                }
                return CompletableFuture.completedFuture(null);
            } finally {
                guard.writeLock().unlock();
            }
        }
    }
}
