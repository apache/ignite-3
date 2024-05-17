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

package org.apache.ignite.internal.client.util;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Cache with time to live mechanism.
 */
public class Cache<K, V> {
    private final Map<K, V> cache = new ConcurrentHashMap<>();

    private final TemporalAmount ttl;

    private final Supplier<Map<K, V>> updateFunction;

    private Instant aliveUntil;

    /**
     * Creates new topology cache with 1 minute ttl.
     *
     * @param updateFunction New server nodes topology provider.
     */
    public Cache(Supplier<Map<K, V>> updateFunction) {
        this(updateFunction, Duration.ofMinutes(1));
    }

    /**
     * Creates new topology cache.
     *
     * @param updateFunction New server nodes topology provider.
     * @param ttl Time to live of cache values.
     */
    public Cache(Supplier<Map<K, V>> updateFunction, TemporalAmount ttl) {
        this.updateFunction = updateFunction;
        this.ttl = ttl;
    }

    /**
     * Refreshes cache with new given topology.
     *
     * @param newTopology New server nodes topology.
     */
    public void refresh(Map<K, V> newTopology) {
        cache.clear();
        cache.putAll(newTopology);
        aliveUntil = Instant.now().plus(ttl);
    }

    /**
     * Returns {@link ClusterNode} for given node name.
     *
     * @param key Node name.
     * @return Cluster node for given node name or {@code null} if node name is not presented in topology.
     */
    public @Nullable V get(K key) {
        if (aliveUntil == null || Instant.now().isAfter(aliveUntil)) {
            refresh(updateFunction.get());
        }
        return cache.get(key);
    }
}
