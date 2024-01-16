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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Cleans entries after TTL is expired.
 *
 * @param <T> Type of entry.
 */
public class Cleaner<T> {
    private static final IgniteLogger LOG = Loggers.forClass(Cleaner.class);

    private ExecutorService cleaner;

    private final Set<UUID> toRemove = new HashSet<>();

    private final Set<UUID> waitToRemove = ConcurrentHashMap.newKeySet();

    /**
     * Starts the cleaner.
     *
     * @param clean Function to clean the entry.
     * @param ttl Time after which the clean function will be called for the scheduled to remove entry.
     */
    public void start(Consumer<UUID> clean, long ttl) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("Cleaner-pool", LOG)
        );
        executor.scheduleAtFixedRate(() -> {
            toRemove.forEach(clean);
            toRemove.clear();

            Set<UUID> nextToRemove = Set.of(waitToRemove.toArray(UUID[]::new));
            waitToRemove.removeAll(nextToRemove);
            toRemove.addAll(nextToRemove);
        }, ttl, ttl, TimeUnit.MILLISECONDS);
        cleaner = executor;
    }

    /**
     * Stops the cleaner.
     */
    public void stop() {
        shutdownAndAwaitTermination(cleaner, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedules entry to remove.
     *
     * @param entryId Entry id.
     */
    public void scheduleRemove(UUID entryId) {
        waitToRemove.add(entryId);
    }
}
