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
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;

/**
 * Cleans entries after TTL is expired.
 *
 * @param <T> Type of entry.
 */
public class Cleaner<T> {
    private static final IgniteLogger LOG = Loggers.forClass(Cleaner.class);

    private ExecutorService cleaner;

    private final Set<UUID> toRemove = new HashSet<>();

    private final Queue<UUID> waitToRemove = new ConcurrentLinkedQueue<>();

    /**
     * Starts the cleaner.
     *
     * @param clean Function to clean the entry.
     * @param ttlMillis Time after which the clean function will be called for the scheduled entry, in milliseconds.
     * @param nodeName Node name.
     */
    public void start(Consumer<UUID> clean, long ttlMillis, String nodeName) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "compute-execution-cleanup", true, LOG)
        );

        executor.scheduleAtFixedRate(() -> {
            toRemove.forEach(clean);
            toRemove.clear();

            UUID nextToRemove;
            //noinspection NestedAssignment
            while ((nextToRemove = waitToRemove.poll()) != null) {
                toRemove.add(nextToRemove);
            }
        }, ttlMillis, ttlMillis, TimeUnit.MILLISECONDS);

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
