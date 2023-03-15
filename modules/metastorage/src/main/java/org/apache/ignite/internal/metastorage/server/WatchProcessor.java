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

package org.apache.ignite.internal.metastorage.server;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Class for storing and notifying Meta Storage Watches.
 *
 * <p>Every Meta Storage update is processed by each registered Watch in parallel, however notifications for a single Watch are
 * linearised (Watches are always notified of one event at a time and in increasing order of revisions).
 */
public class WatchProcessor implements ManuallyCloseable {
    /** Reads an entry from the storage using a given key and revision. */
    @FunctionalInterface
    public interface EntryReader {
        Entry get(byte[] key, long revision);
    }

    private static final IgniteLogger LOG = Loggers.forClass(WatchProcessor.class);

    /** Map that contains Watches and corresponding Watch notification process (represented as a CompletableFuture). */
    private final ConcurrentMap<Watch, CompletableFuture<Void>> watches = new ConcurrentHashMap<>();

    private final EntryReader entryReader;

    /** Callback that gets notified after a {@link WatchEvent} has been processed by a registered watch. */
    private volatile OnRevisionAppliedCallback revisionCallback;

    /** Executor for processing watch events. */
    private final ExecutorService watchExecutor;

    /**
     * Creates a new instance.
     *
     * @param entryReader Function for reading an entry from the storage using a given key and revision.
     */
    public WatchProcessor(String nodeName, EntryReader entryReader) {
        this.entryReader = entryReader;

        this.watchExecutor = Executors.newFixedThreadPool(4, NamedThreadFactory.create(nodeName, "metastorage-watch-executor", LOG));
    }

    /** Adds a watch. */
    public void addWatch(Watch watch) {
        assert !watches.containsKey(watch) : "Watch with id \"" + watch.id() + "\" already exists";

        watches.put(watch, completedFuture(null));
    }

    /** Removes a watch (identified by its listener). */
    public void removeWatch(WatchListener listener) {
        watches.keySet().removeIf(watch -> watch.listener() == listener);
    }

    /**
     * Returns the minimal target revision of all registered watches.
     */
    public OptionalLong minWatchRevision() {
        return watches.keySet().stream()
                .mapToLong(Watch::targetRevision)
                .min();
    }

    /**
     * Sets the callback that will be executed every time after watches have been notified of a particular revision.
     */
    public void setRevisionCallback(OnRevisionAppliedCallback revisionCallback) {
        assert this.revisionCallback == null;

        this.revisionCallback = revisionCallback;
    }

    /**
     * Notifies registered watch about an update event.
     */
    public void notifyWatches(List<Entry> updatedEntries) {
        watches.replaceAll((watch, watchOperation) ->
                watchOperation.thenComposeAsync(v -> notifyWatch(watch, updatedEntries), watchExecutor));
    }

    private CompletableFuture<Void> notifyWatch(Watch watch, List<Entry> updatedEntries) {
        // Revision must be the same for all entries.
        long newRevision = updatedEntries.get(0).revision();

        List<EntryEvent> entryEvents = List.of();

        for (Entry newEntry : updatedEntries) {
            byte[] newKey = newEntry.key();

            assert newEntry.revision() == newRevision;

            if (watch.matches(newKey, newRevision)) {
                Entry oldEntry = entryReader.get(newKey, newRevision - 1);

                if (entryEvents.isEmpty()) {
                    entryEvents = new ArrayList<>();
                }

                entryEvents.add(new EntryEvent(oldEntry, newEntry));
            }
        }

        var event = new WatchEvent(entryEvents, newRevision);

        CompletableFuture<Void> eventNotificationFuture;

        try {
            eventNotificationFuture = entryEvents.isEmpty()
                    ? watch.onRevisionUpdated(newRevision)
                    : watch.onUpdate(event);
        } catch (Throwable e) {
            eventNotificationFuture = failedFuture(e);
        }

        return eventNotificationFuture
                .whenComplete((v, e) -> {
                    if (e != null) {
                        // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                        LOG.error("Error occurred when processing a watch event {}, watch {} is going to be stopped", e, event, watch.id());

                        watch.onError(e);

                        watches.remove(watch);
                    }
                })
                .thenCompose(v -> revisionCallback.onRevisionApplied(watch.id(), event));
    }

    @Override
    public void close() {
        watches.values().forEach(f -> f.cancel(true));

        IgniteUtils.shutdownAndAwaitTermination(watchExecutor, 10, TimeUnit.SECONDS);
    }
}
