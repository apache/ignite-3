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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;

/**
 * Class for storing and notifying Meta Storage Watches.
 */
public class WatchProcessor {
    /** Reads an entry from the storage using a given key and revision. */
    @FunctionalInterface
    public interface EntryReader {
        Entry get(byte[] key, long revision);
    }

    private static final IgniteLogger LOG = Loggers.forClass(WatchProcessor.class);

    /** List of watches that listen to storage events. */
    private final List<Watch> watches = new CopyOnWriteArrayList<>();

    private final EntryReader entryReader;

    /** Callback that gets notified after a {@link WatchEvent} has been processed by all registered watches. */
    private volatile OnRevisionAppliedCallback revisionCallback;

    /**
     * Creates a new instance.
     *
     * @param entryReader Function for reading an entry from the storage using a given key and revision.
     */
    public WatchProcessor(EntryReader entryReader) {
        this.entryReader = entryReader;
    }

    /** Adds a watch. */
    public void addWatch(Watch watch) {
        watches.add(watch);
    }

    /** Removes a watch (identified by its listener). */
    public void removeWatch(WatchListener listener) {
        watches.removeIf(watch -> watch.listener() == listener);
    }

    /**
     * Returns the minimal target revision of all registered watches.
     */
    public OptionalLong minWatchRevision() {
        return watches.stream()
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
        // Revision must be the same for all entries.
        long newRevision = updatedEntries.get(0).revision();

        // Set of entries which keys match at least one watch.
        var matchedEntries = new HashSet<Entry>();

        for (Watch watch : watches) {
            var entryEvents = new ArrayList<EntryEvent>();

            for (Entry newEntry : updatedEntries) {
                byte[] newKey = newEntry.key();

                assert newEntry.revision() == newRevision;

                if (watch.matches(newKey, newRevision)) {
                    Entry oldEntry = entryReader.get(newKey, newRevision - 1);

                    entryEvents.add(new EntryEvent(oldEntry, newEntry));

                    matchedEntries.add(newEntry);
                }
            }

            if (!entryEvents.isEmpty()) {
                var event = new WatchEvent(entryEvents, newRevision);

                try {
                    watch.onUpdate(event);
                } catch (Exception e) {
                    // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                    LOG.error("Error occurred when processing a watch event {}, watch processing will be stopped", e, event);

                    watch.onError(e);

                    throw e;
                }
            }
        }

        if (!matchedEntries.isEmpty()) {
            revisionCallback.onRevisionApplied(newRevision, matchedEntries);
        }
    }
}
