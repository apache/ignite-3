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

package org.apache.ignite.internal.metastorage;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.timebag.TimeBag;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.TestOnly;

/**
 * Watch event contains all entry updates done under one revision. Each particular entry update in this revision is represented by {@link
 * EntryEvent} entity.
 */
public class WatchEvent {
    /** Events about each entry update in the revision. */
    @IgniteToStringInclude
    private final List<EntryEvent> entryEvts;

    private final long revision;

    /** Timestamp assigned by the MetaStorage to the event's revision. */
    private final HybridTimestamp timestamp;

    /** Time bag to collect timings of this event processing. */
    private final TimeBag timeBag;

    /**
     * Constructs an watch event with given entry events collection.
     *
     * @param entryEvts Events for entries corresponding to an update under one revision.
     * @param revision Revision of the updated entries.
     * @param timestamp Timestamp assigned by the MetaStorage to the event's revision.
     * @param timeBag Time bag to collect timings of this event processing.
     */
    public WatchEvent(Collection<EntryEvent> entryEvts, long revision, HybridTimestamp timestamp, TimeBag timeBag) {
        this.entryEvts = List.copyOf(entryEvts);
        this.revision = revision;
        this.timestamp = timestamp;
        this.timeBag = timeBag;
    }

    /**
     * Constructs watch event with single entry update.
     *
     * @param entryEvt Entry event.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19820 - remove/rework.
    @TestOnly
    public WatchEvent(EntryEvent entryEvt) {
        this(List.of(entryEvt), entryEvt.newEntry().revision(), HybridTimestamp.MAX_VALUE, TimeBag.createTimeBag(false, false));
    }

    /**
     * Returns {@code true} if watch event contains only one entry event.
     *
     * @return {@code True} if watch event contains only one entry event.
     */
    public boolean single() {
        return entryEvts.size() == 1;
    }

    /**
     * Returns collection of entry entry event done under one revision.
     *
     * @return Collection of entry entry event done under one revision.
     */
    public Collection<EntryEvent> entryEvents() {
        return entryEvts;
    }

    /**
     * Returns entry event. It is useful method in case when we know that only one event was modified.
     *
     * @return Entry event.
     */
    public EntryEvent entryEvent() {
        assert single() : entryEvts;

        return entryEvts.get(0);
    }

    /**
     * Returns the revision of the modified entries.
     *
     * @return Event revision.
     */
    public long revision() {
        return revision;
    }

    /**
     * Returns the timestamp assigned by the MetaStorage to this event's revision.
     *
     * @return Timestamp assigned by the MetaStorage to this event's revision.
     */
    public HybridTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns the time bag to collect timings of this event processing.
     *
     * @return Time bag.
     */
    public TimeBag timeBag() {
        return timeBag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WatchEvent event = (WatchEvent) o;

        if (revision != event.revision) {
            return false;
        }
        return entryEvts.equals(event.entryEvts);
    }

    @Override
    public int hashCode() {
        int result = entryEvts.hashCode();
        result = 31 * result + (int) (revision ^ (revision >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
