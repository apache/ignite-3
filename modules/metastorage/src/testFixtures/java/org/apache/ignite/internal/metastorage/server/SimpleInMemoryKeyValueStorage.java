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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.OP_EXECUTION_ERR;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory key/value storage for tests.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    private static final IgniteLogger LOG = Loggers.forClass(SimpleInMemoryKeyValueStorage.class);

    /** Lexicographical comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compareUnsigned;

    /** Keys index. Value is the list of all revisions under which entry corresponding to the key was modified. */
    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(CMP);

    /** Timestamp to revision mapping. */
    private final NavigableMap<Long, Long> tsToRevMap = new TreeMap<>();

    /** Revision to timestamp mapping. */
    private final NavigableMap<Long, Long> revToTsMap = new TreeMap<>();

    /** Revisions index. Value contains all entries which were modified under particular revision. */
    private NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new TreeMap<>();

    /** Revision. Will be incremented for each single-entry or multi-entry update operation. */
    private long rev;

    /** Update counter. Will be incremented for each update of any particular entry. */
    private long updCntr;

    /** All operations are queued on this lock. */
    private final Object mux = new Object();

    private boolean areWatchesEnabled = false;

    private final WatchProcessor watchProcessor;

    private final List<Entry> updatedEntries = new ArrayList<>();

    public SimpleInMemoryKeyValueStorage(String nodeName) {
        this.watchProcessor = new WatchProcessor(nodeName, this::get);
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public long revision() {
        synchronized (mux) {
            return rev;
        }
    }

    @Override
    public long updateCounter() {
        synchronized (mux) {
            return updCntr;
        }
    }

    @Override
    public void put(byte[] key, byte[] value, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPut(key, value, curRev);

            updateRevision(curRev, opTs);
        }
    }

    private void updateRevision(long newRevision, HybridTimestamp ts) {
        rev = newRevision;

        tsToRevMap.put(ts.longValue(), rev);
        revToTsMap.put(rev, ts.longValue());

        notifyWatches();
    }

    @Override
    public Entry getAndPut(byte[] key, byte[] bytes, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            long lastRev = doPut(key, bytes, curRev);

            updateRevision(curRev, opTs);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values, opTs);
        }
    }

    @Override
    public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs) {
        Collection<Entry> res;

        synchronized (mux) {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            doPutAll(curRev, keys, values, opTs);
        }

        return res;
    }

    @Override
    public Entry get(byte[] key) {
        synchronized (mux) {
            return doGet(key, rev);
        }
    }

    @Override
    public Entry get(byte[] key, long revUpperBound) {
        synchronized (mux) {
            return doGet(key, revUpperBound);
        }
    }

    @Override
    public Collection<Entry> getAll(List<byte[]> keys) {
        synchronized (mux) {
            return doGetAll(keys, rev);
        }
    }

    @Override
    public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        synchronized (mux) {
            return doGetAll(keys, revUpperBound);
        }
    }

    @Override
    public void remove(byte[] key, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            if (doRemove(key, curRev)) {
                updateRevision(curRev, opTs);
            }
        }
    }

    @Override
    public Entry getAndRemove(byte[] key, HybridTimestamp opTs) {
        synchronized (mux) {
            Entry e = doGet(key, rev);

            if (e.empty() || e.tombstone()) {
                return e;
            }

            return getAndPut(key, TOMBSTONE, opTs);
        }
    }

    @Override
    public void removeAll(List<byte[]> keys, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, rev);

                if (e.empty() || e.tombstone()) {
                    continue;
                }

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals, opTs);
        }
    }

    @Override
    public Collection<Entry> getAndRemoveAll(List<byte[]> keys, HybridTimestamp opTs) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, rev);

                res.add(e);

                if (e.empty() || e.tombstone()) {
                    continue;
                }

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals, opTs);
        }

        return res;
    }

    @Override
    public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure, HybridTimestamp opTs) {
        synchronized (mux) {
            Collection<Entry> e = getAll(Arrays.asList(condition.keys()));

            boolean branch = condition.test(e.toArray(new Entry[]{}));

            Collection<Operation> ops = branch ? success : failure;

            long curRev = rev + 1;

            boolean modified = false;

            for (Operation op : ops) {
                switch (op.type()) {
                    case PUT:
                        doPut(op.key(), op.value(), curRev);

                        modified = true;

                        break;

                    case REMOVE:
                        modified |= doRemove(op.key(), curRev);

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                }
            }

            if (modified) {
                updateRevision(curRev, opTs);
            }

            return branch;
        }
    }

    @Override
    public StatementResult invoke(If iif, HybridTimestamp opTs) {
        synchronized (mux) {
            If currIf = iif;
            while (true) {
                Collection<Entry> e = getAll(Arrays.asList(currIf.cond().keys()));

                Statement branch = (currIf.cond().test(e.toArray(new Entry[]{}))) ? currIf.andThen() : currIf.orElse();

                if (branch.isTerminal()) {
                    long curRev = rev + 1;

                    boolean modified = false;

                    for (Operation op : branch.update().operations()) {
                        switch (op.type()) {
                            case PUT:
                                doPut(op.key(), op.value(), curRev);

                                modified = true;

                                break;

                            case REMOVE:
                                modified |= doRemove(op.key(), curRev);

                                break;

                            case NO_OP:
                                break;

                            default:
                                throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                        }
                    }

                    if (modified) {
                        updateRevision(curRev, opTs);
                    }

                    return branch.update().result();
                } else {
                    currIf = branch.iif();
                }
            }
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo) {
        synchronized (mux) {
            return range(keyFrom, keyTo, rev);
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        synchronized (mux) {
            SortedMap<byte[], List<Long>> subMap = keyTo == null
                    ? keysIdx.tailMap(keyFrom)
                    : keysIdx.subMap(keyFrom, keyTo);

            return subMap.entrySet().stream()
                    .map(e -> {
                        long targetRevision = maxRevision(e.getValue(), revUpperBound);

                        if (targetRevision == -1) {
                            return EntryImpl.empty(e.getKey());
                        }

                        return doGetValue(e.getKey(), targetRevision);
                    })
                    .filter(e -> !e.empty())
                    .collect(collectingAndThen(toList(), Cursor::fromIterable));
        }
    }

    @Override
    public byte @Nullable [] nextKey(byte[] key) {
        return incrementPrefix(key);
    }

    @Override
    public void watchRange(byte[] keyFrom, byte @Nullable [] keyTo, long rev, WatchListener listener) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        Predicate<byte[]> rangePredicate = keyTo == null
                ? k -> CMP.compare(keyFrom, k) <= 0
                : k -> CMP.compare(keyFrom, k) <= 0 && CMP.compare(keyTo, k) > 0;

        watchProcessor.addWatch(new Watch(rev, listener, rangePredicate));
    }

    @Override
    public void watchExact(byte[] key, long rev, WatchListener listener) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        Predicate<byte[]> exactPredicate = k -> CMP.compare(k, key) == 0;

        watchProcessor.addWatch(new Watch(rev, listener, exactPredicate));
    }

    @Override
    public void watchExact(Collection<byte[]> keys, long rev, WatchListener listener) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        Predicate<byte[]> inPredicate = keySet::contains;

        watchProcessor.addWatch(new Watch(rev, listener, inPredicate));
    }

    @Override
    public void startWatches(OnRevisionAppliedCallback revisionCallback) {
        synchronized (mux) {
            areWatchesEnabled = true;

            watchProcessor.setRevisionCallback(revisionCallback);

            replayUpdates();
        }
    }

    private void replayUpdates() {
        OptionalLong minWatchRevision = watchProcessor.minWatchRevision();

        if (minWatchRevision.isEmpty()) {
            return;
        }

        revsIdx.tailMap(minWatchRevision.getAsLong())
                .forEach((revision, entries) -> {
                    entries.forEach((key, value) -> {
                        var entry = new EntryImpl(key, value.bytes(), revision, value.updateCounter());

                        updatedEntries.add(entry);
                    });

                    notifyWatches();
                });
    }

    private void notifyWatches() {
        if (!areWatchesEnabled || updatedEntries.isEmpty()) {
            updatedEntries.clear();

            return;
        }

        Long tsLong = revToTsMap.get(updatedEntries.get(0).revision());
        assert tsLong != null;

        watchProcessor.notifyWatches(List.copyOf(updatedEntries), new HybridTimestamp(tsLong));

        updatedEntries.clear();
    }

    @Override
    public void removeWatch(WatchListener listener) {
        watchProcessor.removeWatch(listener);
    }

    @Override
    public void compact(HybridTimestamp lowWatermark) {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(CMP);

            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx = new TreeMap<>();

            Map.Entry<Long, Long> revisionEntry = tsToRevMap.floorEntry(lowWatermark.longValue());

            if (revisionEntry == null) {
                // Nothing to compact yet.
                return;
            }

            long maxRevision = revisionEntry.getValue();

            keysIdx.forEach((key, revs) -> compactForKey(key, revs, compactedKeysIdx, compactedRevsIdx, maxRevision));

            keysIdx = compactedKeysIdx;

            revsIdx = compactedRevsIdx;
        }
    }

    @Override
    public void close() {
        watchProcessor.close();
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreSnapshot(Path snapshotPath) {
        throw new UnsupportedOperationException();
    }

    private boolean doRemove(byte[] key, long curRev) {
        Entry e = doGet(key, curRev);

        if (e.empty() || e.tombstone()) {
            return false;
        }

        doPut(key, TOMBSTONE, curRev);

        return true;
    }

    /**
     * Compacts all entries by the given key, removing revision that are no longer needed.
     * Last entry with a revision lesser or equal to the {@code minRevisionToKeep} and all consecutive entries will be preserved.
     * If the first entry to keep is a tombstone, it will be removed.
     *
     * @param key A key.
     * @param revs All revisions of a key.
     * @param compactedKeysIdx Out parameter, revisions that need to be kept must be put here.
     * @param compactedRevsIdx Out parameter, values that need to be kept must be put here.
     * @param minRevisionToKeep Minimum revision that should be kept.
     */
    private void compactForKey(
            byte[] key,
            List<Long> revs,
            Map<byte[], List<Long>> compactedKeysIdx,
            Map<Long, NavigableMap<byte[], Value>> compactedRevsIdx,
            long minRevisionToKeep
    ) {
        List<Long> revsToKeep = new ArrayList<>();

        // Index of the first revision we will be keeping in the array of revisions.
        int idxToKeepFrom = 0;

        // Whether there is an entry with the minRevisionToKeep.
        boolean hasMinRevision = false;

        // Traverse revisions, looking for the first revision that needs to be kept.
        for (long rev : revs) {
            if (rev >= minRevisionToKeep) {
                if (rev == minRevisionToKeep) {
                    hasMinRevision = true;
                }
                break;
            }

            idxToKeepFrom++;
        }

        if (!hasMinRevision) {
            // Minimal revision was not encountered, that mean that we are between revisions of a key, so previous revision
            // must be preserved.
            idxToKeepFrom--;
        }

        for (int i = idxToKeepFrom; i < revs.size(); i++) {
            long rev = revs.get(i);

            // If this revision is higher than max revision or is the last revision, we may need to keep it.
            NavigableMap<byte[], Value> kv = revsIdx.get(rev);

            Value value = kv.get(key);

            if (i == idxToKeepFrom) {
                // Check if a first entry to keep is a tombstone.
                if (value.tombstone()) {
                    // If this is a first revision we are keeping and it is a tombstone, then don't keep it.
                    continue;
                }
            }

            NavigableMap<byte[], Value> compactedKv = compactedRevsIdx.computeIfAbsent(
                    rev,
                    k -> new TreeMap<>(CMP)
            );

            // Keep the entry and the revision.
            compactedKv.put(key, value);

            revsToKeep.add(rev);
        }

        if (!revsToKeep.isEmpty()) {
            compactedKeysIdx.put(key, revsToKeep);
        }
    }

    private Collection<Entry> doGetAll(List<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev >= 0;

        Collection<Entry> res = new ArrayList<>(keys.size());

        for (byte[] key : keys) {
            res.add(doGet(key, rev));
        }

        return res;
    }

    private Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= 0 : "Invalid arguments: [revUpperBound=" + revUpperBound + ']';

        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty()) {
            return EntryImpl.empty(key);
        }

        long lastRev = maxRevision(revs, revUpperBound);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1) {
            return EntryImpl.empty(key);
        }

        return doGetValue(key, lastRev);
    }

    /**
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then {@code -1} will be
     * returned.
     *
     * @param revs Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Appropriate revision or {@code -1} if there is no such revision.
     */
    private static long maxRevision(List<Long> revs, long upperBoundRev) {
        int i = revs.size() - 1;

        for (; i >= 0; i--) {
            long rev = revs.get(i);

            if (rev <= upperBoundRev) {
                return rev;
            }
        }

        return -1;
    }

    private Entry doGetValue(byte[] key, long lastRev) {
        if (lastRev == 0) {
            return EntryImpl.empty(key);
        }

        NavigableMap<byte[], Value> lastRevVals = revsIdx.get(lastRev);

        if (lastRevVals == null || lastRevVals.isEmpty()) {
            return EntryImpl.empty(key);
        }

        Value lastVal = lastRevVals.get(key);

        if (lastVal.tombstone()) {
            return EntryImpl.tombstone(key, lastRev, lastVal.updateCounter());
        }

        return new EntryImpl(key, lastVal.bytes(), lastRev, lastVal.updateCounter());
    }

    private long doPut(byte[] key, byte[] bytes, long curRev) {
        long curUpdCntr = ++updCntr;

        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

        revs.add(curRev);

        // Update revsIdx.
        Value val = new Value(bytes, curUpdCntr);

        revsIdx.compute(
                curRev,
                (rev, entries) -> {
                    if (entries == null) {
                        entries = new TreeMap<>(CMP);
                    }

                    entries.put(key, val);

                    return entries;
                }
        );

        var updatedEntry = new EntryImpl(key, val.tombstone() ? null : bytes, curRev, curUpdCntr);

        updatedEntries.add(updatedEntry);

        return lastRev;
    }

    private long doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList, HybridTimestamp opTs) {
        synchronized (mux) {
            // Update revsIdx.
            NavigableMap<byte[], Value> entries = new TreeMap<>(CMP);

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                byte[] bytes = bytesList.get(i);

                long curUpdCntr = ++updCntr;

                // Update keysIdx.
                List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

                revs.add(curRev);

                Value val = new Value(bytes, curUpdCntr);

                entries.put(key, val);

                updatedEntries.add(new EntryImpl(key, bytes, curRev, curUpdCntr));

                revsIdx.put(curRev, entries);
            }

            updateRevision(curRev, opTs);

            return curRev;
        }
    }

    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }
}
