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
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory key/value storage for tests.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    private static final IgniteLogger LOG = Loggers.forClass(SimpleInMemoryKeyValueStorage.class);

    /** Lexicographical comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compareUnsigned;

    /**
     * Special value for revision number which means that operation should be applied to the latest revision of an entry.
     */
    private static final long LATEST_REV = -1;

    /** Keys index. Value is the list of all revisions under which entry corresponding to the key was modified. */
    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(CMP);

    /** Revisions index. Value contains all entries which were modified under particular revision. */
    private NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new TreeMap<>();

    /** Revision. Will be incremented for each single-entry or multi-entry update operation. */
    private long rev;

    /** Update counter. Will be incremented for each update of any particular entry. */
    private long updCntr;

    /** All operations are queued on this lock. */
    private final Object mux = new Object();

    private boolean areWatchesEnabled = false;

    private final WatchProcessor watchProcessor = new WatchProcessor(this::get);

    private final ExecutorService watchExecutor;

    private final List<Entry> updatedEntries = new ArrayList<>();

    public SimpleInMemoryKeyValueStorage(String nodeName) {
        this.watchExecutor = Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, "watch-executor", LOG));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // no-op
    }

    /** {@inheritDoc} */
    @Override
    public long revision() {
        synchronized (mux) {
            return rev;
        }
    }

    /** {@inheritDoc} */
    @Override
    public long updateCounter() {
        synchronized (mux) {
            return updCntr;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void put(byte[] key, byte[] value) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPut(key, value, curRev);

            updateRevision(curRev);
        }
    }

    private void updateRevision(long newRevision) {
        rev = newRevision;

        notifyWatches();
    }

    /** {@inheritDoc} */
    @Override
    public Entry getAndPut(byte[] key, byte[] bytes) {
        synchronized (mux) {
            long curRev = rev + 1;

            long lastRev = doPut(key, bytes, curRev);

            updateRevision(curRev);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        Collection<Entry> res;

        synchronized (mux) {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            doPutAll(curRev, keys, values);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public Entry get(byte[] key) {
        synchronized (mux) {
            return doGet(key, LATEST_REV);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Entry get(byte[] key, long revUpperBound) {
        synchronized (mux) {
            return doGet(key, revUpperBound);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Entry> getAll(List<byte[]> keys) {
        return doGetAll(keys, LATEST_REV);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return doGetAll(keys, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override
    public void remove(byte[] key) {
        synchronized (mux) {
            long curRev = rev + 1;

            if (doRemove(key, curRev)) {
                updateRevision(curRev);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public Entry getAndRemove(byte[] key) {
        synchronized (mux) {
            Entry e = doGet(key, LATEST_REV);

            if (e.empty() || e.tombstone()) {
                return e;
            }

            return getAndPut(key, TOMBSTONE);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void removeAll(List<byte[]> keys) {
        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV);

                if (e.empty() || e.tombstone()) {
                    continue;
                }

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        synchronized (mux) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV);

                res.add(e);

                if (e.empty() || e.tombstone()) {
                    continue;
                }

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
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
                updateRevision(curRev);
            }

            return branch;
        }
    }

    @Override
    public StatementResult invoke(If iif) {
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
                        updateRevision(curRev);
                    }

                    return branch.update().result();
                } else {
                    currIf = branch.iif();
                }
            }
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, boolean includeTombstones) {
        long currentRevision;

        synchronized (mux) {
            currentRevision = rev;
        }

        return range(keyFrom, keyTo, currentRevision, includeTombstones);
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound, boolean includeTombstones) {
        return new RangeCursor(keyFrom, keyTo, revUpperBound, includeTombstones);
    }

    @Override
    public Cursor<Entry> prefix(byte[] prefix, boolean includeTombstones) {
        long currentRevision;

        synchronized (mux) {
            currentRevision = rev;
        }

        return prefix(prefix, currentRevision, includeTombstones);
    }

    @Override
    public Cursor<Entry> prefix(byte[] prefix, long revUpperBound, boolean includeTombstones) {
        return new RangeCursor(prefix, incrementPrefix(prefix), revUpperBound, includeTombstones);
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
    public void watchPrefix(byte[] prefix, long rev, WatchListener listener) {
        watchRange(prefix, incrementPrefix(prefix), rev, listener);
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

        var updatedEntriesCopy = List.copyOf(updatedEntries);

        updatedEntries.clear();

        watchExecutor.execute(() -> watchProcessor.notifyWatches(updatedEntriesCopy));
    }

    @Override
    public void removeWatch(WatchListener listener) {
        watchProcessor.removeWatch(listener);
    }

    @Override
    public void compact() {
        synchronized (mux) {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(CMP);

            NavigableMap<Long, NavigableMap<byte[], Value>> compactedRevsIdx = new TreeMap<>();

            keysIdx.forEach((key, revs) -> compactForKey(key, revs, compactedKeysIdx, compactedRevsIdx));

            keysIdx = compactedKeysIdx;

            revsIdx = compactedRevsIdx;
        }
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(watchExecutor, 10, TimeUnit.SECONDS);
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
        Entry e = doGet(key, LATEST_REV);

        if (e.empty() || e.tombstone()) {
            return false;
        }

        doPut(key, TOMBSTONE, curRev);

        return true;
    }

    private void compactForKey(
            byte[] key,
            List<Long> revs,
            Map<byte[], List<Long>> compactedKeysIdx,
            Map<Long, NavigableMap<byte[], Value>> compactedRevsIdx
    ) {
        Long lastRev = lastRevision(revs);

        NavigableMap<byte[], Value> kv = revsIdx.get(lastRev);

        Value lastVal = kv.get(key);

        if (!lastVal.tombstone()) {
            compactedKeysIdx.put(key, listOf(lastRev));

            NavigableMap<byte[], Value> compactedKv = compactedRevsIdx.computeIfAbsent(
                    lastRev,
                    k -> new TreeMap<>(CMP)
            );

            compactedKv.put(key, lastVal);
        }
    }

    private Collection<Entry> doGetAll(List<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev > 0 || rev == LATEST_REV : "Revision must be positive or " + LATEST_REV + '.';

        Collection<Entry> res = new ArrayList<>(keys.size());

        synchronized (mux) {
            for (byte[] key : keys) {
                res.add(doGet(key, rev));
            }
        }

        return res;
    }

    private Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= LATEST_REV : "Invalid arguments: [revUpperBound=" + revUpperBound + ']';

        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty()) {
            return EntryImpl.empty(key);
        }

        long lastRev;

        if (revUpperBound == LATEST_REV) {
            lastRev = lastRevision(revs);
        } else {
            lastRev = maxRevision(revs, revUpperBound);
        }

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

    private long doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList) {
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

            updateRevision(curRev);

            return curRev;
        }
    }

    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    private static List<Long> listOf(long val) {
        List<Long> res = new ArrayList<>();

        res.add(val);

        return res;
    }

    /**
     * Extension of {@link Cursor}.
     */
    private class RangeCursor implements Cursor<Entry> {
        private final byte[] keyFrom;

        private final byte @Nullable [] keyTo;

        private final long rev;

        @Nullable
        private Entry nextRetEntry;

        private byte[] lastRetKey;

        private final boolean includeTombstones;

        private boolean finished;

        RangeCursor(byte[] keyFrom, byte @Nullable [] keyTo, long rev, boolean includeTombstones) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.rev = rev;
            this.includeTombstones = includeTombstones;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            synchronized (mux) {
                while (true) {
                    if (finished) {
                        return false;
                    }

                    if (nextRetEntry != null) {
                        return true;
                    }

                    byte[] key = lastRetKey;

                    while (!finished || nextRetEntry == null) {
                        Map.Entry<byte[], List<Long>> e =
                                key == null ? keysIdx.ceilingEntry(keyFrom) : keysIdx.higherEntry(key);

                        if (e == null) {
                            finished = true;

                            break;
                        }

                        key = e.getKey();

                        if (keyTo != null && CMP.compare(key, keyTo) >= 0) {
                            finished = true;

                            break;
                        }

                        List<Long> revs = e.getValue();

                        assert revs != null && !revs.isEmpty() :
                                "Revisions should not be empty or null: [revs=" + revs + ']';

                        long lastRev = maxRevision(revs, rev);

                        if (lastRev == -1) {
                            continue;
                        }

                        Entry entry = doGetValue(key, lastRev);

                        if (!entry.tombstone() || includeTombstones) {
                            assert !entry.empty() : "Iterator should not return empty entry.";

                            nextRetEntry = entry;

                            break;
                        }
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public Entry next() {
            synchronized (mux) {
                while (true) {
                    if (finished) {
                        throw new NoSuchElementException();
                    }

                    if (nextRetEntry != null) {
                        Entry e = nextRetEntry;

                        nextRetEntry = null;

                        lastRetKey = e.key();

                        return e;
                    } else {
                        hasNext();
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            // No-op.
        }
    }
}
