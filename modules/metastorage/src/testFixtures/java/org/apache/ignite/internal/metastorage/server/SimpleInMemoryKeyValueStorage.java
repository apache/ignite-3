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

import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOT_FOUND;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertCompactionRevisionLessThanCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertRequestedRevisionLessThanOrEqualToCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.indexToCompact;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.isLastIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.maxRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.minRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.toUtf8String;
import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.IDEMPOTENT_COMMAND_PREFIX;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.OP_EXECUTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.RESTORING_STORAGE_ERR;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory key/value storage for tests.
 */
public class SimpleInMemoryKeyValueStorage implements KeyValueStorage {
    /** Lexicographical comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compareUnsigned;

    /**
     * Keys index. Value is the list of all revisions under which entry corresponding to the key was modified.
     *
     * <p>Concurrent map to avoid {@link java.util.ConcurrentModificationException} on compaction.</p>
     *
     * <p>Guarded by {@link #mux}.</p>
     */
    private final NavigableMap<byte[], List<Long>> keysIdx = new ConcurrentSkipListMap<>(CMP);

    /** Timestamp to revision mapping. */
    private final NavigableMap<Long, Long> tsToRevMap = new TreeMap<>();

    /**
     * Revision to timestamp mapping.
     *
     * <p>Guarded by {@link #mux}.</p>
     */
    private final Map<Long, HybridTimestamp> revToTsMap = new HashMap<>();

    /**
     * Revisions index. Value contains all entries which were modified under particular revision.
     *
     * <p>Concurrent map to avoid {@link java.util.ConcurrentModificationException} on compaction.</p>
     *
     * <p>Guarded by {@link #mux}.</p>
     */
    private final NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new ConcurrentSkipListMap<>();

    /**
     * Revision. Will be incremented for each single-entry or multi-entry update operation.
     *
     * <p>Multi-threaded access is guarded by {@link #mux}.</p>
     */
    private long rev;

    /**
     * Last compaction revision that was set or restored from a snapshot.
     *
     * <p>This field is used by metastorage read methods to determine whether {@link CompactedException} should be thrown.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #mux}.</p>
     */
    private long compactionRevision = -1;

    /**
     * Last {@link #saveCompactionRevision saved} compaction revision.
     *
     * <p>Used only when working with snapshots.</p>
     */
    private long savedCompactionRevision = -1;

    /** All operations are queued on this lock. */
    private final Object mux = new Object();

    private boolean areWatchesEnabled = false;

    private final WatchProcessor watchProcessor;

    private final List<Entry> updatedEntries = new ArrayList<>();

    /**
     * Revision listener for recovery only. Notifies {@link MetaStorageManagerImpl} of revision update.
     * Guarded by {@link #mux}.
     */
    private @Nullable LongConsumer recoveryRevisionListener;

    private final AtomicBoolean stopCompaction = new AtomicBoolean();

    public SimpleInMemoryKeyValueStorage(String nodeName) {
        this.watchProcessor = new WatchProcessor(nodeName, this::get, new NoOpFailureManager());
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
    public void put(byte[] key, byte[] value, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            doPut(key, value, curRev, opTs);

            updateRevision(curRev, opTs);
        }
    }

    private void updateRevision(long newRevision, HybridTimestamp ts) {
        rev = newRevision;

        tsToRevMap.put(ts.longValue(), rev);
        revToTsMap.put(rev, ts);

        notifyWatches();

        notifyRevisionUpdate();
    }

    /**
     * Notifies of revision update.
     * Must be called under the {@link #mux} lock.
     */
    private void notifyRevisionUpdate() {
        if (recoveryRevisionListener != null) {
            // Listener must be invoked only on recovery, after recovery listener must be null.
            recoveryRevisionListener.accept(rev);
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
    public List<Entry> get(byte[] key, long revLowerBound, long revUpperBound) {
        synchronized (mux) {
            return doGet(key, revLowerBound, revUpperBound);
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        synchronized (mux) {
            return doGetAll(keys, rev);
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        synchronized (mux) {
            return doGetAll(keys, revUpperBound);
        }
    }

    @Override
    public void remove(byte[] key, HybridTimestamp opTs) {
        synchronized (mux) {
            long curRev = rev + 1;

            doRemove(key, curRev, opTs);
            updateRevision(curRev, opTs);
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
    public boolean invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure,
            HybridTimestamp opTs,
            CommandId commandId
    ) {
        synchronized (mux) {
            Collection<Entry> e = getAll(Arrays.asList(condition.keys()));

            boolean branch = condition.test(e.toArray(new Entry[]{}));

            List<Operation> ops = branch ? new ArrayList<>(success) : new ArrayList<>(failure);

            // In case of in-memory storage, there's no sense in "persisting" invoke result, however same persistent source operations
            // were added in order to have matching revisions count through all storages.
            ops.add(Operations.put(
                    new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                    branch ? INVOKE_RESULT_TRUE_BYTES : INVOKE_RESULT_FALSE_BYTES)
            );

            long curRev = rev + 1;

            for (Operation op : ops) {
                switch (op.type()) {
                    case PUT:
                        doPut(toByteArray(op.key()), toByteArray(op.value()), curRev, opTs);

                        break;

                    case REMOVE:
                        doRemove(toByteArray(op.key()), curRev, opTs);

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                }
            }

            updateRevision(curRev, opTs);

            return branch;
        }
    }

    @Override
    public StatementResult invoke(If iif, HybridTimestamp opTs, CommandId commandId) {
        synchronized (mux) {
            If currIf = iif;
            while (true) {
                Collection<Entry> e = getAll(Arrays.asList(currIf.cond().keys()));

                Statement branch = (currIf.cond().test(e.toArray(new Entry[]{}))) ? currIf.andThen() : currIf.orElse();

                if (branch.isTerminal()) {
                    long curRev = rev + 1;

                    List<Operation> ops = new ArrayList<>(branch.update().operations());

                    // In case of in-memory storage, there's no sense in "persisting" invoke result, however same persistent source
                    // operations were added in order to have matching revisions count through all storages.
                    ops.add(Operations.put(
                            new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                            branch.update().result().result())
                    );

                    for (Operation op : ops) {
                        switch (op.type()) {
                            case PUT:
                                doPut(toByteArray(op.key()), toByteArray(op.value()), curRev, opTs);

                                break;

                            case REMOVE:
                                doRemove(toByteArray(op.key()), curRev, opTs);

                                break;

                            case NO_OP:
                                break;

                            default:
                                throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                        }
                    }

                    updateRevision(curRev, opTs);

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
            return doRange(keyFrom, keyTo, rev);
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        synchronized (mux) {
            return doRange(keyFrom, keyTo, revUpperBound);
        }
    }

    @Override
    public byte @Nullable [] nextKey(byte[] key) {
        return incrementPrefix(key);
    }

    @Override
    public HybridTimestamp timestampByRevision(long revision) {
        assert revision >= 0 : revision;

        synchronized (mux) {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            HybridTimestamp timestamp = revToTsMap.get(revision);

            if (timestamp == null) {
                throw new CompactedException("Requested revision has already been compacted: " + revision);
            }

            return timestamp;
        }
    }

    @Override
    public long revisionByTimestamp(HybridTimestamp timestamp) {
        synchronized (mux) {
            Map.Entry<Long, Long> revisionEntry = tsToRevMap.floorEntry(timestamp.longValue());

            if (revisionEntry == null) {
                throw new CompactedException("Revisions less than or equal to the requested one are already compacted: " + timestamp);
            }

            return revisionEntry.getValue();
        }
    }

    @Override
    public void setRecoveryRevisionListener(@Nullable LongConsumer listener) {
        synchronized (mux) {
            this.recoveryRevisionListener = listener;
        }
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
    public void startWatches(long startRevision, OnRevisionAppliedCallback revisionCallback) {
        assert startRevision != 0 : "First meaningful revision is 1";

        synchronized (mux) {
            areWatchesEnabled = true;

            watchProcessor.setRevisionCallback(revisionCallback);

            replayUpdates(startRevision);
        }
    }

    private void replayUpdates(long startRevision) {
        long minWatchRevision = Math.max(startRevision, watchProcessor.minWatchRevision().orElse(-1));

        if (minWatchRevision <= 0) {
            return;
        }

        revsIdx.tailMap(minWatchRevision)
                .forEach((revision, entries) -> {
                    entries.forEach((key, value) -> {
                        var entry = new EntryImpl(key, value.bytes(), revision, value.operationTimestamp());

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

        long revision = updatedEntries.get(0).revision();

        HybridTimestamp ts = revToTsMap.get(revision);
        assert ts != null : revision;

        watchProcessor.notifyWatches(List.copyOf(updatedEntries), ts);

        updatedEntries.clear();
    }

    @Override
    public void removeWatch(WatchListener listener) {
        watchProcessor.removeWatch(listener);
    }

    @Override
    public void compact(long revision) {
        assert revision >= 0 : revision;

        for (Map.Entry<byte[], List<Long>> entry : keysIdx.entrySet()) {
            synchronized (mux) {
                assertCompactionRevisionLessThanCurrent(revision, rev);

                if (stopCompaction.get()) {
                    return;
                }

                compactForKey(entry.getKey(), toLongArray(entry.getValue()), revision);
            }
        }

        synchronized (mux) {
            for (Iterator<Map.Entry<Long, HybridTimestamp>> it = revToTsMap.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, HybridTimestamp> e = it.next();

                if (e.getKey() <= revision) {
                    it.remove();

                    tsToRevMap.remove(e.getValue().longValue());
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void stopCompaction() {
        stopCompaction.set(true);
    }

    @Override
    public void close() {
        stopCompaction();

        watchProcessor.close();
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        synchronized (mux) {
            try {
                Files.createDirectories(snapshotPath);

                Path snapshotFile = snapshotPath.resolve(SimpleInMemoryKeyValueStorageSnapshot.FILE_NAME);

                assertTrue(IgniteUtils.deleteIfExists(snapshotFile), snapshotFile.toString());

                Files.createFile(snapshotFile);

                Map<Long, Map<byte[], ValueSnapshot>> revsIdxCopy = revsIdx.entrySet().stream()
                        .collect(toMap(
                                Map.Entry::getKey,
                                revIdxEntry -> revIdxEntry.getValue()
                                        .entrySet()
                                        .stream()
                                        .collect(toMap(Map.Entry::getKey, e -> new ValueSnapshot(e.getValue())))
                        ));

                var snapshot = new SimpleInMemoryKeyValueStorageSnapshot(
                        Map.copyOf(keysIdx),
                        Map.copyOf(tsToRevMap),
                        Map.copyOf(revToTsMap),
                        revsIdxCopy,
                        rev,
                        savedCompactionRevision
                );

                byte[] snapshotBytes = ByteUtils.toBytes(snapshot);

                Files.write(snapshotFile, snapshotBytes, WRITE);

                return nullCompletedFuture();
            } catch (Throwable t) {
                return failedFuture(t);
            }
        }
    }

    @Override
    public void restoreSnapshot(Path snapshotPath) {
        synchronized (mux) {
            try {
                keysIdx.clear();
                tsToRevMap.clear();
                revToTsMap.clear();
                revsIdx.clear();

                Path snapshotFile = snapshotPath.resolve(SimpleInMemoryKeyValueStorageSnapshot.FILE_NAME);

                assertTrue(Files.exists(snapshotPath), snapshotFile.toString());

                byte[] snapshotBytes = Files.readAllBytes(snapshotFile);

                var snapshot = (SimpleInMemoryKeyValueStorageSnapshot) ByteUtils.fromBytes(snapshotBytes);

                keysIdx.putAll(snapshot.keysIdx);
                tsToRevMap.putAll(snapshot.tsToRevMap);
                revToTsMap.putAll(snapshot.revToTsMap);
                snapshot.revsIdx.forEach((revision, entries) -> {
                    TreeMap<byte[], Value> entries0 = new TreeMap<>(CMP);
                    entries.forEach((keyBytes, valueSnapshot) -> entries0.put(keyBytes, valueSnapshot.toValue()));

                    revsIdx.put(revision, entries0);
                });

                rev = snapshot.rev;
                compactionRevision = snapshot.savedCompactionRevision;
                savedCompactionRevision = snapshot.savedCompactionRevision;
            } catch (Throwable t) {
                throw new MetaStorageException(RESTORING_STORAGE_ERR, t);
            }
        }
    }

    private void doRemove(byte[] key, long curRev, HybridTimestamp opTs) {
        Entry e = doGet(key, curRev);

        if (e.empty() || e.tombstone()) {
            return;
        }

        doPut(key, TOMBSTONE, curRev, opTs);
    }

    /**
     * Compacts the key, see the documentation of {@link KeyValueStorage#compact} for examples.
     *
     * @param key Target key.
     * @param revs Key revisions.
     * @param compactionRevision Revision up to which (inclusively) the key will be compacted.
     * @throws MetaStorageException If failed.
     */
    private void compactForKey(byte[] key, long[] revs, long compactionRevision) {
        int indexToCompact = indexToCompact(revs, compactionRevision, revision -> isTombstoneForCompaction(key, revision));

        if (indexToCompact == NOT_FOUND) {
            return;
        }

        // Let's deal with the key revisions.
        if (indexToCompact == revs.length - 1) {
            keysIdx.remove(key);
        } else {
            List<Long> keyRevisions = keysIdx.get(key);

            assert keyRevisions != null : toUtf8String(key);

            keyRevisions.subList(0, indexToCompact + 1).clear();
        }

        // Let's deal with the key values.
        for (int revisionIndex = 0; revisionIndex < indexToCompact; revisionIndex++) {
            long revision = revs[revisionIndex];

            NavigableMap<byte[], Value> valueByKey = revsIdx.get(revision);

            valueByKey.remove(key);

            if (valueByKey.isEmpty()) {
                revsIdx.remove(revision);
            }
        }
    }

    private List<Entry> doGetAll(List<byte[]> keys, long revUpperBound) {
        assert !keys.isEmpty();
        assert revUpperBound >= 0 : revUpperBound;

        var res = new ArrayList<Entry>(keys.size());

        for (byte[] key : keys) {
            res.add(doGet(key, revUpperBound));
        }

        return res;
    }

    private Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= 0 : revUpperBound;

        long[] keyRevisions = toLongArray(keysIdx.get(key));
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

            return EntryImpl.empty(key);
        }

        long revision = keyRevisions[maxRevisionIndex];

        Value value = getValue(key, revision);

        if (revUpperBound <= compactionRevision && (!isLastIndex(keyRevisions, maxRevisionIndex) || value.tombstone())) {
            throw new CompactedException(revUpperBound, compactionRevision);
        }

        return EntryImpl.toEntry(key, revision, value);
    }

    private List<Entry> doGet(byte[] key, long revLowerBound, long revUpperBound) {
        assert revLowerBound >= 0 : revLowerBound;
        assert revUpperBound >= 0 : revUpperBound;
        assert revUpperBound >= revLowerBound : "revLowerBound=" + revLowerBound + ", revUpperBound=" + revUpperBound;

        long[] keyRevisions = toLongArray(keysIdx.get(key));

        int minRevisionIndex = minRevisionIndex(keyRevisions, revLowerBound);
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (minRevisionIndex == NOT_FOUND || maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);

            return List.of();
        }

        var entries = new ArrayList<Entry>();

        for (int i = minRevisionIndex; i <= maxRevisionIndex; i++) {
            long revision = keyRevisions[i];

            Value value = getValue(key, revision);

            if (revision <= compactionRevision && (!isLastIndex(keyRevisions, i) || value.tombstone())) {
                continue;
            }

            entries.add(EntryImpl.toEntry(key, revision, value));
        }

        if (entries.isEmpty()) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);
        }

        return entries;
    }

    private void doPut(byte[] key, byte[] bytes, long curRev, HybridTimestamp opTs) {
        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        revs.add(curRev);

        // Update revsIdx.
        Value val = new Value(bytes, opTs);

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

        var updatedEntry = new EntryImpl(key, val.tombstone() ? null : bytes, curRev, val.operationTimestamp());

        updatedEntries.add(updatedEntry);

    }

    private void doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList, HybridTimestamp opTs) {
        synchronized (mux) {
            // Update revsIdx.
            NavigableMap<byte[], Value> entries = new TreeMap<>(CMP);

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                byte[] bytes = bytesList.get(i);

                // Update keysIdx.
                List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

                revs.add(curRev);

                Value val = new Value(bytes, opTs);

                entries.put(key, val);

                updatedEntries.add(new EntryImpl(key, bytes, curRev, opTs));

                revsIdx.put(curRev, entries);
            }

            updateRevision(curRev, opTs);

        }
    }

    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    @Override
    public void registerRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.registerRevisionUpdateListener(listener);
    }

    @Override
    public void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.unregisterRevisionUpdateListener(listener);
    }

    @Override
    public CompletableFuture<Void> notifyRevisionUpdateListenerOnStart(long newRevision) {
        return watchProcessor.notifyUpdateRevisionListeners(newRevision);
    }

    @Override
    public void advanceSafeTime(HybridTimestamp newSafeTime) {
        synchronized (mux) {
            if (!areWatchesEnabled) {
                return;
            }

            watchProcessor.advanceSafeTime(newSafeTime);
        }
    }

    @Override
    public void saveCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        synchronized (mux) {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            savedCompactionRevision = revision;
        }
    }

    @Override
    public void setCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        synchronized (mux) {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            compactionRevision = revision;
        }
    }

    @Override
    public long getCompactionRevision() {
        synchronized (mux) {
            return compactionRevision;
        }
    }

    @Override
    public long checksum(long revision) {
        throw new UnsupportedOperationException();
    }

    private static long[] toLongArray(@Nullable List<Long> list) {
        if (list == null) {
            return LONG_EMPTY_ARRAY;
        }

        var array = new long[list.size()];

        for (int i = 0; i < array.length; i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    private boolean isTombstoneForCompaction(byte[] key, long revision) {
        NavigableMap<byte[], Value> kv = revsIdx.get(revision);

        Value value = kv.get(key);

        assert value != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return value.tombstone();
    }

    private Value getValue(byte[] key, long revision) {
        Value value = getValueNullable(key, revision);

        assert value != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return value;
    }

    private @Nullable Value getValueNullable(byte[] key, long revision) {
        NavigableMap<byte[], Value> valueByKey = revsIdx.get(revision);

        assert valueByKey != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return valueByKey.get(key);
    }

    private Cursor<Entry> doRange(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        assert revUpperBound >= 0 : revUpperBound;

        CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

        SortedMap<byte[], List<Long>> subMap = keyTo == null
                ? keysIdx.tailMap(keyFrom)
                : keysIdx.subMap(keyFrom, keyTo);

        return subMap.entrySet().stream()
                .map(e -> {
                    byte[] key = e.getKey();
                    long[] keyRevisions = toLongArray(e.getValue());

                    int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

                    if (maxRevisionIndex == NOT_FOUND) {
                        return EntryImpl.empty(key);
                    }

                    long revision = keyRevisions[maxRevisionIndex];
                    Value value = getValueNullable(key, revision);

                    // Value may be null if the compaction has removed it in parallel.
                    if (value == null || (revision <= compactionRevision && value.tombstone())) {
                        return EntryImpl.empty(key);
                    }

                    return EntryImpl.toEntry(key, revision, value);
                })
                .filter(e -> !e.empty())
                .collect(collectingAndThen(toList(), Cursor::fromIterable));
    }
}
