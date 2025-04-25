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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOT_FOUND;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertCompactionRevisionLessThanCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertRequestedRevisionLessThanOrEqualToCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.indexToCompact;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.maxRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.toUtf8String;
import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.toIdempotentCommandKey;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker.TrackingToken;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory key/value storage for tests.
 */
public class SimpleInMemoryKeyValueStorage extends AbstractKeyValueStorage {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * Keys index. Value is the list of all revisions under which entry corresponding to the key was modified.
     *
     * <p>Concurrent map to avoid {@link java.util.ConcurrentModificationException} on compaction.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private final NavigableMap<byte[], List<Long>> keysIdx = new ConcurrentSkipListMap<>(KEY_COMPARATOR);

    /**
     * Timestamp to revision mapping.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private final NavigableMap<Long, Long> tsToRevMap = new TreeMap<>();

    /**
     * Revision to timestamp mapping.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private final Map<Long, HybridTimestamp> revToTsMap = new HashMap<>();

    /**
     * Revisions index. Value contains all entries which were modified under particular revision.
     *
     * <p>Concurrent map to avoid {@link java.util.ConcurrentModificationException} on compaction.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private final NavigableMap<Long, NavigableMap<byte[], Value>> revsIdx = new ConcurrentSkipListMap<>();

    /**
     * Last update index.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private long index;

    /**
     * Last update term.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private long term;

    /** Last saved configuration. */
    private byte @Nullable [] configuration;

    /**
     * Last {@link #saveCompactionRevision saved} compaction revision.
     *
     * <p>Used only when working with snapshots.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private long savedCompactionRevision = -1;

    /** Multi-threaded access is guarded by {@link #rwLock}. */
    private boolean areWatchesEnabled = false;

    /** Multi-threaded access is guarded by {@link #rwLock}. */
    private final List<Entry> updatedEntries = new ArrayList<>();

    /** Constructor. */
    public SimpleInMemoryKeyValueStorage(String nodeName) {
        this(nodeName, new ReadOperationForCompactionTracker());
    }

    /** Constructor. */
    public SimpleInMemoryKeyValueStorage(String nodeName, ReadOperationForCompactionTracker readOperationForCompactionTracker) {
        super(nodeName, new NoOpFailureManager(), readOperationForCompactionTracker);
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void setIndexAndTerm(long index, long term) {
        rwLock.writeLock().lock();

        try {
            this.index = index;
            this.term = term;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public @Nullable IndexWithTerm getIndexWithTerm() {
        rwLock.writeLock().lock();

        try {
            if (index == 0) {
                return null;
            }

            return new IndexWithTerm(index, term);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void saveConfiguration(byte[] configuration, long index, long term) {
        rwLock.writeLock().lock();

        try {
            this.configuration = configuration;

            setIndexAndTerm(index, term);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public byte @Nullable [] getConfiguration() {
        rwLock.writeLock().lock();

        try {
            return configuration;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Entry get(byte[] key) {
        rwLock.readLock().lock();

        try {
            return super.get(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Entry get(byte[] key, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return super.get(key, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        rwLock.readLock().lock();

        try {
            return super.getAll(keys);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return super.getAll(keys, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public long revision() {
        rwLock.readLock().lock();

        try {
            return rev;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            doPut(key, value, curRev, context.timestamp);

            updateRevision(curRev, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void updateRevision(long newRevision, KeyValueUpdateContext context) {
        setIndexAndTerm(context.index, context.term);

        rev = newRevision;

        HybridTimestamp ts = context.timestamp;
        tsToRevMap.put(ts.longValue(), rev);
        revToTsMap.put(rev, ts);

        notifyWatches();

        notifyRevisionsUpdate();
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void remove(byte[] key, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            doRemove(key, curRev, context.timestamp);
            updateRevision(curRev, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeAll(List<byte[]> keys, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
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

            doPutAll(curRev, existingKeys, vals, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeByPrefix(byte[] prefix, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try (Cursor<Entry> entryCursor = range(prefix, nextKey(prefix))) {
            long curRev = rev + 1;

            var keys = new ArrayList<byte[]>();

            var vals = new ArrayList<byte[]>();

            for (Entry e : entryCursor) {
                if (e.empty() || e.tombstone()) {
                    continue;
                }

                keys.add(e.key());

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, keys, vals, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure,
            KeyValueUpdateContext context,
            CommandId commandId
    ) {
        rwLock.writeLock().lock();

        try {
            HybridTimestamp opTs = context.timestamp;

            Collection<Entry> e = getAll(Arrays.asList(condition.keys()));

            boolean branch = condition.test(e.toArray(new Entry[]{}));

            List<Operation> ops = branch ? new ArrayList<>(success) : new ArrayList<>(failure);

            // In case of in-memory storage, there's no sense in "persisting" invoke result, however same persistent source operations
            // were added in order to have matching revisions count through all storages.
            ops.add(Operations.put(
                    toIdempotentCommandKey(commandId),
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

            updateRevision(curRev, context);

            return branch;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public StatementResult invoke(If iif, KeyValueUpdateContext context, CommandId commandId) {
        rwLock.writeLock().lock();

        try {
            HybridTimestamp opTs = context.timestamp;

            If currIf = iif;
            while (true) {
                Collection<Entry> e = getAll(Arrays.asList(currIf.cond().keys()));

                Statement branch = (currIf.cond().test(e.toArray(new Entry[]{}))) ? currIf.andThen() : currIf.orElse();

                if (branch.isTerminal()) {
                    long curRev = rev + 1;

                    List<Operation> ops = new ArrayList<>(branch.update().operations());

                    // In case of in-memory storage, there's no sense in "persisting" invoke result, however same persistent source
                    // operations were added in order to have matching revisions count through all storages.
                    ops.add(Operations.put(toIdempotentCommandKey(commandId), branch.update().result().result()));

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

                    updateRevision(curRev, context);

                    return branch.update().result();
                } else {
                    currIf = branch.iif();
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo) {
        rwLock.readLock().lock();

        try {
            return doRange(keyFrom, keyTo, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doRange(keyFrom, keyTo, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public HybridTimestamp timestampByRevision(long revision) {
        assert revision >= 0 : revision;

        rwLock.readLock().lock();

        try {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            HybridTimestamp timestamp = revToTsMap.get(revision);

            if (timestamp == null) {
                throw new CompactedException("Requested revision has already been compacted: " + revision);
            }

            return timestamp;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public long revisionByTimestamp(HybridTimestamp timestamp) {
        rwLock.readLock().lock();

        try {
            Map.Entry<Long, Long> revisionEntry = tsToRevMap.floorEntry(timestamp.longValue());

            if (revisionEntry == null) {
                throw new CompactedException("Revisions less than or equal to the requested one are already compacted: " + timestamp);
            }

            return revisionEntry.getValue();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void startWatches(long startRevision, WatchEventHandlingCallback callback) {
        assert startRevision > 0 : startRevision;

        rwLock.readLock().lock();

        try {
            watchProcessor.setWatchEventHandlingCallback(callback);

            fillNotifyWatchProcessorEventsFromStorage(startRevision);

            drainNotifyWatchProcessorEventsBeforeStartingWatches();

            areWatchesEnabled = true;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void fillNotifyWatchProcessorEventsFromStorage(long startRevision) {
        long minWatchRevision = Math.max(startRevision, watchProcessor.minWatchRevision().orElse(-1));

        revsIdx.tailMap(minWatchRevision)
                .forEach((revision, entries) -> {
                    entries.forEach((key, value) -> {
                        var entry = new EntryImpl(key, value.bytes(), revision, value.operationTimestamp());

                        updatedEntries.add(entry);
                    });

                    fillNotifyWatchProcessorEventsFromUpdatedEntries();
                });

        for (long revision = startRevision; revision <= rev; revision++) {
            HybridTimestamp time = revToTsMap.get(revision);

            assert time != null : revision;

            notifyWatchProcessorEventsBeforeStartingWatches.add(new UpdateOnlyRevisionEvent(revision, time));
        }
    }

    private void fillNotifyWatchProcessorEventsFromUpdatedEntries() {
        if (updatedEntries.isEmpty()) {
            return;
        }

        long revision = updatedEntries.get(0).revision();

        HybridTimestamp ts = revToTsMap.get(revision);
        assert ts != null : revision;

        var event = new UpdateEntriesEvent(List.copyOf(updatedEntries), ts);

        notifyWatchProcessor(event);

        updatedEntries.clear();
    }

    private void notifyWatches() {
        if (!areWatchesStarted()) {
            updatedEntries.clear();

            return;
        }

        long newRevision = rev;

        HybridTimestamp ts = revToTsMap.get(newRevision);
        assert ts != null : newRevision;

        watchProcessor.notifyWatches(newRevision, List.copyOf(updatedEntries), ts);
        updatedEntries.clear();
    }

    @Override
    public void compact(long revision) {
        assert revision >= 0 : revision;

        for (Map.Entry<byte[], List<Long>> entry : keysIdx.entrySet()) {
            rwLock.writeLock().lock();

            try {
                assertCompactionRevisionLessThanCurrent(revision, rev);

                if (stopCompaction.get()) {
                    return;
                }

                compactForKey(entry.getKey(), toLongArray(entry.getValue()), revision);
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        rwLock.writeLock().lock();

        try {
            for (Iterator<Map.Entry<Long, HybridTimestamp>> it = revToTsMap.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, HybridTimestamp> e = it.next();

                if (e.getKey() <= revision) {
                    it.remove();

                    tsToRevMap.remove(e.getValue().longValue());
                } else {
                    break;
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        stopCompaction();

        watchProcessor.close();
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        rwLock.writeLock().lock();

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
                    savedCompactionRevision,
                    term,
                    index
            );

            byte[] snapshotBytes = ByteUtils.toBytes(snapshot);

            Files.write(snapshotFile, snapshotBytes, WRITE);

            return nullCompletedFuture();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void restoreSnapshot(Path snapshotPath) {
        rwLock.writeLock().lock();

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
                TreeMap<byte[], Value> entries0 = new TreeMap<>(KEY_COMPARATOR);
                entries.forEach((keyBytes, valueSnapshot) -> entries0.put(keyBytes, valueSnapshot.toValue()));

                revsIdx.put(revision, entries0);
            });

            rev = snapshot.rev;
            compactionRevision = snapshot.savedCompactionRevision;
            savedCompactionRevision = snapshot.savedCompactionRevision;
            term = snapshot.term;
            index = snapshot.index;

            notifyRevisionsUpdate();
        } catch (Throwable t) {
            throw new MetaStorageException(RESTORING_STORAGE_ERR, t);
        } finally {
            rwLock.writeLock().unlock();
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
                        entries = new TreeMap<>(KEY_COMPARATOR);
                    }

                    entries.put(key, val);

                    return entries;
                }
        );

        var updatedEntry = new EntryImpl(key, val.tombstone() ? null : bytes, curRev, val.operationTimestamp());

        updatedEntries.add(updatedEntry);
    }

    private void doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList, KeyValueUpdateContext context) {
        HybridTimestamp opTs = context.timestamp;

        // Update revsIdx.
        NavigableMap<byte[], Value> entries = new TreeMap<>(KEY_COMPARATOR);

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

        updateRevision(curRev, context);
    }

    @Override
    public void saveCompactionRevision(long revision, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            super.saveCompactionRevision(revision, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void saveCompactionRevision(long revision, KeyValueUpdateContext context, boolean advanceSafeTime) {
        savedCompactionRevision = revision;

        setIndexAndTerm(context.index, context.term);

        if (advanceSafeTime && areWatchesStarted()) {
            watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
        }
    }

    @Override
    public void setCompactionRevision(long revision) {
        rwLock.writeLock().lock();

        try {
            super.setCompactionRevision(revision);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void updateCompactionRevision(long compactionRevision, KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            super.updateCompactionRevision(compactionRevision, context);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public long checksum(long revision) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChecksumAndRevisions checksumAndRevisions(long revision) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        rwLock.writeLock().lock();

        try {
            this.index = 0;
            this.term = 0;
            this.configuration = null;

            this.rev = 0;
            this.compactionRevision = -1;
            this.savedCompactionRevision = -1;

            this.keysIdx.clear();
            this.revsIdx.clear();

            this.tsToRevMap.clear();
            this.revToTsMap.clear();

            this.updatedEntries.clear();
        } finally {
            rwLock.writeLock().unlock();
        }
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

    @Override
    protected long[] keyRevisionsForOperation(byte[] key) {
        return toLongArray(keysIdx.get(key));
    }

    @Override
    protected Value valueForOperation(byte[] key, long revision) {
        Value value = getValueNullable(key, revision);

        assert value != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return value;
    }

    @Override
    protected boolean areWatchesStarted() {
        return areWatchesEnabled;
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

        List<Entry> entries = subMap.entrySet().stream()
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
                    if (value == null || value.tombstone()) {
                        return EntryImpl.empty(key);
                    }

                    return EntryImpl.toEntry(key, revision, value);
                })
                .filter(e -> !e.empty())
                .collect(toList());

        Iterator<Entry> iterator = entries.iterator();

        //noinspection resource
        TrackingToken token = readOperationForCompactionTracker.track(revUpperBound, this::revision, this::getCompactionRevision);

        return new Cursor<>() {
            @Override
            public void close() {
                token.close();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry next() {
                return iterator.next();
            }
        };
    }

    @Override
    public CompletableFuture<Void> flush() {
        return nullCompletedFuture();
    }
}
