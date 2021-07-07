/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileWriter;
import org.rocksdb.StringAppendOperator;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * Key-value storage based on RocksDB.
 * Keys are stored with revision.
 * Values are stored with update counter and a boolean flag which represenets whether this record is a tombstone.
 * <br/>
 * Key: [8 bytes revision, N bytes key itself].
 * <br/>
 * Value: [8 bytes update counter, 1 byte tombstone flag, N bytes value].
 */
public class RocksDBKeyValueStorage implements KeyValueStorage {
    /** Database snapshot file name. */
    private static final String SNAPSHOT_FILE_NAME = "db.snapshot";

    /** Revision key. */
    private static final byte[] REVISION_KEY = keyToRocksKey(-1, "SYSTEM_REVISION_KEY".getBytes());

    /** Update counter key. */
    private static final byte[] UPDATE_COUNTER_KEY = keyToRocksKey(-1, "SYSTEM_UPDATE_COUNTER_KEY".getBytes());

    static {
        RocksDB.loadLibrary();
    }

    /** RockDB options. */
    private final Options options;

    /** RocksDb instance. */
    private final RocksDB db;

    /** RW lock. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Thread-pool for a snapshot operation execution. */
    private final Executor snapshotExecutor = Executors.newSingleThreadExecutor();
    
    /**
     * Special value for revision number which means that operation should be applied
     * to the latest revision of an entry.
     */
    private static final long LATEST_REV = -1;

    /** Lexicographic order comparator. */
    private final Comparator<byte[]> CMP = Arrays::compare;

    /** Keys index. Value is the list of all revisions under which entry corresponding to the key was modified. */
    private NavigableMap<byte[], List<Long>> keysIdx = new TreeMap<>(CMP);

    /** Revision. Will be incremented for each single-entry or multi-entry update operation. */
    private long rev;

    /** Update counter. Will be incremented for each update of any particular entry. */
    private long updCntr;

    /**
     * Constructor.
     *
     * @param dbPath RocksDB path.
     */
    public RocksDBKeyValueStorage(Path dbPath) {
        try {
            options = new Options();

            options.setCreateIfMissing(true);

            options.useFixedLengthPrefixExtractor(8);

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        }
        catch (Exception e) {
            close();

            throw new IgniteInternalException("Failed to start the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            IgniteUtils.closeAll(Set.of(options, db));
        }
        catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return createSnapshot(snapshotPath);
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path snapshotPath) {
        readSnapshot(snapshotPath);
    }

    /**
     * Reads a snapsot from path and ingest this key-value storage.
     *
     * @param path Snapshot path.
     */
    public void readSnapshot(Path path) {
        Lock writeLock = this.rwLock.writeLock();
        writeLock.lock();

        Path snapshotPath = path.resolve(SNAPSHOT_FILE_NAME);

        if (!Files.exists(snapshotPath))
            throw new IgniteInternalException("Snapshot not found within path: " + path);

        try (IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
            this.db.ingestExternalFile(Collections.singletonList(snapshotPath.toString()), ingestOptions);
            buildKeyIndex();

            rev = ByteUtils.bytesToLong(this.db.get(REVISION_KEY));

            updCntr = ByteUtils.bytesToLong(this.db.get(UPDATE_COUNTER_KEY));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException("Fail to ingest sst file at path: " + path, e);
        }
        finally {
            writeLock.unlock();
        }
    }

    /**
     * Builds an index of this storage.
     *
     * @throws RocksDBException If failed.
     */
    private void buildKeyIndex() throws RocksDBException {
        try (RocksIterator iterator = this.db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] rocksKey = iterator.key();
                byte[] key = rocksKeyToBytes(rocksKey);
                long revision = ByteUtils.bytesToLong(rocksKey);
                if (revision == -1)
                    continue;

                List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());
                revs.add(revision);
            }
        }
    }

    /**
     * Captures a snapshot.
     *
     * @param snapsthotPath Snapshot path.
     * @return Future that represents a state of the operation.
     */
    public CompletableFuture<Void> createSnapshot(Path snapsthotPath) {
        Lock readLock = this.rwLock.readLock();
        readLock.lock();

        try {
            Path tempPath = Paths.get(snapsthotPath.toString() + ".temp");

            IgniteUtils.delete(tempPath);
            Files.createDirectories(tempPath);

            CompletableFuture<Void> snapshotFuture = new CompletableFuture<>();

            CompletableFuture<Void> sstFuture = createSstFile(tempPath);

            sstFuture.whenComplete((aVoid, throwable) -> {
                if (throwable == null) {
                    try {
                        IgniteUtils.delete(snapsthotPath);

                        Files.move(tempPath, snapsthotPath);

                        snapshotFuture.complete(null);
                    }
                    catch (Throwable t) {
                        snapshotFuture.completeExceptionally(t);
                    }
                }
                else
                    snapshotFuture.completeExceptionally(throwable);
            });

            return snapshotFuture;
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Creates a SST file from {@link #db}.
     *
     * @param path Path to store SST file at.
     * @return Future that represents a state of the operation.
     */
    CompletableFuture<Void> createSstFile(Path path) {
        CompletableFuture<Void> sstFuture = new CompletableFuture<>();

        snapshotExecutor.execute(() -> {
            Lock readLock = this.rwLock.readLock();
            readLock.lock();

            Snapshot snapshot = this.db.getSnapshot();

            try (
                ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                EnvOptions envOptions = new EnvOptions();
                Options options = new Options().setMergeOperator(new StringAppendOperator());
                RocksIterator it = this.db.newIterator(readOptions);
                SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)
            ) {
                Path sstFile = path.resolve(SNAPSHOT_FILE_NAME);

                it.seekToFirst();

                sstFileWriter.open(sstFile.toString());

                long count = 0;

                for (;;) {
                    if (!it.isValid()) {
                        break;
                    }
                    byte[] key = it.key();

                    sstFileWriter.put(key, it.value());

                    ++count;

                    it.next();
                }

                if (count == 0)
                    sstFileWriter.close();
                else
                    sstFileWriter.finish();

                sstFuture.complete(null);
            }
            catch (Throwable t) {
                sstFuture.completeExceptionally(t);
            }
            finally {
                readLock.unlock();

                // Nothing to release, rocksDB never own the pointer for a snapshot.
                snapshot.close();
                // The pointer to the snapshot is released by the database instance.
                this.db.releaseSnapshot(snapshot);
            }
        });

        return sstFuture;
    }

    /** {@inheritDoc} */
    @Override public long revision() {
        return rev;
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return updCntr;
    }

    /**
     * Puts a value into {@link #db}.
     *
     * @param key Key.
     * @param value Value.
     */
    private void setValue(byte[] key, byte[] value) {
        try {
            this.db.put(key, value);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Updates the revision of this storage.
     *
     * @param newRevision New revision.
     */
    private void updateRevision(long newRevision) {
        setValue(REVISION_KEY, ByteUtils.longToBytes(newRevision));

        rev = newRevision;
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            doPut(key, value, curRev);

            updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] bytes) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            long lastRev = doPut(key, bytes, curRev);

            updateRevision(curRev);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values);

            updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        Collection<Entry> res;

        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            doPutAll(curRev, keys, values);

            updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return doGet(key, LATEST_REV, false);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key, long rev) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return doGet(key, rev, true);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys) {
        return doGetAll(keys, LATEST_REV);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return doGetAll(keys, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public void remove(byte[] key) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            if (doRemove(key, curRev))
                updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndRemove(byte[] key) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return e;

            return getAndPut(key, TOMBSTONE);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<byte[]> keys) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV, false);

                if (e.empty() || e.tombstone())
                    continue;

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);

            updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV, false);

                res.add(e);

                if (e.empty() || e.tombstone())
                    continue;

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            doPutAll(curRev, existingKeys, vals);

            updateRevision(curRev);
        }
        finally {
            lock.unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        Lock lock = rwLock.writeLock();
        lock.lock();

        try {
            Entry e = get(condition.key());

            boolean branch = condition.test(e);

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
                        throw new IllegalArgumentException("Unknown operation type: " + op.type());
                }
            }

            if (modified)
                updateRevision(curRev);

            return branch;
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        return new RocksDBKeyValueStorage.RangeCursor(keyFrom, keyTo, rev);
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return new RocksDBKeyValueStorage.RangeCursor(keyFrom, keyTo, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte[] keyTo, long rev) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new RocksDBKeyValueStorage.WatchCursor(rev, k ->
            CMP.compare(keyFrom, k) <= 0 && (keyTo == null || CMP.compare(k, keyTo) < 0)
        );
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new RocksDBKeyValueStorage.WatchCursor(rev, k -> CMP.compare(k, key) == 0);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        return new RocksDBKeyValueStorage.WatchCursor(rev, keySet::contains);
    }

    /** {@inheritDoc} */
    @Override public void compact() {
        Lock lock = rwLock.writeLock();
        lock.lock();

        try {
            NavigableMap<byte[], List<Long>> compactedKeysIdx = new TreeMap<>(CMP);

            keysIdx.forEach((key, revs) -> {
                try {
                    compactForKey(key, revs, compactedKeysIdx);
                }
                catch (RocksDBException e) {
                    throw new IgniteInternalException(e);
                }
            });

            keysIdx = compactedKeysIdx;
        }
        finally {
            lock.unlock();
        }
    }

    /** */
    private boolean doRemove(byte[] key, long curRev) {
        Entry e = doGet(key, LATEST_REV, false);

        if (e.empty() || e.tombstone())
            return false;

        doPut(key, TOMBSTONE, curRev);

        return true;
    }

    /** */
    private void compactForKey(
        byte[] key,
        List<Long> revs,
        NavigableMap<byte[], List<Long>> compactedKeysIdx
    ) throws RocksDBException {
        long lastRev = lastRevision(revs);

        for (int i = 0; i < revs.size(); i++) {
            Long revision = revs.get(i);

            byte[] rocksKey = keyToRocksKey(revision, key);

            if (i == revs.size() - 1) {
                Value value = bytesToValue(db.get(rocksKey));

                if (value.tombstone())
                    this.db.delete(rocksKey);
                else
                    compactedKeysIdx.put(key, listOf(lastRev));
            }
            else
                this.db.delete(rocksKey);
        }
    }

    /** */
    @NotNull
    private Collection<Entry> doGetAll(Collection<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev > 0 || rev == LATEST_REV : "Revision must be positive or " + LATEST_REV + '.';

        Collection<Entry> res = new ArrayList<>(keys.size());

        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            for (byte[] key : keys) {
                res.add(doGet(key, rev, false));
            }
        }
        finally {
            lock.unlock();
        }

        return res;
    }

    /** */
    @NotNull
    private Entry doGet(byte[] key, long rev, boolean exactRev) {
        assert rev == LATEST_REV && !exactRev || rev > LATEST_REV :
            "Invalid arguments: [rev=" + rev + ", exactRev=" + exactRev + ']';

        List<Long> revs = keysIdx.get(key);

        if (revs == null || revs.isEmpty())
            return Entry.empty(key);

        long lastRev;

        if (rev == LATEST_REV)
            lastRev = lastRevision(revs);
        else
            lastRev = exactRev ? rev : maxRevision(revs, rev);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1)
            return Entry.empty(key);

        return doGetValue(key, lastRev);
    }

    /**
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then
     * {@code -1} will be returned.
     *
     * @param revs Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Appropriate revision or {@code -1} if there is no such revision.
     */
    private static long maxRevision(List<Long> revs, long upperBoundRev) {
        int i = revs.size() - 1;

        for (; i >= 0; i--) {
            long rev = revs.get(i);

            if (rev <= upperBoundRev)
                return rev;
        }

        return -1;
    }

    /**
     * Add a revision to a key.
     *
     * @param revision Revision.
     * @param key Key.
     * @return Key with a revision.
     */
    private static byte[] keyToRocksKey(long revision, byte[] key) {
        byte[] buffer = new byte[Long.BYTES + key.length];

        ByteUtils.toBytes(revision, buffer, 0, Long.BYTES);

        System.arraycopy(key, 0, buffer, Long.BYTES, key.length);

        return buffer;
    }

    /**
     * Get a key from a key with revision.
     *
     * @param rocksKey Key with a revision.
     * @return Key without a revision.
     */
    private static byte[] rocksKeyToBytes(byte[] rocksKey) {
        byte[] buffer = new byte[rocksKey.length - Long.BYTES];

        System.arraycopy(rocksKey, Long.BYTES, buffer, 0, buffer.length);

        return buffer;
    }

    /**
     * Build a value from a byte array.
     *
     * @param valueBytes Value byte array.
     * @return Value.
     */
    private static Value bytesToValue(byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.wrap(valueBytes);

        long updateCounter = buf.getLong();

        boolean hasValue = buf.get() != 0;

        byte[] val;
        if (hasValue) {
            val = new byte[buf.remaining()];
            buf.get(val);
        }
        else
            val = TOMBSTONE;

        return new Value(val, updateCounter);
    }

    /**
     * Add an update counter and a tombstone flag to a value.
     * @param value Value byte array.
     * @param updateCounter Update counter.
     * @return Value with an update counter and a tombstone.
     */
    private static byte[] valueToBytes(byte[] value, long updateCounter) {
        byte[] bytes = new byte[Long.BYTES + Byte.BYTES + value.length];

        ByteUtils.toBytes(updateCounter, bytes, 0, Long.BYTES);

        bytes[Long.BYTES] = (byte) (value == TOMBSTONE ? 0 : 1);

        System.arraycopy(value, 0, bytes, Long.BYTES + Byte.BYTES, value.length);

        return bytes;
    }

    /** */
    @NotNull
    private Entry doGetValue(byte[] key, long lastRev) {
        if (lastRev == 0)
            return Entry.empty(key);

        byte[] valueBytes;

        try {
            valueBytes = db.get(keyToRocksKey(lastRev, key));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }

        if (valueBytes == null || valueBytes.length == 0)
            return Entry.empty(key);

        Value lastVal = bytesToValue(valueBytes);

        if (lastVal.tombstone())
            return Entry.tombstone(key, lastRev, lastVal.updateCounter());

        return new Entry(key, lastVal.bytes(), lastRev, lastVal.updateCounter());
    }

    /** */
    private long doPut(byte[] key, byte[] bytes, long curRev) {
        long curUpdCntr = ++updCntr;

        setValue(UPDATE_COUNTER_KEY, ByteUtils.longToBytes(curUpdCntr));

        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

        revs.add(curRev);

        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(bytes, curUpdCntr);

        setValue(rocksKey, rocksValue);

        return lastRev;
    }

    /** */
    private long doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                byte[] bytes = bytesList.get(i);

                doPut(key, bytes, curRev);
            }

            return curRev;
        }
        finally {
            lock.unlock();
        }
    }

    /** */
    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    /** */
    private static List<Long> listOf(long val) {
        List<Long> res = new ArrayList<>();

        res.add(val);

        return res;
    }

    /** */
    private class RangeCursor implements Cursor<Entry> {
        /** */
        private final byte[] keyFrom;

        /** */
        private final byte[] keyTo;

        /** */
        private final long rev;

        /** */
        private final Iterator<Entry> it;

        /** */
        private Entry nextRetEntry;

        /** */
        private byte[] lastRetKey;

        /** */
        private boolean finished;

        /** */
        RangeCursor(byte[] keyFrom, byte[] keyTo, long rev) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.rev = rev;
            this.it = createIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Entry next() {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Entry> iterator() {
            return it;
        }

        @NotNull
        Iterator<Entry> createIterator() {
            return new Iterator<>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    Lock lock = rwLock.readLock();
                    lock.lock();

                    try {
                        while (true) {
                            if (finished)
                                return false;

                            if (nextRetEntry != null)
                                return true;

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

                                if (lastRev == -1)
                                    continue;

                                Entry entry = doGetValue(key, lastRev);

                                assert !entry.empty() : "Iterator should not return empty entry.";

                                nextRetEntry = entry;

                                break;
                            }
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

                /** {@inheritDoc} */
                @Override public Entry next() {
                    Lock lock = rwLock.writeLock();
                    lock.lock();
                    try {
                        while (true) {
                            if (finished)
                                throw new NoSuchElementException();

                            if (nextRetEntry != null) {
                                Entry e = nextRetEntry;

                                nextRetEntry = null;

                                lastRetKey = e.key();

                                return e;
                            } else
                                hasNext();
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            };
        }
    }

    /** */
    private class WatchCursor implements Cursor<WatchEvent> {
        /** */
        private final Predicate<byte[]> p;

        /** */
        private final Iterator<WatchEvent> it;

        /** Options for {@link #nativeIterator}. */
        private final ReadOptions options = new ReadOptions().setPrefixSameAsStart(true);

        /** RocksDB iterator. */
        @Nullable
        private RocksIterator nativeIterator = null;

        /** */
        private long lastRetRev;

        /** */
        private long nextRetRev = -1;

        /** */
        WatchCursor(long rev, Predicate<byte[]> p) {
            this.p = p;
            this.lastRetRev = rev - 1;
            this.it = createIterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public WatchEvent next() {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            IgniteUtils.closeAll(Set.of(nativeIterator, options));
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<WatchEvent> iterator() {
            return it;
        }

        @NotNull
        Iterator<WatchEvent> createIterator() {
            return new Iterator<>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    Lock lock = rwLock.readLock();
                    lock.lock();
                    try {
                        if (nextRetRev != -1)
                            return true;

                        while (true) {
                            long curRev = lastRetRev + 1;

                            byte[] revisionPrefix = new byte[Long.BYTES];
                            ByteUtils.toBytes(curRev, revisionPrefix, 0, Long.BYTES);

                            boolean empty = true;

                            if (nativeIterator == null)
                                nativeIterator = db.newIterator(options);

                            for (nativeIterator.seek(revisionPrefix); nativeIterator.isValid(); nativeIterator.next()) {
                                empty = false;

                                byte[] key = rocksKeyToBytes(nativeIterator.key());

                                if (p.test(key)) {
                                    nextRetRev = curRev;

                                    return true;
                                }
                            }

                            if (empty) {
                                nativeIterator.close();

                                nativeIterator = null;

                                return false;
                            }

                            lastRetRev++;
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }

                /** {@inheritDoc} */
                @Override public WatchEvent next() {
                    Lock lock = rwLock.readLock();
                    lock.lock();
                    try {
                        while (true) {
                            if (nextRetRev != -1) {
                                boolean empty = true;

                                List<EntryEvent> evts = new ArrayList<>();

                                for (; nativeIterator.isValid(); nativeIterator.next()) {
                                    empty = false;

                                    byte[] key = rocksKeyToBytes(nativeIterator.key());

                                    Value val = bytesToValue(nativeIterator.value());

                                    if (p.test(key)) {
                                        Entry newEntry;

                                        if (val.tombstone())
                                            newEntry = Entry.tombstone(key, nextRetRev, val.updateCounter());
                                        else
                                            newEntry = new Entry(key, val.bytes(), nextRetRev, val.updateCounter());

                                        Entry oldEntry = doGet(key, nextRetRev - 1, false);

                                        evts.add(new EntryEvent(oldEntry, newEntry));
                                    }
                                }

                                if (empty)
                                    return null;

                                if (evts.isEmpty())
                                    continue;

                                lastRetRev = nextRetRev;

                                nextRetRev = -1;

                                nativeIterator.close();

                                nativeIterator = null;

                                return new WatchEvent(evts);
                            }
                            else if (!hasNext())
                                return null;
                        }
                    }
                    finally {
                        lock.unlock();
                    }
                }
            };
        }
    }
}
