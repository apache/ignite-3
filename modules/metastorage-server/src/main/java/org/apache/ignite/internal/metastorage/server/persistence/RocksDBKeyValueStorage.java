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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.Operation;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
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
import org.rocksdb.SstFileWriter;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;

/**
 * Key-value storage based on RocksDB.
 * Keys are stored with revision.
 * Values are stored with update counter and a boolean flag which represents whether this record is a tombstone.
 * <br>
 * Key: [8 bytes revision, N bytes key itself].
 * <br>
 * Value: [8 bytes update counter, 1 byte tombstone flag, N bytes value].
 */
public class RocksDBKeyValueStorage implements KeyValueStorage {
    /** Database snapshot file name. */
    private static final String SNAPSHOT_FILE_NAME = "db.snapshot";

    /** Suffix for the temporary snapshot folder */
    private static final String TMP_SUFFIX = ".tmp";

    // A revision to store with a system entries.
    private static final long SYSTEM_REVISION_MARKER_VALUE = -1;

    /** Revision key. */
    private static final byte[] REVISION_KEY = keyToRocksKey(
        SYSTEM_REVISION_MARKER_VALUE,
        "SYSTEM_REVISION_KEY".getBytes(StandardCharsets.UTF_8)
    );

    /** Update counter key. */
    private static final byte[] UPDATE_COUNTER_KEY = keyToRocksKey(
        SYSTEM_REVISION_MARKER_VALUE,
        "SYSTEM_UPDATE_COUNTER_KEY".getBytes(StandardCharsets.UTF_8)
    );

    static {
        RocksDB.loadLibrary();
    }

    /** RockDB options. */
    private final Options options;

    /** RocksDb instance. */
    private final RocksDB db;

    /** RW lock. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Thread-pool for snapshot operations execution. */
    private final Executor snapshotExecutor = Executors.newSingleThreadExecutor();
    
    /**
     * Special value for the revision number which means that operation should be applied
     * to the latest revision of an entry.
     */
    private static final long LATEST_REV = -1;

    /** Lexicographic order comparator. */
    static final Comparator<byte[]> CMP = Arrays::compare;

    /** Keys index. Value is the list of all revisions under which the corresponding entry has ever been modified. */
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
            options = new Options()
                .setCreateIfMissing(true)
                // The prefix is the revision of an entry, so prefix length is the size of a long
                .useFixedLengthPrefixExtractor(Long.BYTES);

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        }
        catch (Exception e) {
            try {
                close();
            }
            catch (Exception exception) {
                e.addSuppressed(exception);
            }

            throw new IgniteInternalException("Failed to start the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        Exception error = null;

        try {
            if (options != null)
                options.close();
        }
        catch (Exception e) {
            error = e;
        }

        try {
            if (db != null)
                db.close();
        }
        catch (Exception e) {
            if (error != null)
                error.addSuppressed(e);
            else
                error = e;
        }

        if (error != null)
            throw error;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
        Path tempPath = Paths.get(snapshotPath.toString() + TMP_SUFFIX);

        IgniteUtils.deleteIfExists(tempPath);

        try {
            Files.createDirectories(tempPath);
        }
        catch (IOException e) {
            return CompletableFuture.failedFuture(
                new IgniteInternalException("Failed to create directory: " + tempPath, e)
            );
        }

        return createSstFile(tempPath).thenAccept(aVoid -> {
            IgniteUtils.deleteIfExists(snapshotPath);

            try {
                Files.move(tempPath, snapshotPath);
            }
            catch (IOException e) {
                throw new IgniteInternalException("Failed to rename: " + tempPath + " to " + snapshotPath, e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path path) {
        Path snapshotPath = path.resolve(SNAPSHOT_FILE_NAME);

        if (!Files.exists(snapshotPath))
            throw new IgniteInternalException("Snapshot not found: " + snapshotPath);

        rwLock.writeLock().lock();

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
            rwLock.writeLock().unlock();
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

                if (revision == SYSTEM_REVISION_MARKER_VALUE)
                    // It's a system entry like REVISION_KEY, ignore it whily building key index.
                    continue;

                List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());
                revs.add(revision);
            }

            checkIterator(iterator);
        }
    }

    /**
     * Creates a SST file from {@link #db}.
     *
     * @param path Path to store SST file at.
     * @return Future that represents a state of the operation.
     */
    private CompletableFuture<Void> createSstFile(Path path) {
        return CompletableFuture.supplyAsync(() -> {
            rwLock.readLock().lock();

            try (
                ReadOptions readOptions = new ReadOptions();
                EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                RocksIterator it = this.db.newIterator(readOptions);
                SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)
            ) {
                Path sstFile = path.resolve(SNAPSHOT_FILE_NAME);

                sstFileWriter.open(sstFile.toString());

                for (it.seekToFirst(); it.isValid(); it.next())
                    sstFileWriter.put(it.key(), it.value());

                checkIterator(it);

                sstFileWriter.finish();

                return null;
            }
            catch (Throwable t) {
                throw new IgniteInternalException("Failed to write snapshot: " + t.getMessage(), t);
            }
            finally {
                rwLock.readLock().unlock();
            }
        }, snapshotExecutor);
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
    private void putValue(byte[] key, byte[] value) {
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
        putValue(REVISION_KEY, ByteUtils.longToBytes(newRevision));

        rev = newRevision;
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            doPut(key, value, curRev);

            updateRevision(curRev);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] bytes) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            long lastRev = doPut(key, bytes, curRev);

            updateRevision(curRev);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            doPutAll(curRev, keys, values);

            updateRevision(curRev);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        Collection<Entry> res;

        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            doPutAll(curRev, keys, values);

            updateRevision(curRev);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key) {
        rwLock.readLock().lock();

        try {
            return doGet(key, LATEST_REV, false);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key, long rev) {
        rwLock.readLock().lock();

        try {
            return doGet(key, rev, true);
        }
        finally {
            rwLock.readLock().unlock();
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
        rwLock.writeLock().lock();

        try {
            long curRev = rev + 1;

            if (doRemove(key, curRev))
                updateRevision(curRev);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndRemove(byte[] key) {
        rwLock.writeLock().lock();

        try {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return e;

            return getAndPut(key, TOMBSTONE);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<byte[]> keys) {
        rwLock.writeLock().lock();

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
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        rwLock.writeLock().lock();

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
            rwLock.writeLock().unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        rwLock.writeLock().lock();

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
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        return new RangeCursor(this, keyFrom, keyTo, rev);
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return new RangeCursor(this, keyFrom, keyTo, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte @Nullable [] keyTo, long rev) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(this, rev, k ->
            CMP.compare(keyFrom, k) <= 0 && (keyTo == null || CMP.compare(k, keyTo) < 0)
        );
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(this, rev, k -> CMP.compare(k, key) == 0);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        return new WatchCursor(this, rev, keySet::contains);
    }

    /** {@inheritDoc} */
    @Override public void compact() {
        rwLock.writeLock().lock();

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
            rwLock.writeLock().unlock();
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

        for (int i = 0; i < revs.size() - 1; i++)
            this.db.delete(keyToRocksKey(revs.get(i), key));

        byte[] rocksKey = keyToRocksKey(lastRev, key);

        Value value = bytesToValue(db.get(rocksKey));

        if (!value.tombstone()) {
            List<Long> revisions = new ArrayList<>();

            revisions.add(lastRev);

            compactedKeysIdx.put(key, revisions);
        }
        else
            this.db.delete(rocksKey);
    }

    /** */
    @NotNull
    private Collection<Entry> doGetAll(Collection<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev > 0 || rev == LATEST_REV : "Revision must be positive or " + LATEST_REV + '.';

        Collection<Entry> res = new ArrayList<>(keys.size());

        rwLock.readLock().lock();

        try {
            for (byte[] key : keys)
                res.add(doGet(key, rev, false));
        }
        finally {
            rwLock.readLock().unlock();
        }

        return res;
    }

    /** */
    @NotNull
    Entry doGet(byte[] key, long rev, boolean exactRev) {
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
     * @return Maximum revision or {@code -1} if there is no such revision.
     */
    static long maxRevision(List<Long> revs, long upperBoundRev) {
        for (int i = revs.size() - 1; i >= 0; i--) {
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
    static byte[] rocksKeyToBytes(byte[] rocksKey) {
        return Arrays.copyOfRange(rocksKey, Long.BYTES, rocksKey.length);
    }

    /**
     * Build a value from a byte array.
     *
     * @param valueBytes Value byte array.
     * @return Value.
     */
    static Value bytesToValue(byte[] valueBytes) {
        ByteBuffer buf = ByteBuffer.wrap(valueBytes);

        // Read an update counter (8-byte long) from the entry.
        long updateCounter = buf.getLong();

        // Read a has-value flag (1 byte) from the entry.
        boolean hasValue = buf.get() != 0;

        byte[] val;
        if (hasValue) {
            // Read the value.
            val = new byte[buf.remaining()];
            buf.get(val);
        }
        else
            // There is no value, mark it as a tombstone.
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
    Entry doGetValue(byte[] key, long lastRev) {
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

        putValue(UPDATE_COUNTER_KEY, ByteUtils.longToBytes(curUpdCntr));

        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

        revs.add(curRev);

        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(bytes, curUpdCntr);

        putValue(rocksKey, rocksValue);

        return lastRev;
    }

    /** */
    private void doPutAll(long curRev, List<byte[]> keys, List<byte[]> bytesList) {
        rwLock.writeLock().lock();

        try {
            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                byte[] bytes = bytesList.get(i);

                doPut(key, bytes, curRev);
            }
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** */
    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    ReadWriteLock lock() {
        return rwLock;
    }

    RocksDB db() {
        return db;
    }

    Map.Entry<byte[], List<Long>> revisionCeilingEntry(byte[] keyFrom) {
        return keysIdx.ceilingEntry(keyFrom);
    }

    Map.Entry<byte[], List<Long>> revisionHigherEntry(byte[] key) {
        return keysIdx.higherEntry(key);
    }

    /**
     * Check the status of the iterator and throw an exception if it is not correct.
     *
     * @param it RocksDB iterator.
     * @throws IgniteInternalException if the iterator has an incorrect status.
     */
    static void checkIterator(RocksIterator it) {
        try {
            it.status();
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }
}
