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
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

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

    /** Path to the rocksdb database. */
    private final Path dbPath;

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

            this.dbPath = dbPath;

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
        IgniteUtils.closeAll(options, db);
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

                updateKeysIndex(key, revision);
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

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long cntr = updCntr + 1;

            addToBatch(batch, key, value, curRev, cntr);

            fillAndWriteBatch(batch, curRev, cntr);

            updateKeysIndex(key, curRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Adds a revision to the keys index.
     *
     * @param key Key.
     * @param curRev New revision for key.
     */
    private void updateKeysIndex(byte[] key, long curRev) {
        // Update keysIdx.
        List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

        revs.add(curRev);
    }

    /**
     * Fills the batch with system values (an update counter and a revision) and writes to the db.
     *
     * @param batch Write batch.
     * @param newRev New revision.
     * @param newCntr New update counter.
     * @throws RocksDBException If failed.
     */
    private void fillAndWriteBatch(WriteBatch batch, long newRev, long newCntr) throws RocksDBException {
        try (WriteOptions opts = new WriteOptions()) {
            batch.put(UPDATE_COUNTER_KEY, ByteUtils.longToBytes(newCntr));
            batch.put(REVISION_KEY, ByteUtils.longToBytes(newRev));

            db.write(opts, batch);

            rev = newRev;
            updCntr = newCntr;
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] value) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;
            long cntr = updCntr + 1;

            List<Long> revs = keysIdx.computeIfAbsent(key, k -> new ArrayList<>());

            long lastRev = revs.isEmpty() ? 0 : lastRevision(revs);

            addToBatch(batch, key, value, curRev, cntr);

            fillAndWriteBatch(batch, curRev, cntr);

            updateKeysIndex(key, curRev);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long counter = addAllToBatch(batch, keys, values, curRev);

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);

                updateKeysIndex(key, curRev);
            }

            fillAndWriteBatch(batch, curRev, counter);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
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

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            long counter = addAllToBatch(batch, keys, values, curRev);

            fillAndWriteBatch(batch, curRev, counter);

            for (int i = 0; i < keys.size(); i++)
                updateKeysIndex(keys.get(i), curRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
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

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;
            long counter = updCntr + 1;

            if (addToBatchForRemoval(batch, key, curRev, counter)) {
                fillAndWriteBatch(batch, curRev, counter);

                updateKeysIndex(key, curRev);
            }
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
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

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            long counter = updCntr;

            for (byte[] key : keys) {
                counter++;

                if (addToBatchForRemoval(batch, key, curRev, counter))
                    existingKeys.add(key);
                else
                    counter--;
            }

            fillAndWriteBatch(batch, curRev, counter);

            for (byte[] key : existingKeys)
                updateKeysIndex(key, curRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
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

        try (WriteBatch batch = new WriteBatch()) {
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

            long counter = addAllToBatch(batch, existingKeys, vals, curRev);

            fillAndWriteBatch(batch, curRev, counter);

            for (byte[] key : existingKeys)
                updateKeysIndex(key, curRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            Entry e = get(condition.key());

            boolean branch = condition.test(e);

            Collection<Operation> ops = branch ? success : failure;

            long curRev = rev + 1;

            boolean modified = false;

            long counter = updCntr;

            List<byte[]> updatedKeys = new ArrayList<>();

            for (Operation op : ops) {
                byte[] key = op.key();

                switch (op.type()) {
                    case PUT:
                        counter++;

                        addToBatch(batch, key, op.value(), curRev, counter);

                        updatedKeys.add(key);

                        modified = true;

                        break;

                    case REMOVE:
                        counter++;

                        boolean removed = addToBatchForRemoval(batch, key, curRev, counter);

                        if (!removed)
                            counter--;
                        else
                            updatedKeys.add(key);

                        modified |= removed;

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown operation type: " + op.type());
                }
            }

            if (modified) {
                fillAndWriteBatch(batch, curRev, counter);

                for (byte[] key : updatedKeys)
                    updateKeysIndex(key, curRev);
            }

            return branch;
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
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

    /**
     * Adds a key to a batch marking the value as a tombstone.
     *
     * @param batch Write batch.
     * @param key Target key.
     * @param curRev Revision.
     * @param counter Update counter.
     * @return {@code true} if an entry can be deleted.
     * @throws RocksDBException If failed.
     */
    private boolean addToBatchForRemoval(WriteBatch batch, byte[] key, long curRev, long counter) throws RocksDBException {
        Entry e = doGet(key, LATEST_REV, false);

        if (e.empty() || e.tombstone())
            return false;

        addToBatch(batch, key, TOMBSTONE, curRev, counter);

        return true;
    }

    /**
     * Compacts all entries by the given key, removing all previous revisions and deleting the last entry if it is
     * a tombstone.
     *
     * @param key Target key.
     * @param revs Revision.
     * @param compactedKeysIdx Keys index.
     * @throws RocksDBException If failed.
     */
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

    /**
     * Gets all entries with given keys and a revision.
     *
     * @param keys Target keys.
     * @param rev Target revision.
     * @return Collection of entries.
     */
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

    /**
     * Gets the value by key and revision.
     *
     * @param key Target key.
     * @param rev Target revision.
     * @param exactRev {@code true} if searching for exact revision, {@code false} if rev is an upper bound (inclusive).
     * @return Value.
     */
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
     * Adds a revision to a key.
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
     * Gets a key from a key with revision.
     *
     * @param rocksKey Key with a revision.
     * @return Key without a revision.
     */
    static byte[] rocksKeyToBytes(byte[] rocksKey) {
        return Arrays.copyOfRange(rocksKey, Long.BYTES, rocksKey.length);
    }

    /**
     * Builds a value from a byte array.
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
     * Adds an update counter and a tombstone flag to a value.
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

    /**
     * Gets the value by a key and a revision.
     *
     * @param key Target key.
     * @param revision Target revision.
     * @return Entry.
     */
    @NotNull
    Entry doGetValue(byte[] key, long revision) {
        if (revision == 0)
            return Entry.empty(key);

        byte[] valueBytes;

        try {
            valueBytes = db.get(keyToRocksKey(revision, key));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }

        if (valueBytes == null || valueBytes.length == 0)
            return Entry.empty(key);

        Value lastVal = bytesToValue(valueBytes);

        if (lastVal.tombstone())
            return Entry.tombstone(key, revision, lastVal.updateCounter());

        return new Entry(key, lastVal.bytes(), revision, lastVal.updateCounter());
    }

    /**
     * Adds an entry to the batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @param value Value.
     * @param curRev Revision.
     * @param cntr Update counter.
     * @throws RocksDBException If failed.
     */
    private void addToBatch(WriteBatch batch, byte[] key, byte[] value, long curRev, long cntr) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(value, cntr);

        batch.put(rocksKey, rocksValue);
    }

    /**
     * Adds all entries to the batch.
     *
     * @param batch Write batch.
     * @param keys Keys.
     * @param values Values.
     * @param curRev Revision.
     * @return New update counter value.
     * @throws RocksDBException If failed.
     */
    private long addAllToBatch(WriteBatch batch, List<byte[]> keys, List<byte[]> values, long curRev) throws RocksDBException {
        long counter = this.updCntr;

        for (int i = 0; i < keys.size(); i++) {
            counter++;

            byte[] key = keys.get(i);

            byte[] bytes = values.get(i);

            addToBatch(batch, key, bytes, curRev, counter);
        }

        return counter;
    }

    /**
     * Gets last revision from the list.
     *
     * @param revs Revisions.
     * @return Last revision.
     */
    private static long lastRevision(List<Long> revs) {
        return revs.get(revs.size() - 1);
    }

    /**
     * @return Database lock
     */
    ReadWriteLock lock() {
        return rwLock;
    }

    /**
     * @return Database.
     */
    RocksDB db() {
        return db;
    }

    /**
     * Gets a ceiling (higher or equal) entry for a key in the keys index.
     *
     * @param keyFrom Key.
     * @return Higher entry.
     */
    Map.Entry<byte[], List<Long>> revisionCeilingEntry(byte[] keyFrom) {
        return keysIdx.ceilingEntry(keyFrom);
    }

    /**
     * Gets a higher entry for a key in the keys index.
     *
     * @param key Key.
     * @return Higher entry.
     */
    Map.Entry<byte[], List<Long>> revisionHigherEntry(byte[] key) {
        return keysIdx.higherEntry(key);
    }

    /** */
    @TestOnly
    public Path getDbPath() {
        return dbPath;
    }

    /**
     * Checks the status of the iterator and throw an exception if it is not correct.
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
