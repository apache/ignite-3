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

package org.apache.ignite.internal.table.distributed.storage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TODO asch IGNITE-15934 use read only buffers ? replace Pair from ignite-schema
 * TODO asch IGNITE-15935 can use some sort of a cache on tx coordinator to avoid network IO.
 * TODO asch IGNITE-15934 invokes on storage not used for now, can it be changed ?
 */
public class VersionedRowStore {
    /** Storage delegate. */
    private final MvPartitionStorage storage;

    /** Transaction manager. */
    private TxManager txManager;

    //TODO: https://issues.apache.org/jira/browse/IGNITE-17205 Temporary solution until the implementation of the primary index is done.
    /** Dummy primary index. */
    private ConcurrentHashMap<ByteBuffer, RowId> primaryIndex = new ConcurrentHashMap<>();

    /** Keys that were inserted by the transaction. */
    private ConcurrentHashMap<UUID, List<ByteBuffer>> txsInsertedKeys = new ConcurrentHashMap<>();

    /** Keys that were removed by the transaction. */
    private ConcurrentHashMap<UUID, List<ByteBuffer>> txsRemovedKeys = new ConcurrentHashMap<>();

    // TODO: tmp
    /** Pending keys. */
    public ConcurrentHashMap<UUID, List<Object>> pendingKeys = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param storage The storage.
     * @param txManager The TX manager.
     */
    public VersionedRowStore(@NotNull MvPartitionStorage storage, @NotNull TxManager txManager) {
        this.storage = Objects.requireNonNull(storage);
        this.txManager = Objects.requireNonNull(txManager);

        Set<RowId> ids = new HashSet<>();

        storage.forEach((rowId, binaryRow) -> {
            if (ids.add(rowId)) {
                primaryIndex.put(binaryRow.keySlice(), rowId);
            }
        });
    }

    /**
     * Sets the last applied index value.
     */
    public void lastAppliedIndex(long appliedIndex) {
        storage.lastAppliedIndex(appliedIndex);
    }

    /**
     * Index of the highest write command applied to the storage. {@code 0} if index is unknown.
     */
    public long lastAppliedIndex() {
        return storage.lastAppliedIndex();
    }

    /**
     * Gets a row.
     *
     * @param row The search row.
     * @param txId Transaction id.
     * @return The result row.
     */
    public BinaryRow get(@NotNull BinaryRow row, UUID txId) {
        assert row != null;

        RowId rowId = primaryIndex.get(row.keySlice());

        if (rowId == null) {
            return null;
        }

        BinaryRow result = storage.read(rowId, txId);

        return result;
    }

    /**
     * Gets multiple rows.
     *
     * @param keyRows Search rows.
     * @param txId Transaction id.
     * @return The result rows.
     */
    public List<BinaryRow> getAll(Collection<BinaryRow> keyRows, UUID txId) {
        assert keyRows != null && !keyRows.isEmpty();

        List<BinaryRow> res = new ArrayList<>(keyRows.size());

        for (BinaryRow keyRow : keyRows) {
            res.add(get(keyRow, txId));
        }

        return res;
    }

    /**
     * Upserts a row.
     *
     * @param row The row.
     * @param txId Transaction id.
     */
    public void upsert(@NotNull BinaryRow row, UUID txId) {
        assert row != null;

        ByteBuffer key = row.keySlice();

        // TODO: tmp IGNITE-17258
        // TODO: tmp IGNITE-17258
        List<Object> txKeys = pendingKeys.computeIfAbsent(txId, k -> new ArrayList<>());
        synchronized (pendingKeys) {
            txKeys.add(key);
        }

        RowId rowId = primaryIndex.get(key);

        if (rowId == null) {
            rowId = storage.insert(row, txId);

            primaryIndex.put(key, rowId);

            txsInsertedKeys.computeIfAbsent(txId, entry -> new CopyOnWriteArrayList<ByteBuffer>()).add(key);
        } else {
            storage.addWrite(rowId, row,  txId);
        }
    }

    /**
     * Upserts a row and returns previous value.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return Previous row.
     */
    @Nullable
    public BinaryRow getAndUpsert(@NotNull BinaryRow row, UUID txId) {
        assert row != null;

        BinaryRow oldRow = get(row, txId);

        upsert(row, txId);

        return oldRow != null ? oldRow : null;
    }

    /**
     * Deletes a row.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return {@code True} if was deleted.
     */
    public boolean delete(BinaryRow row, UUID txId) {
        assert row != null;

        RowId rowId = primaryIndex.get(row.keySlice());

        if (rowId == null) {
            return false;
        }

        BinaryRow prevRow = storage.read(rowId, txId);

        if (prevRow == null) {
            return false;
        }

        // TODO: tmp IGNITE-17258
        List<Object> txKeys = pendingKeys.computeIfAbsent(txId, k -> new ArrayList<>());
        synchronized (pendingKeys) {
            txKeys.add(row.keySlice());
        }

        storage.addWrite(primaryIndex.get(row.keySlice()), null, txId);

        txsRemovedKeys.computeIfAbsent(txId, entry -> new CopyOnWriteArrayList<>()).add(row.keySlice());

        return true;
    }

    /**
     * Upserts multiple rows.
     *
     * @param rows Search rows.
     * @param txId Transaction id.
     */
    public void upsertAll(Collection<BinaryRow> rows, UUID txId) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows) {
            upsert(row, txId);
        }
    }

    /**
     * Inserts a row.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return {@code true} if was inserted.
     */
    public boolean insert(BinaryRow row, UUID txId) {
        assert row != null && row.hasValue() : row;

        ByteBuffer key = row.keySlice();

        // TODO: tmp IGNITE-17258
        List<Object> txKeys = pendingKeys.computeIfAbsent(txId, k -> new ArrayList<>());
        synchronized (pendingKeys) {
            txKeys.add(key);
        }

        RowId rowId = primaryIndex.get(key);

        if (rowId != null) {
            return false;
        } else {
            rowId = storage.insert(row, txId);

            primaryIndex.put(key, rowId);

            txsInsertedKeys.computeIfAbsent(txId, entry -> new CopyOnWriteArrayList<ByteBuffer>()).add(key);

            return true;
        }

    }

    /**
     * Inserts multiple rows.
     *
     * @param rows Rows.
     * @param txId Transaction id.
     * @return List of not inserted rows.
     */
    public List<BinaryRow> insertAll(Collection<BinaryRow> rows, UUID txId) {
        assert rows != null && !rows.isEmpty();

        List<BinaryRow> inserted = new ArrayList<>(rows.size());

        for (BinaryRow row : rows) {
            if (!insert(row, txId)) {
                inserted.add(row);
            }
        }

        return inserted;
    }

    /**
     * Replaces an existing row.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow row, UUID txId) {
        assert row != null;

        BinaryRow oldRow = get(row, txId);

        if (oldRow != null) {
            upsert(row, txId);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces a row by exact match.
     *
     * @param oldRow Old row.
     * @param newRow New row.
     * @param txId Transaction id.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow oldRow, BinaryRow newRow, UUID txId) {
        assert oldRow != null;
        assert newRow != null;

        BinaryRow oldRow0 = get(oldRow, txId);

        if (oldRow0 != null && equalValues(oldRow0, oldRow)) {
            upsert(newRow, txId);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces existing row and returns a previous value.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return Replaced row.
     */
    public BinaryRow getAndReplace(BinaryRow row, UUID txId) {
        BinaryRow oldRow = get(row, txId);

        if (oldRow != null) {
            upsert(row, txId);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes a row by exact match.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return {@code True} if was deleted.
     */
    public boolean deleteExact(BinaryRow row, UUID txId) {
        assert row != null;
        assert row.hasValue();

        BinaryRow oldRow = get(row, txId);

        if (oldRow != null && equalValues(oldRow, row)) {
            delete(oldRow, txId);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Delets a row and returns a previous value.
     *
     * @param row The row.
     * @param txId Transaction id.
     * @return Deleted row.
     */
    public BinaryRow getAndDelete(BinaryRow row, UUID txId) {
        BinaryRow oldRow = get(row, txId);

        if (oldRow != null) {
            delete(oldRow, txId);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes multiple rows.
     *
     * @param keyRows Search rows.
     * @param txId Transaction id.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAll(Collection<BinaryRow> keyRows, UUID txId) {
        var notDeleted = new ArrayList<BinaryRow>();

        for (BinaryRow keyRow : keyRows) {
            if (!delete(keyRow, txId)) {
                notDeleted.add(keyRow);
            }
        }

        return notDeleted;
    }

    /**
     * Deletes multiple rows by exact match.
     *
     * @param rows Search rows.
     * @param txId Transaction id.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAllExact(Collection<BinaryRow> rows, UUID txId) {
        assert rows != null && !rows.isEmpty();

        var notDeleted = new ArrayList<BinaryRow>(rows.size());

        for (BinaryRow row : rows) {
            if (!deleteExact(row, txId)) {
                notDeleted.add(row);
            }
        }

        return notDeleted;
    }

    /**
     * Commits a pending update of the ongoing transaction.
     *
     * @param key Row key.
     * @param txId Transaction id.
     */
    public void commitWrite(ByteBuffer key, UUID txId) {
        try {
            RowId rowId = primaryIndex.get(key);

            if (rowId == null) {
                return;
            }

            List<ByteBuffer> keys = txsRemovedKeys.get(txId);

            if (keys != null) {
                boolean removed = keys.remove(key);

                if (removed) {
                    primaryIndex.remove(key);
                }

                if (keys.size() == 0) {
                    txsRemovedKeys.remove(txId);
                }
            }

            keys = txsInsertedKeys.get(txId);

            if (keys != null) {
                keys.remove(key);

                if (keys.size() == 0) {
                    txsInsertedKeys.remove(txId);
                }
            }

            storage.commitWrite(rowId, new Timestamp(txId));
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    /**
     * Aborts a pending update of the ongoing uncommitted transaction.
     *
     * @param key Row key.
     */
    public void abortWrite(ByteBuffer key) {
        RowId rowId = primaryIndex.get(key);

        if (rowId == null) {
            return;
        }

        AtomicReference<UUID> txId = new AtomicReference<>();

        txsInsertedKeys.entrySet().forEach(entry -> {
            if (entry.getValue().contains(key)) {
                txId.set(entry.getKey());
            }
        });

        if (txId.get() != null) {
            List<ByteBuffer> keys = txsInsertedKeys.get(txId.get());

            if (keys != null) {
                boolean removed = keys.remove(key);

                if (removed) {
                    primaryIndex.remove(key);
                }

                if (keys.size() == 0) {
                    txsInsertedKeys.remove(txId.get());
                }
            }

            txsRemovedKeys.remove(txId.get());
        }

        storage.abortWrite(rowId);
    }

    /**
     * Tests row values for equality.
     *
     * @param row Row.
     * @param row2 Row.
     * @return Extracted key.
     */
    private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue()) {
            return false;
        }

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * Closes a storage.
     *
     * @throws Exception If failed.
     */
    public void close() throws Exception {
        storage.close();
    }

    /**
     * Takes a snapshot.
     *
     * @param path The path.
     * @return Snapshot future.
     */
    public CompletionStage<Void> snapshot(Path path) {
        return storage.flush();
    }

    /**
     * Restores a snapshot.
     *
     * @param path The path.
     */
    public void restoreSnapshot(Path path) {
    }

    /**
     * Executes a scan.
     *
     * @param pred The predicate.
     * @return The cursor.
     */
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> pred) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-17309 Transactional support for partition scans
        Cursor<BinaryRow> delegate = storage.scan(pred, Timestamp.nextVersion());

        // TODO asch add tx support IGNITE-15087.
        return new Cursor<BinaryRow>() {
            private @Nullable BinaryRow cur = null;

            @Override
            public void close() throws Exception {
                delegate.close();
            }

            @Override
            public boolean hasNext() {
                if (cur != null) {
                    return true;
                }

                if (delegate.hasNext()) {
                    cur = delegate.next();

                    return cur != null ? true : hasNext(); // Skip tombstones.
                }

                return false;
            }

            @Override
            public BinaryRow next() {
                BinaryRow next = cur;

                cur = null;

                assert next != null;

                return next;
            }
        };

    }

    /**
     * Versioned value.
     */
    private static class Value {
        /** Current value. */
        BinaryRow newRow;

        /** The value for rollback. */
        @Nullable BinaryRow oldRow;

        /** Transaction id. */
        UUID txId;

        /**
         * The constructor.
         *
         * @param newRow New row.
         * @param oldRow Old row.
         * @param txId The transaction id.
         */
        Value(@Nullable BinaryRow newRow, @Nullable BinaryRow oldRow, UUID txId) {
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.txId = txId;
        }
    }

    /**
     * Returns a storage delegate.
     *
     * @return The delegate.
     */
    public MvPartitionStorage delegate() {
        return storage;
    }

    /**
     * Returns a transaction manager.
     *
     * @return Transaction manager.
     */
    public TxManager txManager() {
        return txManager;
    }
}
