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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.basic.DelegatingDataRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Pair;
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

    private ConcurrentHashMap<ByteBuffer, RowId> primaryIndex = new ConcurrentHashMap<>();

    private ConcurrentHashMap<UUID, List<ByteBuffer>> txsKeys = new ConcurrentHashMap<>();
    private ConcurrentHashMap<UUID, List<ByteBuffer>> txsKeysForRemove = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param storage The storage.
     * @param txManager The TX manager.
     */
    public VersionedRowStore(@NotNull MvPartitionStorage storage, @NotNull TxManager txManager) {
        this.storage = Objects.requireNonNull(storage);
        this.txManager = Objects.requireNonNull(txManager);
    }

//    /**
//     * Decodes a storage row to a pair where the first is an actual value (visible to a current transaction) and the second is an old value
//     * used for rollback.
//     *
//     * @param row       The row.
//     * @param id Timestamp timestamp.
//     * @return Actual value.
//     */
//    protected Pair<BinaryRow, BinaryRow> versionedRow(@Nullable DataRow row, UUID id) {
//        return resolve(unpack(row), id);
//    }

    /**
     * Gets a row.
     *
     * @param row The search row.
     * @param id The timestamp.
     * @return The result row.
     */
    public BinaryRow get(@NotNull BinaryRow row, UUID id) {
        assert row != null;
//        System.out.println("get1");
        RowId rowId = primaryIndex.get(row.keySlice());

        if (rowId == null) {
            return null;
        }

//        System.out.println("get2");

        BinaryRow result = storage.read(rowId, id);

//        System.out.println("get3");

        return result;

//        assert row != null;
//
//        System.out.println("get1");
//
//        try(Cursor<BinaryRow> rowCursor = storage.scan(row0 -> row0.keySlice().equals(row.keySlice()), id)) {
//            if (rowCursor.hasNext()) {
//                return rowCursor.next();
//            }
//            else {
//                return null;
//            }
//        } catch (Exception e) {
//            throw new IgniteException(e);
//        }
    }

    /**
     * Gets multiple rows.
     *
     * @param keyRows Search rows.
     * @param id The timestamp.
     * @return The result rows.
     */
    public List<BinaryRow> getAll(Collection<BinaryRow> keyRows, UUID id) {
        assert keyRows != null && !keyRows.isEmpty();

        List<BinaryRow> res = new ArrayList<>(keyRows.size());

        for (BinaryRow keyRow : keyRows) {
            res.add(get(keyRow, id));
        }

        return res;
    }

    /**
     * Upserts a row.
     *
     * @param row The row.
     * @param id The timestamp.
     */
    public void upsert(@NotNull BinaryRow row, UUID id) {
//        System.out.println("VRS.upsert");
        assert row != null;

        ByteBuffer key = row.keySlice();

        RowId rowId = primaryIndex.get(key);

        if (rowId == null) {
            rowId = storage.insert(row, id);

            primaryIndex.put(key, rowId);
            txsKeys.computeIfAbsent(id, entry -> new CopyOnWriteArrayList<ByteBuffer>()).add(key);
        }
        else {
//            System.out.println("upsert else");
            storage.addWrite(rowId, row,  id);
        }


//        try(Cursor<BinaryRow> rowCursor = storage.scan(row0 -> row0.keySlice().equals(row.keySlice()), id)) {
//            if (rowCursor.hasNext()) {
//                return rowCursor.next();
//            }
//            else {
//                return null;
//            }
//        } catch (Exception e) {
//            throw new IgniteException(e);
//        }
    }

    /**
     * Upserts a row and returns previous value.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return Previous row.
     */
    @Nullable
    public BinaryRow getAndUpsert(@NotNull BinaryRow row, UUID id) {
        assert row != null;

        BinaryRow oldRow = get(row, id);

        upsert(row, id);

        return oldRow != null ? oldRow : null;
    }

    /**
     * Deletes a row.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean delete(BinaryRow row, UUID id) {
        assert row != null;

        RowId rowId = primaryIndex.get(row.keySlice());

        if (rowId == null) {
            return false;
        }

//        BinaryRow result = storage.read(rowId, id);
//        primaryIndex.remove(row.keySlice());
        // Write a tombstone.

        //------------
//        BinaryRow prevRow = storage.addWrite(primaryIndex.get(row.keySlice()), null, id);
//
//        if (prevRow == null && txsKeysForRemove.getOrDefault(id, Collections.emptyList()).contains(row.keySlice()))
//            return false;
        //------------


        BinaryRow prevRow = storage.read(primaryIndex.get(row.keySlice()), id);

        if (prevRow == null)
            return false;

        storage.addWrite(primaryIndex.get(row.keySlice()), null, id);

        //------------

//        BinaryRow result = storage.read(rowId, id);
//        primaryIndex.remove(row.keySlice());
        // Write a tombstone.
        txsKeysForRemove.computeIfAbsent(id, entry -> new CopyOnWriteArrayList<ByteBuffer>()).add(row.keySlice());



        return true;
    }

    /**
     * Upserts multiple rows.
     *
     * @param rows Search rows.
     * @param id The timestamp.
     */
    public void upsertAll(Collection<BinaryRow> rows, UUID id) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows) {
            upsert(row, id);
        }
    }

    /**
     * Inserts a row.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return {@code true} if was inserted.
     */
    public boolean insert(BinaryRow row, UUID id) {
//        System.out.println("VRS.insert");
        assert row != null && row.hasValue() : row;

        ByteBuffer key = row.keySlice();

        RowId rowId = primaryIndex.get(key);

        if (rowId != null) {
            return false;
        }
        else {
            rowId = storage.insert(row, id);

            primaryIndex.put(key, rowId);

            txsKeys.computeIfAbsent(id, entry -> new CopyOnWriteArrayList<ByteBuffer>()).add(key);

            return true;
        }

//        BinaryRow result = storage.read(rowId, id);

//        if (result != null) {
//            return false;
//        }

    }

    /**
     * Inserts multiple rows.
     *
     * @param rows Rows.
     * @param id The timestamp.
     * @return List of not inserted rows.
     */
    public List<BinaryRow> insertAll(Collection<BinaryRow> rows, UUID id) {
        assert rows != null && !rows.isEmpty();

        List<BinaryRow> inserted = new ArrayList<>(rows.size());

        for (BinaryRow row : rows) {
            if (!insert(row, id)) {
                inserted.add(row);
            }
        }

        return inserted;
    }

    /**
     * Replaces an existing row.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow row, UUID id) {
        assert row != null;

        BinaryRow oldRow = get(row, id);

        if (oldRow != null) {
            upsert(row, id);

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
     * @param id The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow oldRow, BinaryRow newRow, UUID id) {
        assert oldRow != null;
        assert newRow != null;

        BinaryRow oldRow0 = get(oldRow, id);

        if (oldRow0 != null && equalValues(oldRow0, oldRow)) {
            upsert(newRow, id);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces existing row and returns a previous value.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return Replaced row.
     */
    public BinaryRow getAndReplace(BinaryRow row, UUID id) {
        BinaryRow oldRow = get(row, id);

        if (oldRow != null) {
            upsert(row, id);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes a row by exact match.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean deleteExact(BinaryRow row, UUID id) {
        assert row != null;
        assert row.hasValue();

        BinaryRow oldRow = get(row, id);

        if (oldRow != null && equalValues(oldRow, row)) {
            delete(oldRow, id);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Delets a row and returns a previous value.
     *
     * @param row The row.
     * @param id The timestamp.
     * @return Deleted row.
     */
    public BinaryRow getAndDelete(BinaryRow row, UUID id) {
        BinaryRow oldRow = get(row, id);

        if (oldRow != null) {
            delete(oldRow, id);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes multiple rows.
     *
     * @param keyRows Search rows.
     * @param id The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAll(Collection<BinaryRow> keyRows, UUID id) {
        var notDeleted = new ArrayList<BinaryRow>();

        for (BinaryRow keyRow : keyRows) {
            if (!delete(keyRow, id)) {
                notDeleted.add(keyRow);
            }
        }

        return notDeleted;
    }

    /**
     * Deletes multiple rows by exact match.
     *
     * @param rows Search rows.
     * @param id The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAllExact(Collection<BinaryRow> rows, UUID id) {
        assert rows != null && !rows.isEmpty();

        var notDeleted = new ArrayList<BinaryRow>(rows.size());

        for (BinaryRow row : rows) {
            if (!deleteExact(row, id)) {
                notDeleted.add(row);
            }
        }

        return notDeleted;
    }

    public void commitWrite(ByteBuffer key, UUID id) {
//        BinaryRow row = storage.scan(row0 -> row0.keySlice().equals(key), timestamp.toUUID()).iterator().next();

        RowId rowId = primaryIndex.get(key);

        if (rowId == null) {
            return;
        }

//        System.out.println("VRS.commitWrite " + Arrays.toString(key.array()) + " " + rowId);

//        txsKeys.remove(id);






            List<ByteBuffer> keys = txsKeysForRemove.get(id);

            if (keys != null) {
                boolean removed = keys.remove(key);

                if (removed) {
                    primaryIndex.remove(key);
                }

                if (keys.size() == 0) {
                    txsKeysForRemove.remove(id);
                }
            }





        keys = txsKeys.get(id);

            if (keys != null) {
                keys.remove(key);
//                primaryIndex.remove(key);

                if (keys.size() == 0) {
                    txsKeys.remove(id);
                }
            }






//        List<ByteBuffer> keys = txsKeys.get(id);
//
//        if (keys != null) {
//            keys.remove(key);
//
//            if (keys.size() == 0) {
//                txsKeys.remove(id);
//            }
//        }

        storage.commitWrite(rowId, new Timestamp(id));
    }

    public void abortWrite(ByteBuffer key) {
//        BinaryRow row = storage.scan(row0 -> row0.keySlice().equals(key), timestamp.toUUID()).iterator().next();

        RowId rowId = primaryIndex.get(key);

        if (rowId == null) {
            return;
        }

        AtomicReference<UUID> id = new AtomicReference<>();

        txsKeys.entrySet().forEach(entry -> {
            if (entry.getValue().contains(key)) {
                id.set(entry.getKey());
            }
//            primaryIndex.remove(entry.getValue());
//            txForRemove.add(entry.getKey());
        });

        if (id.get() != null) {
            List<ByteBuffer> keys = txsKeys.get(id.get());

            if (keys != null) {
                boolean removed = keys.remove(key);

                if (removed) {
                    primaryIndex.remove(key);
                }

                if (keys.size() == 0) {
                    txsKeys.remove(id.get());
                }
            }
        }

//        assert txForRemove !=
//
//        txsKeys.remove(key)
//
//
//
//
//        List<ByteBuffer> keys = txsKeys.get(id);
//
//        if (keys != null) {
//            keys.remove(key);
//
//            if (keys.size() == 0) {
//                txsKeys.remove(id);
//            }
//        }



//        System.out.println("VRS.abortWrite " + key + " " + rowId);

        storage.abortWrite(rowId);

//        System.out.println("VRS.abortWrite after");
    }

    /**
     * Tests row values for equality.
     *
     * @param row Row.
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
     * Unpacks a raw value into (cur, old, id) triplet. TODO asch IGNITE-15934 not very efficient.
     *
     * @param row The row.
     * @return The value.
     */
    private static Value unpack(@Nullable DataRow row) {
        if (row == null) {
            return new Value(null, null, null);
        }

        ByteBuffer buf = row.value();

        BinaryRow newVal = null;
        BinaryRow oldVal = null;

        int l1 = buf.asIntBuffer().get();

        int pos = 4;

        buf.position(pos);

        if (l1 != 0) {
            // TODO asch IGNITE-15934 get rid of copying
            byte[] tmp = new byte[l1];

            buf.get(tmp);

            newVal = new ByteBufferRow(tmp);

            pos += l1;
        }

        buf.position(pos);

        int l2 = buf.asIntBuffer().get();

        pos += 4;

        buf.position(pos);

        if (l2 != 0) {
            // TODO asch get rid of copying
            byte[] tmp = new byte[l2];

            buf.get(tmp);

            oldVal = new ByteBufferRow(tmp);

            pos += l2;
        }

        buf.position(pos);

        long ts = buf.getLong();
        long nodeId = buf.getLong();

        return new Value(newVal, oldVal, new UUID(ts, nodeId));
    }

    /**
     * Packs a multi-versioned value.
     *
     * @param key The key.
     * @param value The value.
     * @return Data row.
     */
    private static DataRow pack(SearchRow key, Value value) {
        byte[] b1 = null;
        byte[] b2 = null;

        int l1 = value.newRow == null ? 0 : (b1 = value.newRow.bytes()).length;
        int l2 = value.oldRow == null ? 0 : (b2 = value.oldRow.bytes()).length;

        // TODO asch write only values.
        ByteBuffer buf = ByteBuffer.allocate(4 + l1 + 4 + l2 + 16);

        buf.asIntBuffer().put(l1);

        buf.position(4);

        if (l1 > 0) {
            buf.put(b1);
        }

        buf.asIntBuffer().put(l2);

        buf.position(buf.position() + 4);

        if (l2 > 0) {
            buf.put(b2);
        }

        buf.putLong(value.id.getMostSignificantBits());
        buf.putLong(value.id.getLeastSignificantBits());

        return new DelegatingDataRow(key, buf.array());
    }

    /**
     * Resolves a multi-versioned value depending on a viewer's timestamp.
     *
     * @param val        The value.
     * @param id  The timestamp
     * @return New and old rows pair.
     */
    private Pair<BinaryRow, BinaryRow> resolve(Value val, UUID id) {
        if (val.id == null) { // New or after reset.
            assert val.oldRow == null : val;

            return new Pair<>(val.newRow, null);
        }

        // Checks "inTx" condition. Will be false if this is a first transactional op.
        if (val.id.equals(id)) {
            return new Pair<>(val.newRow, val.oldRow);
        }

        TxState state = txManager.state(val.id);

        BinaryRow cur;

        if (state == TxState.ABORTED) { // Was aborted and had written a temp value.
            cur = val.oldRow;
        } else {
            cur = val.newRow;
        }

        return new Pair<>(cur, cur);
    }

    /**
     * Takes a snapshot.
     *
     * @param path The path.
     * @return Snapshot future.
     */
    public CompletionStage<Void> snapshot(Path path) {
//        System.out.println("VersionedRowStore.snapshot");
//        return storage.snapshot(path);
        return null;
    }

    /**
     * Restores a snapshot.
     *
     * @param path The path.
     */
    public void restoreSnapshot(Path path) {
//        System.out.println("VersionedRowStore.restoreSnapshot");
//        storage.restoreSnapshot(path);
    }

    /**
     * Executes a scan.
     *
     * @param pred The predicate.
     * @return The cursor.
     */
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> pred) {
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
//                    System.out.println("hasNext " + cur.keySlice());

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

        /** Transaction's timestamp. */
        UUID id;

        /**
         * The constructor.
         *
         * @param newRow New row.
         * @param oldRow Old row.
         * @param id The timestamp.
         */
        Value(@Nullable BinaryRow newRow, @Nullable BinaryRow oldRow, UUID id) {
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.id = id;
        }
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    public static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * The constructor.
         *
         * @param data Wrapped data.
         */
        public KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            KeyWrapper wrapper = (KeyWrapper) o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Returns a storage delegate.
     *
     * @return The delegate.
     */
    public MvPartitionStorage delegate() {
        return storage;
//        return null;
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
