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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TODO asch use read only buffers ? replace Pair from ignite-schema
 */
public class VersionedRowStore {
    /** Storage delegate. */
    private final PartitionStorage storage;
    
    /** */
    private TxManager txManager;

    /**
     * @param storage The storage.
     * @param txManager The TX manager.
     */
    public VersionedRowStore(PartitionStorage storage, TxManager txManager) {
        this.storage = storage;
        this.txManager = txManager;
    }

    /**
     * Decodes a storage row to a pair where the first is an actual value (visible to a current transaction) and the
     * second is an old value used for rollback.
     *
     * @param row The row.
     * @param ts Timestamp ts.
     * @return Actual value.
     */
    protected Pair<BinaryRow, BinaryRow> versionedRow(@Nullable DataRow row, Timestamp ts) {
        return result(unpack(row), ts);
    }

    /**
     * @param row The search row.
     * @param ts The timestamp.
     * @return The result row.
     */
    public BinaryRow get(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SearchRow key = extractAndWrapKey(row);

        DataRow readValue = storage.read(key);

        return versionedRow(readValue, ts).getFirst();
    }

    /**
     * @param keyRows Search rows.
     * @param ts The timestamp.
     * @return The result rows.
     */
    public List<BinaryRow> getAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        assert keyRows != null && !keyRows.isEmpty();

        List<BinaryRow> res = new ArrayList<>(keyRows.size());

        for (BinaryRow keyRow : keyRows)
            res.add(get(keyRow, ts));

        return res;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     */
    public void upsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        txManager.getOrCreateTransaction(ts);

        Pair<BinaryRow, BinaryRow> pair = result(unpack(storage.read(key)), ts);

        storage.write(pack(key, new Value(row, pair.getSecond(), ts)));
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return Previous row.
     */
    @Nullable public BinaryRow getAndUpsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        BinaryRow oldRow = get(row, ts);

        upsert(row, ts);

        return oldRow != null ? oldRow : null;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean delete(BinaryRow row, Timestamp ts) {
        assert row != null;

        txManager.getOrCreateTransaction(ts);

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Pair<BinaryRow, BinaryRow> pair = result(unpack(storage.read(key)), ts);

        if (pair.getFirst() == null)
            return false;

        // Write a tombstone.
        storage.write(pack(key, new Value(null, pair.getSecond(), ts)));

        return true;
    }

    /**
     * @param rows Search rows.
     * @param ts The timestamp.
     */
    public void upsertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows)
            upsert(row, ts);
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was inserted.
     */
    public boolean insert(BinaryRow row, Timestamp ts) {
        assert row != null && row.hasValue() : row;
        txManager.getOrCreateTransaction(ts);

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Pair<BinaryRow, BinaryRow> pair = result(unpack(storage.read(key)), ts);

        if (pair.getFirst() != null)
            return false;

        storage.write(pack(key, new Value(row, null, ts)));

        return true;
    }

    /**
     * @param rows Rows.
     * @param ts The timestamp.
     * @return List of not inserted rows.
     */
    public List<BinaryRow> insertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        List<BinaryRow> inserted = new ArrayList<>(rows.size());

        for (BinaryRow row : rows) {
            if (!insert(row, ts))
                inserted.add(row);
        }

        return inserted;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow row, Timestamp ts) {
        assert row != null;

        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            upsert(row, ts);

            return true;
        }
        else
            return false;
    }

    /**
     * @param oldRow Old row.
     * @param newRow New row.
     * @param ts The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow oldRow, BinaryRow newRow, Timestamp ts) {
        assert oldRow != null;
        assert newRow != null;

        BinaryRow oldRow0 = get(oldRow, ts);

        if (oldRow0 != null && equalValues(oldRow0, oldRow)) {
            upsert(newRow, ts);

            return true;
        }
        else
            return false;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return Replaced row.
     */
    public BinaryRow getAndReplace(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            upsert(row, ts);

            return oldRow;
        }
        else
            return null;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean deleteExact(BinaryRow row, Timestamp ts) {
        assert row != null;
        assert row.hasValue();

        BinaryRow oldRow = get(row, ts);

        if (oldRow != null && equalValues(oldRow, row)) {
            delete(oldRow, ts);

            return true;
        }
        else
            return false;
    }

    /**
     * @param row The row.
     * @param ts The timestamp.
     * @return Deleted row.
     */
    public BinaryRow getAndDelete(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            delete(oldRow, ts);

            return oldRow;
        }
        else
            return null;
    }

    /**
     * @param keyRows Search rows.
     * @param ts The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        var notDeleted = new ArrayList<BinaryRow>();

        for (BinaryRow keyRow : keyRows) {
            if (!delete(keyRow, ts))
                notDeleted.add(keyRow);
        }

        return notDeleted;
    }

    /**
     * @param rows Search rows.
     * @param ts The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAllExact(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        var notDeleted = new ArrayList<BinaryRow>(rows.size());

        for (BinaryRow row : rows) {
            if (!deleteExact(row, ts))
                notDeleted.add(row);
        }

        return notDeleted;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * @param value The value.
     * @param ts The timestamp.
     * @return Actual value visible to a user.
     */
    private BinaryRow resolve(@Nullable Value value, @Nullable Timestamp ts) {
        if (value == null)
            return null;

        if (ts != null) {
            if (!ts.equals(value.timestamp))
                assert value.oldRow == null : value + " " + ts;

            return value.newRow;
        }
        else {
            if (value.timestamp != null) {
                TxState state = txManager.state(value.timestamp);

                if (state == TxState.COMMITED)
                    return value.newRow;
                else
                    return value.oldRow;
            }
            else
                return value.newRow;
        }
    }


    /**
     * @throws Exception If failed.
     */
    public void close() throws Exception {
        storage.close();
    }

    /**
     * Extracts a key and a value from the {@link BinaryRow} and wraps it in a {@link DataRow}.
     *
     * @param row Binary row.
     * @return Data row.
     */
    @NotNull private static DataRow extractAndWrapKeyValue(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, row.hasValue() ? row.bytes() : null);
    }

    /**
     * Extracts a key from the {@link BinaryRow} and wraps it in a {@link SearchRow}.
     *
     * @param row Binary row.
     * @return Search row.
     */
    @NotNull private static SearchRow extractAndWrapKey(@NotNull BinaryRow row) {
        // TODO asch can reuse thread local byte buffer
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, null);
    }

    /**
     * Unpacks a raw value into (cur, old, ts) triplet.
     * TODO asch not very efficient.
     * @param row The row.
     * @return The value.
     */
    private static Value unpack(@Nullable DataRow row) {
        if (row == null)
            return new Value(null, null, null);

        ByteBuffer buf = row.value();

        BinaryRow newVal = null;
        BinaryRow oldVal = null;

        int l1 = buf.asIntBuffer().get();

        int pos = 4;

        buf.position(pos);

        if (l1 != 0) {
            // TODO asch get rid of copying
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

        long ts = buf.asLongBuffer().get();

        return new Value(newVal, oldVal, new Timestamp(ts));
    }

    /**
     * @param key The key.
     * @param value The value.
     * @return Data row.
     */
    private static DataRow pack(SearchRow key, Value value) {
        byte[] b1 = null;
        byte[] b2 = null;

        int l1 = value.newRow == null ? 0 : (b1 = value.newRow.bytes()).length;
        int l2 = value.oldRow == null ? 0 : (b2 = value.oldRow.bytes()).length;

        ByteBuffer buf = ByteBuffer.allocate(4 + l1 + 4 + l2 + 8);

        buf.asIntBuffer().put(l1);

        buf.position(4);

        if (l1 > 0)
            buf.put(b1);

        buf.asIntBuffer().put(l2);

        buf.position(buf.position() + 4);

        if (l2 > 0)
            buf.put(b2);

        buf.asLongBuffer().put(value.timestamp.get());

        return new SimpleDataRow(key.keyBytes(), buf.array());
    }

    /**
     * @see {@link #versionedRow(DataRow, Timestamp)}
     * @param val The value.
     * @param ts The transaction
     * @return New and old rows pair.
     */
    private Pair<BinaryRow, BinaryRow> result(Value val, Timestamp ts) {
        if (val.timestamp == null) { // New or after reset.
            assert val.oldRow == null : val;

            return new Pair<>(val.newRow, null);
        }

        // Checks "inTx" condition. Will be false if this is a first transactional op.
        if (val.timestamp.equals(ts))
            return new Pair<>(val.newRow, val.oldRow);

        TxState state = txManager.state(val.timestamp);

        BinaryRow cur;

        if (state == TxState.ABORTED) // Was aborted and had written a temp value.
            cur = val.oldRow;
        else
            cur = val.newRow;

        return new Pair<>(cur, cur);
    }

    /**
     * @param path The path.
     * @return Snapshot future.
     */
    public CompletionStage<Void> snapshot(Path path) {
        return storage.snapshot(path);
    }

    /**
     * @param path The path.
     */
    public void restoreSnapshot(Path path) {
        storage.restoreSnapshot(path);
    }

    /**
     * @param pred The predicate.
     * @return The cursor.
     */
    public Cursor<BinaryRow> scan(Predicate<SearchRow> pred) {
        Cursor<DataRow> delegate = storage.scan(pred);

        return new Cursor<BinaryRow>() {
            @Override public void close() throws Exception {
                delegate.close();
            }

            @NotNull @Override public Iterator<BinaryRow> iterator() {
                return this;
            }

            @Override public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override public BinaryRow next() {
                DataRow row = delegate.next();

                return versionedRow(row, null).getFirst(); // TODO asch add tx support.
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
        Timestamp timestamp;

        Value(@Nullable BinaryRow newRow, @Nullable BinaryRow oldRow, Timestamp timestamp) {
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.timestamp = timestamp;
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
         * Constructor.
         *
         * @param data Wrapped data.
         */
        public KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }

    /**
     * @return The delegate.
     */
    public PartitionStorage delegate() {
        return storage;
    }

    /**
     * @return Transaction manager.
     */
    public TxManager txManager() {
        return txManager;
    }

    /**
     * Adapter that converts a {@link BinaryRow} into a {@link SearchRow}.
     */
    private static class BinarySearchRow implements SearchRow {
        /** Search key. */
        private final byte[] keyBytes;

        /** Source row. */
        private final BinaryRow sourceRow;

        /**
         * Constructor.
         *
         * @param row Row to search for.
         */
        BinarySearchRow(BinaryRow row) {
            sourceRow = row;
            keyBytes = new byte[row.keySlice().capacity()];

            row.keySlice().get(keyBytes);
        }

        /** {@inheritDoc} */
        @Override public byte @NotNull [] keyBytes() {
            return keyBytes;
        }

        /** {@inheritDoc} */
        @Override public @NotNull ByteBuffer key() {
            return ByteBuffer.wrap(keyBytes);
        }
    }
}
