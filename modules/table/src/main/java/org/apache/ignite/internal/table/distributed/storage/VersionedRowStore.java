package org.apache.ignite.internal.table.distributed.storage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.basic.GetAndReplaceInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
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

    /** {@inheritDoc} */
    public BinaryRow get(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SearchRow key = extractAndWrapKey(row);

        DataRow readValue = storage.read(key);

        Value val = extractValue(readValue);

        Pair<BinaryRow, BinaryRow> result = result(val, ts);

        return result.getFirst();
    }

    /** {@inheritDoc} */
    public List<BinaryRow> getAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        assert keyRows != null && !keyRows.isEmpty();

        List<BinaryRow> res = new ArrayList<>(keyRows.size());

        for (BinaryRow keyRow : keyRows)
            res.add(get(keyRow, ts));

        return res;
    }

    /** {@inheritDoc} */
    public void upsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        txManager.getOrCreateTransaction(ts);

        Value value = extractValue(storage.read(key));

        Pair<BinaryRow, BinaryRow> pair = result(value, ts);

        storage.write(packValue(key, new Value(row, pair.getSecond(), ts)));
    }

    /** {@inheritDoc} */
    @Nullable public BinaryRow getAndUpsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        BinaryRow oldRow = get(row, ts);

        upsert(row, ts);

        return oldRow != null ? oldRow : null;
    }

    /** {@inheritDoc} */
    public boolean delete(BinaryRow row, Timestamp ts) {
        assert row != null;

        txManager.getOrCreateTransaction(ts);

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Value value = extractValue(storage.read(key));

        Pair<BinaryRow, BinaryRow> pair = result(value, ts);

        if (pair.getFirst() == null)
            return false;

        // Write a tombstone.
        storage.write(packValue(key, new Value(null, pair.getSecond(), ts)));

        return true;
    }

    /** {@inheritDoc} */
    public void upsertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows)
            upsert(row, ts);
    }

    /** {@inheritDoc} */
    public boolean insert(BinaryRow row, Timestamp ts) {
        assert row != null && row.hasValue() : row;
        txManager.getOrCreateTransaction(ts);

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Value value = extractValue(storage.read(key));

        Pair<BinaryRow, BinaryRow> pair = result(value, ts);

        if (pair.getFirst() != null)
            return false;

        storage.write(packValue(key, new Value(row, null, ts)));

        return true;
    }

    /** {@inheritDoc} */
    public List<BinaryRow> insertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        List<BinaryRow> inserted = new ArrayList<>(rows.size());

        for (BinaryRow row : rows) {
            if (insert(row, ts))
                inserted.add(row);
        }

        return inserted;
    }

    /** {@inheritDoc} */
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

    public boolean replace(BinaryRow oldRow, BinaryRow newRow, Timestamp ts) {
        assert oldRow != null;
        assert newRow != null;

        BinaryRow oldRow0 = get(oldRow, ts);

        if (oldRow0 != null && equalValues(oldRow0, newRow)) {
            upsert(newRow, ts);

            return true;
        }
        else
            return false;
    }

    public BinaryRow getAndReplace(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            upsert(row, ts);

            return oldRow;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
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

    public BinaryRow getAndDelete(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            delete(oldRow, ts);

            return oldRow;
        }
        else
            return null;
    }

    public List<BinaryRow> deleteAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        var notDeleted = new ArrayList<BinaryRow>();

        for (BinaryRow keyRow : keyRows) {
            if (!delete(keyRow, ts))
                notDeleted.add(keyRow);
        }

        return notDeleted;
    }

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
    @NotNull private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
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

    /** {@inheritDoc} */
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
     * TODO asch not very efficient.
     * @param row The row.
     * @return The value.
     */
    private static Value extractValue(@Nullable DataRow row) {
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
            //ByteBuffer tmp = buf.duplicate().limit(pos + l1).slice().order(ByteOrder.LITTLE_ENDIAN);

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
            //ByteBuffer tmp = buf.duplicate().limit(pos + l2).slice().limit(pos + l2).order(ByteOrder.LITTLE_ENDIAN);

            oldVal = new ByteBufferRow(tmp);

            pos += l2;
        }

        buf.position(pos);

        long ts = buf.asLongBuffer().get();

        return new Value(newVal, oldVal, new Timestamp(ts));
    }

    private static DataRow packValue(SearchRow key, Value value) {
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

        if (state == TxState.ABORTED) // Was aborted and have written temp value.
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
