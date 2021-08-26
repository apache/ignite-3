package org.apache.ignite.internal.table.distributed.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.basic.DeleteExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndRemoveInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndReplaceInvokeClosure;
import org.apache.ignite.internal.storage.basic.InsertInvokeClosure;
import org.apache.ignite.internal.storage.basic.ReplaceExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class VersionedRowStore {
    /** Storage delegate. */
    private final Storage storage;
    
    /** */
    private TxManager txManager;

    /**
     * @param storage The storage.
     * @param txManager The TX manager.
     */
    public VersionedRowStore(Storage storage, TxManager txManager) {
        this.storage = storage;
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    public BinaryRow get(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        SearchRow key = extractAndWrapKey(row);

        DataRow readValue = storage.read(key);

        Value val = extractValue(readValue);

        Pair<BinaryRow, BinaryRow> result = result(val, tx);

        return result.getFirst();
    }

    /** {@inheritDoc} */
    public Collection<BinaryRow> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        List<SearchRow> keys = keyRows.stream().map(VersionedRowStore::extractAndWrapKey)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage
            .readAll(keys)
            .stream()
            .filter(DataRow::hasValueBytes)
            .map(read -> new ByteBufferRow(read.valueBytes()))
            .collect(Collectors.toList());

        return res;
    }

    /** {@inheritDoc} */
    public void upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        TxState state = txManager.state(tx.timestamp());

        assert state == TxState.PENDING;

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Value value = extractValue(storage.read(key));

        Pair<BinaryRow, BinaryRow> pair = result(value, tx);

        // Read in tx TODO asch get rid
        BinaryRow oldRow = value.timestamp != null && value.timestamp.equals(tx.timestamp()) ? pair.getSecond() :
            pair.getFirst();

        storage.write(packValue(key, new Value(row, oldRow, tx.timestamp())));
    }

    /** {@inheritDoc} */
    public BinaryRow getAndUpsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow keyValue = extractAndWrapKeyValue(row);

        var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, false);

        storage.invoke(keyValue, getAndReplace);

        DataRow oldRow = getAndReplace.oldRow();

        if (oldRow.hasValueBytes())
            return new ByteBufferRow(oldRow.valueBytes());
        else
            return null;
    }

    /** {@inheritDoc} */
    public boolean delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        SearchRow newRow = extractAndWrapKey(row);

        var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

        storage.invoke(newRow, getAndRemoveClosure);

        return getAndRemoveClosure.result();
    }

    /** {@inheritDoc} */
    public void upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        storage.writeAll(rows.stream().map(VersionedRowStore::extractAndWrapKeyValue).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    public Boolean insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow newRow = extractAndWrapKeyValue(row);

        InsertInvokeClosure writeIfAbsent = new InsertInvokeClosure(newRow);

        storage.invoke(newRow, writeIfAbsent);

        return writeIfAbsent.result();
    }

    /** {@inheritDoc} */
    public Collection<BinaryRow> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream().map(VersionedRowStore::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.insertAll(keyValues).stream()
            .filter(DataRow::hasValueBytes)
            .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return res;
    }

    /** {@inheritDoc} */
    public boolean replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow keyValue = extractAndWrapKeyValue(row);

        var replaceIfExists = new GetAndReplaceInvokeClosure(keyValue, true);

        storage.invoke(keyValue, replaceIfExists);

        return replaceIfExists.result();
    }

    public boolean replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        DataRow expected = extractAndWrapKeyValue(oldRow);
        DataRow newR = extractAndWrapKeyValue(newRow);

        var replaceClosure = new ReplaceExactInvokeClosure(expected, newR);

        storage.invoke(expected, replaceClosure);

        return replaceClosure.result();
    }

    public BinaryRow getAndReplace(BinaryRow row, InternalTransaction ts) {
        DataRow keyValue = extractAndWrapKeyValue(row);

        var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, true);

        storage.invoke(keyValue, getAndReplace);

        DataRow oldRow = getAndReplace.oldRow();

        return oldRow.hasValueBytes() ? new ByteBufferRow(oldRow.valueBytes()) : null;
    }

    /** {@inheritDoc} */
    public boolean deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        DataRow keyValue = extractAndWrapKeyValue(row);

        var deleteExact = new DeleteExactInvokeClosure(keyValue);

        storage.invoke(keyValue, deleteExact);

        return deleteExact.result();
    }

    public BinaryRow getAndDelete(BinaryRow row, InternalTransaction ts) {
        SearchRow keyRow = extractAndWrapKey(row);

        var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

        storage.invoke(keyRow, getAndRemoveClosure);

        return getAndRemoveClosure.result() ? new ByteBufferRow(getAndRemoveClosure.oldRow().valueBytes()) : null;
    }

    public Collection<BinaryRow> deleteAll(Collection<BinaryRow> rows, InternalTransaction ts) {
        List<SearchRow> keys = rows.stream().map(VersionedRowStore::extractAndWrapKey)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAll(keys).stream()
            .filter(DataRow::hasValueBytes)
            .map(removed -> new ByteBufferRow(removed.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return res;
    }

    public Collection<BinaryRow> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction ts) {
        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream().map(VersionedRowStore::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAllExact(keyValues).stream()
            .filter(DataRow::hasValueBytes)
            .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return res;
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
     * @param value
     * @param tx
     * @return
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
    private static Value extractValue(@NotNull DataRow row) {
        if (!row.hasValueBytes())
            return new Value(null, null, null);

        ByteBuffer buf = row.value();

        BinaryRow newVal = null;
        BinaryRow oldVal = null;

        int l1 = buf.asIntBuffer().get();

        int pos = 4;

        buf.position(pos);

        if (l1 != 0) {
            ByteBuffer tmp = buf.duplicate().limit(pos + l1).slice().order(ByteOrder.LITTLE_ENDIAN);

            newVal = new ByteBufferRow(tmp);

            pos += l1;
        }

        buf.position(pos);

        int l2 = buf.asIntBuffer().get();

        pos += 4;

        buf.position(pos);

        if (l2 != 0) {
            ByteBuffer tmp = buf.duplicate().limit(pos + l2).slice().order(ByteOrder.LITTLE_ENDIAN);

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
     * @param tx The transaction
     * @return New and old rows pair.
     */
    private Pair<BinaryRow, BinaryRow> result(Value val, InternalTransaction tx) {
        if (val.timestamp == null) { // New or after reset.
            assert val.oldRow == null : val;

            return new Pair<>(val.newRow, null);
        }

        TxState state = txManager.state(val.timestamp);

        if (state == TxState.COMMITED)
            return new Pair<>(val.newRow, null);
        else if (state == TxState.ABORTED)
            return new Pair<>(val.oldRow, null);
        else
            return tx.timestamp().equals(val.timestamp) ? new Pair<>(val.newRow, val.oldRow) : new Pair<>(val.oldRow, null);
    }

    /**
     * Versioned value.
     */
    private static class Value {
        BinaryRow newRow;

        BinaryRow oldRow;

        Timestamp timestamp;

        Value(BinaryRow newRow, BinaryRow oldRow, Timestamp timestamp) {
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.timestamp = timestamp;
        }
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
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
}
