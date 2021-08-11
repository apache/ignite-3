package org.apache.ignite.internal.table.distributed.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
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

    /** */
    private LockManager lockManager;

    /**
     * @param storage The storage.
     * @param txManager The TX manager.
     * @param lockManager The lock manager.
     */
    public VersionedRowStore(Storage storage, TxManager txManager, LockManager lockManager) {
        this.storage = storage;
        this.txManager = txManager;
        this.lockManager = lockManager;
    }

    /** {@inheritDoc} */
    public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow readValue = storage.read(extractAndWrapKey(row));

        ByteBufferRow responseRow = null;

        if (readValue.hasValueBytes())
            responseRow = new ByteBufferRow(readValue.valueBytes());

        return CompletableFuture.completedFuture(responseRow);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        List<SearchRow> keys = keyRows.stream().map(VersionedRowStore::extractAndWrapKey)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage
            .readAll(keys)
            .stream()
            .filter(DataRow::hasValueBytes)
            .map(read -> new ByteBufferRow(read.valueBytes()))
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Void> upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        storage.write(extractAndWrapKeyValue(row));

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow keyValue = extractAndWrapKeyValue(row);

        var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, false);

        storage.invoke(keyValue, getAndReplace);

        DataRow oldRow = getAndReplace.oldRow();

        if (oldRow.hasValueBytes())
            return CompletableFuture.completedFuture(new ByteBufferRow(oldRow.valueBytes()));
        else
            return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        SearchRow newRow = extractAndWrapKey(row);

        var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

        storage.invoke(newRow, getAndRemoveClosure);

        return CompletableFuture.completedFuture(getAndRemoveClosure.result());
    }

    /** {@inheritDoc} */
    public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        storage.writeAll(rows.stream().map(VersionedRowStore::extractAndWrapKeyValue).collect(Collectors.toList()));

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow newRow = extractAndWrapKeyValue(row);

        InsertInvokeClosure writeIfAbsent = new InsertInvokeClosure(newRow);

        storage.invoke(newRow, writeIfAbsent);

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream().map(VersionedRowStore::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.insertAll(keyValues).stream()
            .filter(DataRow::hasValueBytes)
            .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        DataRow keyValue = extractAndWrapKeyValue(row);

        var replaceIfExists = new GetAndReplaceInvokeClosure(keyValue, true);

        storage.invoke(keyValue, replaceIfExists);

        return CompletableFuture.completedFuture(replaceIfExists.result());
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        DataRow expected = extractAndWrapKeyValue(oldRow);
        DataRow newR = extractAndWrapKeyValue(newRow);

        var replaceClosure = new ReplaceExactInvokeClosure(expected, newR);

        storage.invoke(expected, replaceClosure);

        return CompletableFuture.completedFuture(replaceClosure.result());
    }

    public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction ts) {
        return null;
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        DataRow keyValue = extractAndWrapKeyValue(row);

        var deleteExact = new DeleteExactInvokeClosure(keyValue);

        storage.invoke(keyValue, deleteExact);

        return CompletableFuture.completedFuture(deleteExact.result());
    }

    public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction ts) {
        return null;
    }

    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, InternalTransaction ts) {
        List<SearchRow> keys = rows.stream().map(VersionedRowStore::extractAndWrapKey)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAll(keys).stream()
            .filter(DataRow::hasValueBytes)
            .map(removed -> new ByteBufferRow(removed.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction ts) {
        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream().map(VersionedRowStore::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAllExact(keyValues).stream()
            .filter(DataRow::hasValueBytes)
            .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
            .filter(BinaryRow::hasValue)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
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

        return new SimpleDataRow(key, row.bytes());
    }

    /**
     * Extracts a key from the {@link BinaryRow} and wraps it in a {@link SearchRow}.
     *
     * @param row Binary row.
     * @return Search row.
     */
    @NotNull private static SearchRow extractAndWrapKey(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, null);
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

    public void commit(Timestamp timestamp) {

    }

    public void rollback(Timestamp timestamp) {

    }

    public void close() throws Exception {
        storage.close();
    }

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
