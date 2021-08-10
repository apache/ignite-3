package org.apache.ignite.internal.table.distributed.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
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
    /** In-memory dummy store. TODO refactor to interface */
    private final Map<KeyWrapper, Value> store = new HashMap<>();
    
    /** */
    private TxManager txManager;

    /** */
    private LockManager lockManager;

    public VersionedRowStore(TxManager txManager, LockManager lockManager) {
        this.txManager = txManager;
        this.lockManager = lockManager;
    }

    /** {@inheritDoc} */
    public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return CompletableFuture.completedFuture(resolve(store.get(extractAndWrapKey(row)), tx.timestamp()));
    }

    /** {@inheritDoc} */
    public CompletableFuture<Void> upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        store.compute(extractAndWrapKey(row), (key, value) ->
            value == null ? new Value(row, null, tx.timestamp()) :
                new Value(row, value.oldRow == null ? value.newRow : value.oldRow, tx.timestamp())); // Keep first old value.

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        KeyWrapper key = extractAndWrapKey(row);

        final Value old = store.get(key);

        store.put(key, new Value(row, old == null ? null : old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(old.newRow);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        final Value old = store.get(key);

        store.put(key, new Value(null, old == null ? null : old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(old != null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        final List<BinaryRow> res = keyRows.stream()
            .map(this::extractAndWrapKey)
            .map(store::get)
            .map(value -> resolve(value, tx.timestamp()))
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows)
            upsert(row, tx);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        KeyWrapper key = extractAndWrapKey(row);

        Value old = store.get(key);

        if (old != null && old.newRow != null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, null, tx == null ? null : tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        Collection<BinaryRow> inserted = new ArrayList<>();

        for (BinaryRow row : rows) {
            CompletableFuture<Boolean> fut = insert(row, tx);

            if (fut.isDone())
                inserted.add(row);
        }

        return CompletableFuture.completedFuture(inserted);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);
        final Value oldRow = store.get(key);

        if (oldRow == null || oldRow.newRow == null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, oldRow.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        final KeyWrapper key = extractAndWrapKey(oldRow);

        final Value old = store.get(key);

        if (old == null || old.newRow == null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(newRow, old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction ts) {
        return null;
    }

    /** {@inheritDoc} */
    public CompletableFuture<Boolean> deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        final KeyWrapper key = extractAndWrapKey(row);
        final Value old = store.get(key);

        if (old == null || old.newRow == null || !equalValues(row, old.newRow))
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction ts) {
        return null;
    }

    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, InternalTransaction ts) {
        return null;
    }

    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction ts) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
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

    public void reset(@NotNull BinaryRow row) {
        final KeyWrapper key = extractAndWrapKey(row);

        Value val = store.get(key);

        assert val != null;

        store.put(key, new Value(val.oldRow, null, null));
    }

    public void commit(Timestamp timestamp) {

    }

    public void rollback(Timestamp timestamp) {

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
