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

package org.apache.ignite.internal.table.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.TransactionImpl;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy table storage implementation with tx support.
 * All methods are expected to be called under read/write locks.
 */
public class DummyInternalTableWithTxImpl implements InternalTable {
    /** In-memory dummy store. */
    private final Map<KeyWrapper, Value> store = new HashMap<>();

    /** */
    private final Map<Timestamp, TxState> states = new ConcurrentHashMap<>();

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

    /** {@inheritDoc} */
    @Override public @NotNull UUID tableId() {
        return UUID.randomUUID();
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull SchemaMode schemaMode() {
        return SchemaMode.STRICT_SCHEMA;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaMode schemaMode) {
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        return CompletableFuture.completedFuture(resolve(store.get(extractAndWrapKey(row)), tx));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        store.compute(extractAndWrapKey(row), (key, value) ->
            value == null ? new Value(row, null, tx.timestamp()) :
                new Value(row, value.oldRow == null ? value.newRow : value.oldRow, tx.timestamp())); // Keep first old value.

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row, InternalTransaction tx) {
        assert row != null;

        KeyWrapper key = extractAndWrapKey(row);

        final Value old = store.get(key);

        store.put(key, new Value(row, old == null ? null : old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(old.newRow);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);

        final Value old = store.get(key);

        store.put(key, new Value(null, old == null ? null : old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(old != null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        final List<BinaryRow> res = keyRows.stream()
            .map(this::extractAndWrapKey)
            .map(store::get)
            .map(value -> resolve(value, tx))
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows)
            upsert(row, tx);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        KeyWrapper key = extractAndWrapKey(row);

        Value old = store.get(key);

        if (old != null && old.newRow != null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, null, tx == null ? null : tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
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
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);
        final Value oldRow = store.get(key);

        if (oldRow == null || oldRow.newRow == null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, oldRow.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        assert oldRow != null;
        assert newRow != null;

        final KeyWrapper key = extractAndWrapKey(oldRow);

        final Value old = store.get(key);

        if (old == null || old.newRow == null)
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(newRow, old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow row, InternalTransaction tx) {
        assert row != null;
        assert row.hasValue();

        final KeyWrapper key = extractAndWrapKey(row);
        final Value old = store.get(key);

        if (old == null || old.newRow == null || !equalValues(row, old.newRow))
            return CompletableFuture.completedFuture(false);

        store.put(key, new Value(row, old.oldRow, tx.timestamp()));

        return CompletableFuture.completedFuture(true);
    }

    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        InternalTransaction tx) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private DummyInternalTableWithTxImpl.KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
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
    private BinaryRow resolve(@Nullable Value value, @Nullable Transaction tx) {
        if (value == null)
            return null;

        if (tx != null) {
            TransactionImpl tx0 = (TransactionImpl) tx;

            if (!tx0.timestamp().equals(value.timestamp))
                assert value.oldRow == null : value + " " + tx;

            return value.newRow;
        }
        else {
            if (value.timestamp != null) {
                TxState state = states.get(value.timestamp);

                if (state == TxState.COMMITED)
                    return value.newRow;
                else
                    return value.oldRow;
            }
            else
                return value.newRow;
        }
    }

    @Override public void commit(Timestamp ts) {
        states.put(ts, TxState.COMMITED);
    }

    public void cleanup(@NotNull BinaryRow row) {
        // TODO
    }

    public void abort(@NotNull BinaryRow row) {
        final KeyWrapper key = extractAndWrapKey(row);

        Value val = store.get(key);

        assert val != null;

        store.put(key, new Value(val.oldRow, null, null));
    }
}
