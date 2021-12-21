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

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation for binary user-object representation.
 */
public class KeyValueBinaryViewImpl extends AbstractTableView implements KeyValueView<Tuple, Tuple> {
    /** The marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * The constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     */
    public KeyValueBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, @NotNull Tuple key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, @NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row keyRow = marshal(key, null);

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /**
     * This method is not supported, {@link #get(Transaction, Tuple)} must be used instead.
     *
     * <p>Because of binary view doesn't allow null values, binary view method {@link #get(Transaction, Tuple)} returns {@code null} if and
     * only if no value exists for the given key. Thus, this method is redundant.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public NullableValue<Tuple> getNullable(@Nullable Transaction tx, @NotNull Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null values.");
    }

    /**
     * This method is not supported, {@link #getAsync(Transaction, Tuple)} must be used instead.
     *
     * <p>Because of binary view doesn't allow null values, binary view method {@link #get(Transaction, Tuple)} returns {@code null} if and
     * only if no value exists for the given key. Thus, this method is redundant.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public @NotNull CompletableFuture<NullableValue<Tuple>> getNullableAsync(@Nullable Transaction tx, @NotNull Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null values.");
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getOrDefault(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getOrDefaultAsync(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple defaultValue) {
        Objects.requireNonNull(defaultValue);

        BinaryRow keyRow = marshal(Objects.requireNonNull(key), null);

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(r -> r == null ? defaultValue : unmarshal(r));
    }

    /** {@inheritDoc} */
    @Override
    public Map<Tuple, Tuple> getAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        List<BinaryRow> keyRows = new ArrayList<>(keys.size());

        for (Tuple keyRec : keys) {
            final Row keyRow = marshal(keyRec, null);

            keyRows.add(keyRow);
        }

        return tbl.getAll(keyRows, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, @NotNull Tuple key) {
        return get(tx, key) != null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull Tuple key) {
        return getAsync(tx, key).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, @NotNull Tuple key, Tuple val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.upsert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, @NotNull Map<Tuple, Tuple> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs);

        List<BinaryRow> rows = new ArrayList<>(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs.entrySet()) {
            final Row row = marshal(pair.getKey(), pair.getValue());

            rows.add(row);
        }

        return tbl.upsertAll(rows, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndPut(@Nullable Transaction tx, @NotNull Tuple key, Tuple val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndPutAsync(@Nullable Transaction tx, @NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.getAndUpsert(row, (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.insert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull Tuple key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row row = marshal(key, null);

        return tbl.delete(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshal(key, val);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> removeAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        List<BinaryRow> keyRows = new ArrayList<>(keys.size());

        for (Tuple keyRec : keys) {
            final Row keyRow = marshal(keyRec, null);

            keyRows.add(keyRow);
        }

        return tbl.deleteAll(keyRows, (InternalTransaction) tx)
                       .thenApply(binaryRows -> {
                           List<Tuple> tuples = new ArrayList<>(binaryRows.size());

                           for (BinaryRow r : binaryRows) {
                               if (r != null) {
                                   tuples.add(this.unmarshal(r));
                               }
                           }

                           return tuples;
                       });
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndRemove(@Nullable Transaction tx, @NotNull Tuple key) {
        Objects.requireNonNull(key);

        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(@Nullable Transaction tx, @NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.getAndDelete(marshal(key, null), (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple oldVal, @NotNull Tuple newVal) {
        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshal(key, val);

        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(
            @Nullable Transaction tx,
            @NotNull Tuple key,
            @NotNull Tuple oldVal,
            @NotNull Tuple newVal
    ) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(oldVal);
        Objects.requireNonNull(newVal);

        Row oldRow = marshal(key, oldVal);
        Row newRow = marshal(key, newVal);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, @NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.getAndReplace(marshal(key, val), (InternalTransaction) tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(
            @Nullable Transaction tx,
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @Nullable Transaction tx,
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> Map<Tuple, R> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<Tuple, R>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Marshal key-value pair to a row.
     *
     * @param key Key.
     * @param val Value.
     * @return Row.
     * @throws IgniteException If failed to marshal key and/or value.
     */
    private Row marshal(@NotNull Tuple key, @Nullable Tuple val) throws IgniteException {
        try {
            return marsh.marshal(key, val);
        } catch (TupleMarshallerException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Returns value tuple of given row.
     *
     * @param row Binary row.
     * @return Value tuple.
     */
    private Tuple unmarshal(BinaryRow row) {
        if (row == null) {
            return null;
        }

        return TableRow.valueTuple(schemaReg.resolve(row));
    }

    /**
     * Returns key-value pairs of tuples for given rows.
     *
     * @param rows Binary rows.
     * @return Key-value pairs of tuples.
     */
    public Map<Tuple, Tuple> unmarshal(Collection<BinaryRow> rows) {
        Map<Tuple, Tuple> pairs = new HashMap<>();

        for (Row row : schemaReg.resolve(rows)) {
            if (row.hasValue()) {
                pairs.put(TableRow.keyTuple(row), TableRow.valueTuple(row));
            }
        }

        return pairs;
    }
}
