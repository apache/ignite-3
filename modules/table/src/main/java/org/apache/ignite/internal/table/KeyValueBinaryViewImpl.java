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
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.TableManager;
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

    /** Table manager. */
    private final TableManager tblMgr;

    /**
     * The constructor.
     *
     * @param tbl       Table storage.
     * @param schemaReg Schema registry.
     * @param tblMgr    Table manager.
     * @param tx        The transaction.
     */
    public KeyValueBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg, TableManager tblMgr, Transaction tx) {
        super(tbl, schemaReg, tx);

        this.tblMgr = tblMgr;

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@NotNull Tuple key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row keyRow = marshal(key, null);

        return tbl.get(keyRow, tx).thenApply(this::unmarshal);
    }

    /**
     * Method throws UnsupportedOperationException, unconditionally.
     *
     * <p> Binary view doesn't support the operation because there is no ambiguity, {@code null} value means no rows found in the table for
     * the given key.
     */
    @Override
    public NullableValue<Tuple> getNullable(@NotNull Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null values. Please, use get(key) method instead.");
    }

    /**
     * Method throws UnsupportedOperationException, unconditionally.
     *
     * <p>Binary view doesn't support the operation because there is no ambiguity, {@code null} value means no rows found in the table for
     * the given key.
     */
    @Override
    public @NotNull CompletableFuture<NullableValue<Tuple>> getNullableAsync(@NotNull Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null values. Please, use getAsync(key) method instead.");
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getOrDefault(@NotNull Tuple key, @NotNull Tuple defaultValue) {
        return sync(getOrDefaultAsync(key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getOrDefaultAsync(@NotNull Tuple key, @NotNull Tuple defaultValue) {
        Objects.requireNonNull(defaultValue);

        BinaryRow keyRow = marshal(Objects.requireNonNull(key), null);

        return tbl.get(keyRow, tx).thenApply(r -> r == null ? defaultValue : unmarshal(r));
    }

    /** {@inheritDoc} */
    @Override
    public Map<Tuple, Tuple> getAll(@NotNull Collection<Tuple> keys) {
        return sync(getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        List<BinaryRow> keyRows = new ArrayList<>(keys.size());

        for (Tuple keyRec : keys) {
            final Row keyRow = marshal(keyRec, null);

            keyRows.add(keyRow);
        }

        return tbl.getAll(keyRows, tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@NotNull Tuple key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@NotNull Tuple key) {
        return getAsync(key).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@NotNull Tuple key, Tuple val) {
        sync(putAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.upsert(row, tx);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@NotNull Map<Tuple, Tuple> pairs) {
        sync(putAllAsync(pairs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs);

        List<BinaryRow> rows = new ArrayList<>(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs.entrySet()) {
            final Row row = marshal(pair.getKey(), pair.getValue());

            rows.add(row);
        }

        return tbl.upsertAll(rows, tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndPut(@NotNull Tuple key, Tuple val) {
        return sync(getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndPutAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.getAndUpsert(row, tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshal(key, val);

        return tbl.insert(row, tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@NotNull Tuple key) {
        return sync(removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(removeAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row row = marshal(key, null);

        return tbl.delete(row, tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshal(key, val);

        return tbl.deleteExact(row, tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> removeAll(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return sync(removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> removeAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        List<BinaryRow> keyRows = new ArrayList<>(keys.size());

        for (Tuple keyRec : keys) {
            final Row keyRow = marshal(keyRec, null);

            keyRows.add(keyRow);
        }

        return tbl.deleteAll(keyRows, tx)
                       .thenApply(t -> t.stream().filter(Objects::nonNull).map(this::unmarshal).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndRemove(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return sync(getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.getAndDelete(marshal(key, null), tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull Tuple key, @NotNull Tuple oldVal, @NotNull Tuple newVal) {
        return sync(replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshal(key, val);

        return tbl.replace(row, tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, @NotNull Tuple oldVal, @NotNull Tuple newVal) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(oldVal);
        Objects.requireNonNull(newVal);

        Row oldRow = marshal(key, oldVal);
        Row newRow = marshal(key, newVal);

        return tbl.replace(oldRow, newRow, tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.getAndReplace(marshal(key, val), tx).thenApply(this::unmarshal);
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @NotNull Tuple key,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> Map<Tuple, R> invokeAll(
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<Tuple, R>> invokeAllAsync(
            @NotNull Collection<Tuple> keys,
            InvokeProcessor<Tuple, Tuple, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public KeyValueBinaryViewImpl withTransaction(Transaction tx) {
        return new KeyValueBinaryViewImpl(tbl, schemaReg, tblMgr, tx);
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
