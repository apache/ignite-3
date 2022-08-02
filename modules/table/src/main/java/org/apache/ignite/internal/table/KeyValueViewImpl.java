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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class KeyValueViewImpl<K, V> extends AbstractTableView implements KeyValueView<K, V> {
    /** Marshaller factory. */
    private final Function<SchemaDescriptor, KvMarshaller<K, V>> marshallerFactory;

    /** Key-value marshaller. */
    private volatile KvMarshaller<K, V> marsh;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KeyValueViewImpl(
            InternalTable tbl,
            SchemaRegistry schemaReg,
            Mapper<K> keyMapper,
            Mapper<V> valueMapper
    ) {
        super(tbl, schemaReg);

        marshallerFactory = (schema) -> new KvMarshallerImpl<>(schema, keyMapper, valueMapper);
    }

    /** {@inheritDoc} */
    @Override
    public V get(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::unmarshallValue);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, @NotNull K key) {
        return sync(getNullableAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r)));
    }

    /** {@inheritDoc} */
    @Override
    public V getOrDefault(@Nullable Transaction tx, @NotNull K key, V defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, @NotNull K key, V defaultValue) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(r -> IgniteUtils.nonNullOrElse(unmarshalNullableValue(r), defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(keys));

        return tbl.getAll(rows, (InternalTransaction) tx).thenApply(this::unmarshalPairs);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, @NotNull K key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, @NotNull K key, V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.upsert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(pairs));

        return tbl.upsertAll(rows, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.getAndUpsert(marshal(key, val), (InternalTransaction) tx).thenApply(this::unmarshallValue);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getNullableAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.getAndUpsert(row, (InternalTransaction) tx)
                       .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r)));
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.insert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key));

        return tbl.delete(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Collection<BinaryRowEx> rows = marshal(Objects.requireNonNull(keys));

        return tbl.deleteAll(rows, (InternalTransaction) tx).thenApply(this::unmarshalKeys);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.getAndDelete(keyRow, (InternalTransaction) tx).thenApply(this::unmarshallValue);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return sync(getNullableAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRowEx keyRow = marshal(Objects.requireNonNull(key));

        return tbl.getAndDelete(keyRow, (InternalTransaction) tx)
                       .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r)));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        BinaryRowEx oldRow = marshal(key, oldVal);
        BinaryRowEx newRow = marshal(key, newVal);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.getAndReplace(marshal(key, val), (InternalTransaction) tx).thenApply(this::unmarshallValue);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRowEx row = marshal(Objects.requireNonNull(key), val);

        return tbl.getAndReplace(row, (InternalTransaction) tx)
                       .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r)));
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(
            @Nullable Transaction tx,
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @Nullable Transaction tx,
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> Map<K, R> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Returns marshaller.
     *
     * @param schemaVersion Schema version.
     */
    private KvMarshaller<K, V> marshaller(int schemaVersion) {
        KvMarshaller<K, V> marsh = this.marsh;

        if (marsh != null && marsh.schemaVersion() == schemaVersion) {
            return marsh;
        }

        // TODO: Cache marshaller for schema version or upgrade row?

        return this.marsh = marshallerFactory.apply(schemaReg.schema(schemaVersion));
    }

    /**
     * Marshal key.
     *
     * @param key Key object.
     * @return Binary row.
     */
    private BinaryRowEx marshal(@NotNull K key) {
        final KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Marshal key-value pair to a row.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Binary row.
     */
    private BinaryRowEx marshal(@NotNull K key, V val) {
        final KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshal(key, val);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Marshal keys to a row.
     *
     * @param keys Key objects.
     * @return Binary rows.
     */
    private Collection<BinaryRowEx> marshal(Collection<K> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        List<BinaryRowEx> keyRows = new ArrayList<>(keys.size());

        try {
            for (K key : keys) {
                keyRows.add(marsh.marshal(Objects.requireNonNull(key)));
            }
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }

        return keyRows;
    }

    /**
     * Marshal key-value pairs.
     *
     * @param pairs Key-value map.
     * @return Binary rows.
     */
    private List<BinaryRowEx> marshal(Map<K, V> pairs) {
        if (pairs.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        List<BinaryRowEx> rows = new ArrayList<>(pairs.size());

        try {
            for (Map.Entry<K, V> pair : pairs.entrySet()) {
                rows.add(marsh.marshal(Objects.requireNonNull(pair.getKey()), pair.getValue()));
            }
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }

        return rows;
    }

    /**
     * Marshal keys.
     *
     * @param rows Binary rows.
     * @return Keys.
     */
    private Collection<K> unmarshalKeys(Collection<BinaryRow> rows) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        List<K> keys = new ArrayList<>(rows.size());

        try {
            for (Row row : schemaReg.resolve(rows)) {
                if (row != null) {
                    keys.add(marsh.unmarshalKey(row));
                }
            }

            return keys;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Unmarshal value object from given binary row.
     *
     * @param binaryRow Binary row.
     * @return Value object or {@code null} if not exists.
     */
    private @Nullable V unmarshalNullableValue(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null || !binaryRow.hasValue()) {
            return null;
        }

        Row row = schemaReg.resolve(binaryRow);

        KvMarshaller<K, V> marshaller = marshaller(row.schemaVersion());

        try {
            return marshaller.unmarshalValue(row);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Marshal key-value pairs.
     *
     * @param rows Binary rows.
     * @return Key-value pairs.
     */
    private Map<K, V> unmarshalPairs(Collection<BinaryRow> rows) {
        if (rows.isEmpty()) {
            return Collections.emptyMap();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        Map<K, V> pairs = IgniteUtils.newHashMap(rows.size());

        try {
            for (Row row : schemaReg.resolve(rows)) {
                pairs.put(marsh.unmarshalKey(row), marsh.unmarshalValue(row));
            }

            return pairs;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Unmarshal value object from given binary row or fail, if value object is null.
     *
     *
     * @param binaryRow Binary row.
     * @return Value object or {@code null} if not exists.
     * @throws UnexpectedNullValueException if value object is null.
     */
    private @Nullable V unmarshallValue(@Nullable BinaryRow binaryRow) {
        if (binaryRow == null) {
            return null;
        }

        V v = unmarshalNullableValue(binaryRow);

        if (v == null) {
            throw new UnexpectedNullValueException("use `getNullable` sibling method instead.");
        }

        return v;
    }
}
