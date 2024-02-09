/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.TupleReader;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class KeyValueViewImpl<K, V> extends AbstractTableView<Entry<K, V>> implements KeyValueView<K, V> {
    /** Key class mapper. */
    private final Mapper<K> keyMapper;

    /** Value class mapper. */
    private final Mapper<V> valueMapper;

    /** Marshaller factory. */
    private final Function<SchemaDescriptor, KvMarshaller<K, V>> marshallerFactory;

    /** Key-value marshaller. */
    private volatile @Nullable KvMarshaller<K, V> marsh;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaRegistry Schema registry.
     * @param schemaVersions Schema versions access.
     * @param marshallers Marshallers provider.
     * @param sql Ignite SQL facade.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KeyValueViewImpl(
            InternalTable tbl,
            SchemaRegistry schemaRegistry,
            SchemaVersions schemaVersions,
            MarshallersProvider marshallers,
            IgniteSql sql,
            Mapper<K> keyMapper,
            Mapper<V> valueMapper
    ) {
        super(tbl, schemaVersions, schemaRegistry, sql, marshallers);

        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;

        marshallerFactory = (schema) -> new KvMarshallerImpl<>(schema, marshallers, keyMapper, valueMapper);
    }

    /** {@inheritDoc} */
    @Override
    public V get(@Nullable Transaction tx, K key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx).thenApply(binaryRow -> unmarshalValue(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return sync(getNullableAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx)
                    .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r, schemaVersion)));
        });
    }

    /** {@inheritDoc} */
    @Override
    public V getOrDefault(@Nullable Transaction tx, K key, V defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, V defaultValue) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx)
                    .thenApply(r -> IgniteUtils.nonNullOrElse(unmarshalNullableValue(r, schemaVersion), defaultValue));
        });
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        checkKeysForNulls(keys);

        return withSchemaSync(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshal(keys, schemaVersion);

            return tbl.getAll(rows, (InternalTransaction) tx).thenApply(resultRows -> unmarshalPairs(resultRows, schemaVersion));
        });
    }

    private static <K> void checkKeysForNulls(Collection<K> keys) {
        Objects.requireNonNull(keys);

        for (K key : keys) {
            Objects.requireNonNull(key);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx).thenApply(Objects::nonNull);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.upsert(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        Objects.requireNonNull(pairs);
        for (Entry<K, V> entry : pairs.entrySet()) {
            Objects.requireNonNull(entry.getKey());
        }

        return withSchemaSync(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshalPairs(pairs.entrySet(), schemaVersion);

            return tbl.upsertAll(rows, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            return tbl.getAndUpsert(marshal(key, val, schemaVersion), (InternalTransaction) tx)
                    .thenApply(binaryRow -> unmarshalValue(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(getNullableAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.getAndUpsert(row, (InternalTransaction) tx)
                   .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r, schemaVersion)));
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.insert(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, schemaVersion);

            return tbl.delete(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.deleteExact(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        checkKeysForNulls(keys);

        return withSchemaSync(tx, (schemaVersion) -> {
            Collection<BinaryRowEx> rows = marshal(keys, schemaVersion);

            return tbl.deleteAll(rows, (InternalTransaction) tx).thenApply(resultRows -> unmarshalKeys(resultRows, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, K key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.getAndDelete(keyRow, (InternalTransaction) tx)
                    .thenApply(binaryRow -> unmarshalValue(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return sync(getNullableAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, schemaVersion);

            return tbl.getAndDelete(keyRow, (InternalTransaction) tx)
                   .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r, schemaVersion)));
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.replace(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx oldRow = marshal(key, oldVal, schemaVersion);
            BinaryRowEx newRow = marshal(key, newVal, schemaVersion);

            return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            return tbl.getAndReplace(marshal(key, val, schemaVersion), (InternalTransaction) tx)
                    .thenApply(binaryRow -> unmarshalValue(binaryRow, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx row = marshal(key, val, schemaVersion);

            return tbl.getAndReplace(row, (InternalTransaction) tx)
                   .thenApply(r -> r == null ? null : NullableValue.of(unmarshalNullableValue(r, schemaVersion)));
        });
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

        SchemaRegistry registry = rowConverter.registry();

        marsh = marshallerFactory.apply(registry.schema(schemaVersion));
        this.marsh = marsh;

        return marsh;
    }

    /**
     * Marshal key.
     *
     * @param key Key object.
     * @param schemaVersion Schema version to use when marshalling.
     * @return Binary row.
     */
    private BinaryRowEx marshal(K key, int schemaVersion) {
        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

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
     * @param schemaVersion Schema version to use when marshalling.
     * @return Binary row.
     */
    private BinaryRowEx marshal(K key, @Nullable V val, int schemaVersion) {
        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

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
     * @param schemaVersion Schema version to use when marshalling.
     * @return Binary rows.
     */
    private Collection<BinaryRowEx> marshal(Collection<K> keys, int schemaVersion) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

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
     * @param schemaVersion Schema version to use when marshalling.
     * @return Binary rows.
     */
    private List<BinaryRowEx> marshalPairs(Collection<Entry<K, V>> pairs, int schemaVersion) {
        if (pairs.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

        List<BinaryRowEx> rows = new ArrayList<>(pairs.size());

        try {
            for (Map.Entry<K, V> pair : pairs) {
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
     * @param schemaVersion Schema version to use when marshalling.
     * @return Keys.
     */
    private Collection<K> unmarshalKeys(Collection<BinaryRow> rows, int schemaVersion) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

        List<K> keys = new ArrayList<>(rows.size());

        try {
            for (Row row : rowConverter.resolveKeys(rows, schemaVersion)) {
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
     * @param schemaVersion Schema version to use when unmarshalling.
     * @return Value object or {@code null} if not exists.
     */
    private @Nullable V unmarshalNullableValue(@Nullable BinaryRow binaryRow, int schemaVersion) {
        if (binaryRow == null) {
            return null;
        }

        Row row = rowConverter.resolveRow(binaryRow, schemaVersion);

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
     * @param schemaVersion Schema version to use when unmarshalling.
     * @return Key-value pairs.
     */
    private Map<K, V> unmarshalPairs(Collection<BinaryRow> rows, int schemaVersion) {
        if (rows.isEmpty()) {
            return Collections.emptyMap();
        }

        KvMarshaller<K, V> marsh = marshaller(schemaVersion);

        Map<K, V> pairs = IgniteUtils.newHashMap(rows.size());

        try {
            for (Row row : rowConverter.resolveRows(rows, schemaVersion)) {
                if (row != null) {
                    pairs.put(marsh.unmarshalKey(row), marsh.unmarshalValue(row));
                }
            }

            return pairs;
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    /**
     * Unmarshal value object from given binary row or fail, if value object is null.
     *
     * @param binaryRow Binary row.
     * @param schemaVersion Schema version to use when unmarshalling.
     * @return Value object or {@code null} if not exists.
     * @throws UnexpectedNullValueException if value object is null.
     */
    private @Nullable V unmarshalValue(@Nullable BinaryRow binaryRow, int schemaVersion) {
        if (binaryRow == null) {
            return null;
        }

        V v = unmarshalNullableValue(binaryRow, schemaVersion);

        if (v == null) {
            throw new UnexpectedNullValueException("Got unexpected null value: use `getNullable` sibling method instead.");
        }

        return v;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<Entry<K, V>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
        // columns never change (so they are the same for all schema versions of the table),
        var partitioner = new KeyValuePojoStreamerPartitionAwarenessProvider<>(
                rowConverter.registry(),
                tbl.partitions(),
                marshaller(rowConverter.registry().lastKnownSchemaVersion())
        );

        StreamerBatchSender<Entry<K, V>, Integer> batchSender = (partitionId, items, deleted) ->
                withSchemaSync(
                        null,
                        schemaVersion -> this.tbl.updateAll(marshalPairs(items, schemaVersion), deleted, partitionId));

        return DataStreamer.streamData(publisher, options, batchSender, partitioner);
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, Entry<K, V>> queryMapper(ResultSetMetadata meta, SchemaDescriptor schema) {
        List<Column> keyCols = schema.keyColumns();
        List<Column> valCols = schema.valueColumns();

        MarshallerSchema marshallerSchema = schema.marshallerSchema();
        Marshaller keyMarsh = marshallers.getKeysMarshaller(marshallerSchema, keyMapper, false, true);
        Marshaller valMarsh = marshallers.getValuesMarshaller(marshallerSchema, valueMapper, false, true);

        return (row) -> {
            try {
                return new IgniteBiTuple<>(
                        (K) keyMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, columnNames(keyCols))), null),
                        (V) valMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, columnNames(valCols))), null)
                );
            } catch (MarshallerException e) {
                throw new org.apache.ignite.lang.MarshallerException(e);
            }
        };
    }
}
