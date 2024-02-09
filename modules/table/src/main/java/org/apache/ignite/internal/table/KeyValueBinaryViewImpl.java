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
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation for binary user-object representation.
 *
 * <p>NB: Binary view doesn't allow null tuples. Methods return either a tuple that represents the value, or {@code null} if no value
 * exists for the given key.
 */
public class KeyValueBinaryViewImpl extends AbstractTableView<Entry<Tuple, Tuple>> implements KeyValueView<Tuple, Tuple> {
    private final TupleMarshallerCache marshallerCache;

    /**
     * The constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     * @param schemaVersions Schema versions access.
     * @param marshallers Marshallers provider.
     * @param sql Ignite SQL facade.
     */
    public KeyValueBinaryViewImpl(
            InternalTable tbl,
            SchemaRegistry schemaReg,
            SchemaVersions schemaVersions,
            MarshallersProvider marshallers,
            IgniteSql sql
    ) {
        super(tbl, schemaVersions, schemaReg, sql, marshallers);

        marshallerCache = new TupleMarshallerCache(schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, Tuple key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row keyRow = marshal(key, null, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx).thenApply(row -> unmarshalValue(row, schemaVersion));
        });
    }

    /**
     * This method is not supported, {@link #get(Transaction, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public NullableValue<Tuple> getNullable(@Nullable Transaction tx, Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /**
     * This method is not supported, {@link #getAsync(Transaction, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAsync(@Nullable Transaction tx, Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getOrDefault(@Nullable Transaction tx, Tuple key, Tuple defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getOrDefaultAsync(@Nullable Transaction tx, Tuple key, Tuple defaultValue) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            BinaryRowEx keyRow = marshal(key, null, schemaVersion);

            return tbl.get(keyRow, (InternalTransaction) tx)
                    .thenApply(r -> IgniteUtils.nonNullOrElse(unmarshalValue(r, schemaVersion), defaultValue));
        });
    }

    /** {@inheritDoc} */
    @Override
    public Map<Tuple, Tuple> getAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        checkKeysForNulls(keys);

        return withSchemaSync(tx, (schemaVersion) -> {
            List<BinaryRowEx> keyRows = marshalKeys(keys, schemaVersion);

            return tbl.getAll(keyRows, (InternalTransaction) tx).thenApply(rows -> unmarshalValues(rows, schemaVersion));
        });
    }

    private static void checkKeysForNulls(Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        for (Tuple key : keys) {
            Objects.requireNonNull(key);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, Tuple key) {
        return get(tx, key) != null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, Tuple key) {
        return getAsync(tx, key).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, Tuple key, Tuple val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, val, schemaVersion);

            return tbl.upsert(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs);
        for (Entry<Tuple, Tuple> entry : pairs.entrySet()) {
            Objects.requireNonNull(entry.getKey());
            Objects.requireNonNull(entry.getValue());
        }

        return withSchemaSync(tx, (schemaVersion) -> {
            return tbl.upsertAll(marshalPairs(pairs.entrySet(), schemaVersion), (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndPut(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndPutAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, val, schemaVersion);

            return tbl.getAndUpsert(row, (InternalTransaction) tx).thenApply(resultRow -> unmarshalValue(resultRow, schemaVersion));
        });
    }

    /**
     * This method is not supported, {@link #getAndPut(Transaction, Tuple, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public NullableValue<Tuple> getNullableAndPut(@Nullable Transaction tx, Tuple key, Tuple val) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /**
     * This method is not supported, {@link #getAndPutAsync(Transaction, Tuple, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndPutAsync(@Nullable Transaction tx, Tuple key,
            Tuple val) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, val, schemaVersion);

            return tbl.insert(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, Tuple key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, null, schemaVersion);

            return tbl.delete(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, val, schemaVersion);

            return tbl.deleteExact(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> removeAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<Tuple>> removeAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        checkKeysForNulls(keys);

        return withSchemaSync(tx, (schemaVersion) -> {
            List<BinaryRowEx> keyRows = marshalKeys(keys, schemaVersion);

            return tbl.deleteAll(keyRows, (InternalTransaction) tx).thenApply(rows -> unmarshalKeys(rows, schemaVersion));
        });
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndRemove(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return withSchemaSync(tx, (schemaVersion) -> {
            return tbl.getAndDelete(marshal(key, null, schemaVersion), (InternalTransaction) tx)
                    .thenApply(row -> unmarshalValue(row, schemaVersion));
        });
    }

    /**
     * This method is not supported, {@link #getAndRemove(Transaction, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public NullableValue<Tuple> getNullableAndRemove(@Nullable Transaction tx, Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /**
     * This method is not supported, {@link #getAndRemoveAsync(Transaction, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, Tuple key, Tuple oldVal, Tuple newVal) {
        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row row = marshal(key, val, schemaVersion);

            return tbl.replace(row, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(
            @Nullable Transaction tx,
            Tuple key,
            Tuple oldVal,
            Tuple newVal
    ) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(oldVal);
        Objects.requireNonNull(newVal);

        return withSchemaSync(tx, (schemaVersion) -> {
            Row oldRow = marshal(key, oldVal, schemaVersion);
            Row newRow = marshal(key, newVal, schemaVersion);

            return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
        });
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return withSchemaSync(tx, (schemaVersion) -> {
            return tbl.getAndReplace(marshal(key, val, schemaVersion), (InternalTransaction) tx)
                    .thenApply(row -> unmarshalValue(row, schemaVersion));
        });
    }

    /**
     * This method is not supported, {@link #getAndReplace(Transaction, Tuple, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public NullableValue<Tuple> getNullableAndReplace(@Nullable Transaction tx, Tuple key, Tuple val) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /**
     * This method is not supported, {@link #getAndReplaceAsync(Transaction, Tuple, Tuple)} must be used instead.
     *
     * @throws UnsupportedOperationException unconditionally.
     */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndReplaceAsync(@Nullable Transaction tx, Tuple key,
            Tuple val) {
        throw new UnsupportedOperationException("Binary view doesn't allow null tuples.");
    }

    /**
     * Marshal key-value pair to a row.
     *
     * @param key Key.
     * @param val Value.
     * @param schemaVersion Schema version to use when marshalling
     * @return Row.
     * @throws IgniteException If failed to marshal key and/or value.
     */
    private Row marshal(Tuple key, @Nullable Tuple val, int schemaVersion) throws IgniteException {
        try {
            return marshallerCache.marshaller(schemaVersion).marshal(key, val);
        } catch (TupleMarshallerException ex) {
            throw new MarshallerException(ex);
        }
    }

    /**
     * Returns value tuple of given row.
     *
     * @param row Binary row.
     * @param schemaVersion The version to use when unmarshalling.
     * @return Value tuple.
     */
    private @Nullable Tuple unmarshalValue(BinaryRow row, int schemaVersion) {
        if (row == null) {
            return null;
        }

        return TableRow.valueTuple(rowConverter.resolveRow(row, schemaVersion));
    }

    /**
     * Returns key-value pairs of tuples for given rows.
     *
     * @param rows Binary rows.
     * @param schemaVersion The version to use when unmarshalling.
     * @return Key-value pairs of tuples.
     */
    private Map<Tuple, Tuple> unmarshalValues(Collection<BinaryRow> rows, int schemaVersion) {
        Map<Tuple, Tuple> pairs = IgniteUtils.newHashMap(rows.size());

        for (Row row : rowConverter.resolveRows(rows, schemaVersion)) {
            if (row != null) {
                pairs.put(TableRow.keyTuple(row), TableRow.valueTuple(row));
            }
        }

        return pairs;
    }

    /**
     * Marshal key tuples to rows.
     *
     * @param keys Key tuples.
     * @param schemaVersion Schema version to use when marshalling.
     * @return Rows.
     */
    private List<BinaryRowEx> marshalKeys(Collection<Tuple> keys, int schemaVersion) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }

        List<BinaryRowEx> keyRows = new ArrayList<>(keys.size());

        for (Tuple keyRec : keys) {
            keyRows.add(marshal(Objects.requireNonNull(keyRec), null, schemaVersion));
        }
        return keyRows;
    }

    /**
     * Returns key tuples of given row.
     *
     * @param rows Binary rows.
     * @param schemaVersion Schema version to use when marshalling.
     * @return Keys.
     */
    private Collection<Tuple> unmarshalKeys(Collection<BinaryRow> rows, int schemaVersion) {
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        List<Tuple> tuples = new ArrayList<>(rows.size());

        for (Row row : rowConverter.resolveKeys(rows, schemaVersion)) {
            tuples.add(TableRow.keyTuple(row));
        }

        return tuples;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<Entry<Tuple, Tuple>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var partitioner = new KeyValueTupleStreamerPartitionAwarenessProvider(rowConverter.registry(), tbl.partitions());
        StreamerBatchSender<Entry<Tuple, Tuple>, Integer> batchSender = (partitionId, items, deleted) ->
                withSchemaSync(
                        null,
                        schemaVersion -> this.tbl.updateAll(marshalPairs(items, schemaVersion), deleted, partitionId));

        return DataStreamer.streamData(publisher, options, batchSender, partitioner);
    }

    private List<BinaryRowEx> marshalPairs(Collection<Entry<Tuple, Tuple>> pairs, int schemaVersion) {
        List<BinaryRowEx> rows = new ArrayList<>(pairs.size());

        for (Entry<Tuple, Tuple> pair : pairs) {
            Row row = marshal(Objects.requireNonNull(pair.getKey()), Objects.requireNonNull(pair.getValue()), schemaVersion);

            rows.add(row);
        }

        return rows;
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, Entry<Tuple, Tuple>> queryMapper(ResultSetMetadata meta, SchemaDescriptor schema) {
        return (row) -> new IgniteBiTuple<>(
                new SqlRowProjection(row, meta, columnNames(schema.keyColumns())),
                new SqlRowProjection(row, meta, columnNames(schema.valueColumns()))
        );
    }
}
