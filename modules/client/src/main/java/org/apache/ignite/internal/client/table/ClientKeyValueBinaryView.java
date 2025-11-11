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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getPartitionAwarenessProvider;
import static org.apache.ignite.internal.util.CompletableFutures.emptyCollectionCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyMapCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ViewUtils.checkKeysForNulls;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation for binary user-object representation.
 *
 * <p>NB: Binary view doesn't allow null tuples. Methods return either a tuple that represents the value, or {@code null} if no value
 * exists for the given key.
 */
public class ClientKeyValueBinaryView extends AbstractClientView<Entry<Tuple, Tuple>> implements KeyValueView<Tuple, Tuple> {
    /** Tuple serializer. */
    private final ClientTupleSerializer ser;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param sql Sql.
     */
    ClientKeyValueBinaryView(ClientTable tbl, ClientSql sql) {
        super(tbl, sql);

        ser = new ClientTupleSerializer(tbl.tableId());
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, Tuple key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                (s, r) -> ClientTupleSerializer.readValueTuple(s, r.in()),
                null,
                getPartitionAwarenessProvider(key),
                tx);
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

        if (keys.isEmpty()) {
            return emptyMapCompletedFuture();
        }

        List<Transaction> txns = new ArrayList<>();

        // Implicit getAll transaction is executed as multiple independent explicit transactions, coordinated from a client.
        IgniteTriFunction<Collection<Tuple>, PartitionAwarenessProvider, Boolean, CompletableFuture<Map<Tuple, Tuple>>> clo =
                (batch, provider, startImplicit) -> {
            Transaction tx0 = tbl.startImplicitTxIfNeeded(tx, txns, startImplicit);

            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_GET_ALL,
                    (s, w, n) -> ser.writeTuples(tx0, batch, s, w, n, true),
                    (s, r) -> ClientTupleSerializer.readKvTuplesNullable(s, r.in()),
                    Collections.emptyMap(),
                    provider,
                    tx0);
        };

        // TODO handle single partition batch as empty tx
        return tbl.splitAndRun(keys, clo, new HashMap<>(),
                (agg, cur) -> {
                    agg.putAll(cur);
                    return agg;
                },
                ClientTupleSerializer::getColocationHash, txns);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<Tuple> getNullable(@Nullable Transaction tx, Tuple key) {
        return sync(getNullableAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key, "key");

        // This method implemented for consistency and has the same semantics as regular get().
        // NullableValue.get() will never return null and there is no ambiguity between value absence and null result.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                (s, r) -> NullableValue.of(ClientTupleSerializer.readValueTuple(s, r.in())),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getOrDefault(@Nullable Transaction tx, Tuple key, Tuple defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getOrDefaultAsync(@Nullable Transaction tx, Tuple key, Tuple defaultValue) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                (s, r) -> ClientTupleSerializer.readValueTuple(s, r.in()),
                defaultValue,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, Tuple key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(containsAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return trueCompletedFuture();
        }

        List<Transaction> txns = new ArrayList<>();

        IgniteTriFunction<Collection<Tuple>, PartitionAwarenessProvider, Boolean, CompletableFuture<Boolean>> clo =
                (batch, provider, startImplicit) -> {
            Transaction tx0 = tbl.startImplicitTxIfNeeded(tx, txns, startImplicit);

            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_CONTAINS_ALL_KEYS,
                    (s, w, n) -> ser.writeTuples(tx0, batch, s, w, n, true),
                    r -> r.in().unpackBoolean(),
                    provider,
                    tx0);
        };

        return tbl.splitAndRun(keys, clo, Boolean.TRUE, (agg, cur) -> agg && cur, ClientTupleSerializer::getColocationHash);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                r -> null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs, "pairs");

        for (Entry<Tuple, Tuple> entry : pairs.entrySet()) {
            Objects.requireNonNull(entry.getKey(), "key");
            Objects.requireNonNull(entry.getValue(), "val");
        }

        if (pairs.isEmpty()) {
            return nullCompletedFuture();
        }

        IgniteTriFunction<Collection<Entry<Tuple, Tuple>>, PartitionAwarenessProvider, Boolean, CompletableFuture<Void>> clo =
                (batch, provider, startImplicit) -> {
            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_UPSERT_ALL,
                    (s, w, n) -> ser.writeKvTuples(tx, batch, s, w, n),
                    r -> null,
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(pairs.entrySet(), getPartitionAwarenessProvider(pairs.keySet().iterator().next()), false);
        }

        return tbl.splitAndRun(pairs.entrySet(), clo, null, (agg, cur) -> null,
                (schema, entry) -> ClientTupleSerializer.getColocationHash(schema, entry.getKey()));
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndPut(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndPutAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                (s, r) -> ClientTupleSerializer.readValueTuple(s, r.in()),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<Tuple> getNullableAndPut(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getNullableAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndPutAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        // This method implemented for consistency and has the same semantics as regular get().
        // NullableValue.get() will never return null and there is no ambiguity between value absence and null result.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                (s, r) -> NullableValue.of(ClientTupleSerializer.readValueTuple(s, r.in())),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
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
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> removeAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    @Override
    public void removeAll(@Nullable Transaction tx) {
        sync(removeAllAsync(tx));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<Tuple>> removeAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return emptyCollectionCompletedFuture();
        }

        IgniteTriFunction<Collection<Tuple>, PartitionAwarenessProvider, Boolean, CompletableFuture<Collection<Tuple>>> clo =
                (batch, provider, startImplicit) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_DELETE_ALL,
                    (s, w, n) -> ser.writeTuples(tx, batch, s, w, n, true),
                    (s, r) -> ClientTupleSerializer.readTuples(s, r.in(), true),
                    Collections.emptyList(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(keys, getPartitionAwarenessProvider(keys.iterator().next()), false);
        }

        return tbl.splitAndRun(keys, clo, new HashSet<>(),
                (agg, cur) -> {
                    agg.addAll(cur);
                    return agg;
                },
                ClientTupleSerializer::getColocationHash);
    }

    @Override
    public CompletableFuture<Void> removeAllAsync(@Nullable Transaction tx) {
        return sql.executeAsync(tx, "DELETE FROM " + tbl.name()).thenApply(r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndRemove(@Nullable Transaction tx, Tuple key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                (s, r) -> ClientTupleSerializer.readValueTuple(s, r.in()),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<Tuple> getNullableAndRemove(@Nullable Transaction tx, Tuple key) {
        return sync(getNullableAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key, "key");

        // This method implemented for consistency and has the same semantics as regular get().
        // NullableValue.get() will never return null and there is no ambiguity between value absence and null result.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w, n) -> ser.writeTuple(tx, key, s, w, n, true),
                (s, r) -> NullableValue.of(ClientTupleSerializer.readValueTuple(s, r.in())),
                null,
                getPartitionAwarenessProvider(key),
                tx);
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
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple key, Tuple oldVal, Tuple newVal) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(oldVal, "oldVal");
        Objects.requireNonNull(newVal, "newVal");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w, n) -> {
                    ser.writeKvTuple(tx, key, oldVal, s, w, n, false);
                    ser.writeKvTuple(tx, key, newVal, s, w, n, true);
                },
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                (s, r) -> ClientTupleSerializer.readValueTuple(s, r.in()),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<Tuple> getNullableAndReplace(@Nullable Transaction tx, Tuple key, Tuple val) {
        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndReplaceAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(val, "val");

        // This method implemented for consistency and has the same semantics as regular get().
        // NullableValue.get() will never return null and there is no ambiguity between value absence and null result.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w, n) -> ser.writeKvTuple(tx, key, val, s, w, n, false),
                (s, r) -> NullableValue.of(ClientTupleSerializer.readValueTuple(s, r.in())),
                null,
                getPartitionAwarenessProvider(key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(
            Publisher<DataStreamerItem<Entry<Tuple, Tuple>>> publisher,
            @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher, "publisher");

        var provider = new KeyValueTupleStreamerPartitionAwarenessProvider(tbl);
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<Entry<Tuple, Tuple>, Integer, Void> batchSender = (partition, items, deleted) -> tbl.doSchemaOutOpAsync(
                ClientOp.STREAMER_BATCH_SEND,
                (s, w, n) -> ser.writeStreamerKvTuples(partition, items, deleted, s, w),
                r -> null,
                PartitionAwarenessProvider.of(partition),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()),
                null);

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }

    @Override
    public <E, V, A, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<V, A, R> receiver,
            Function<E, Entry<Tuple, Tuple>> keyFunc,
            Function<E, V> payloadFunc,
            @Nullable A receiverArg,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);
        Objects.requireNonNull(keyFunc);
        Objects.requireNonNull(payloadFunc);
        Objects.requireNonNull(receiver);

        return ClientDataStreamer.streamData(
                publisher,
                keyFunc,
                payloadFunc,
                x -> false,
                options == null ? DataStreamerOptions.DEFAULT : options,
                new KeyValueTupleStreamerPartitionAwarenessProvider(tbl),
                tbl,
                resultSubscriber,
                receiver,
                receiverArg
        );
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, Entry<Tuple, Tuple>> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        String[] keyCols = columnNames(schema.keyColumns());
        String[] valCols = columnNames(schema.valColumns());

        return (row) -> new IgniteBiTuple<>(new SqlRowProjection(row, meta, keyCols), new SqlRowProjection(row, meta, valCols));
    }
}
