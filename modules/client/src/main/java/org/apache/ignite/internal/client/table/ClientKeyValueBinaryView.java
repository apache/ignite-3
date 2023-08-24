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

import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerOptions;
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
public class ClientKeyValueBinaryView implements KeyValueView<Tuple, Tuple> {
    /** Underlying table. */
    private final ClientTable tbl;

    /** Tuple serializer. */
    private final ClientTupleSerializer ser;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public ClientKeyValueBinaryView(ClientTable tbl) {
        assert tbl != null;

        this.tbl = tbl;
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
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                ClientTupleSerializer::readValueTuple,
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public Map<Tuple, Tuple> getAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> ser.writeTuples(tx, keys, s, w, true),
                ClientTupleSerializer::readKvTuplesNullable,
                Collections.emptyMap(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keys.iterator().next()));
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
        return getOrDefaultAsync(tx, key, defaultValue).join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getOrDefaultAsync(@Nullable Transaction tx, Tuple key, Tuple defaultValue) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                (s, r) -> IgniteUtils.nonNullOrElse(ClientTupleSerializer.readValueTuple(s, r), defaultValue),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, Tuple key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        if (pairs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> ser.writeKvTuples(tx, pairs.entrySet(), s, w),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, pairs.keySet().iterator().next()));
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                ClientTupleSerializer::readValueTuple,
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> removeAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<Tuple>> removeAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> ser.writeTuples(tx, keys, s, w, true),
                (s, r) -> ClientTupleSerializer.readTuples(s, r, true),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keys.iterator().next()));
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndRemove(@Nullable Transaction tx, Tuple key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Tuple> getAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> ser.writeTuple(tx, key, s, w, true),
                ClientTupleSerializer::readValueTuple,
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple key, Tuple oldVal, Tuple newVal) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    ser.writeKvTuple(tx, key, oldVal, s, w, false);
                    ser.writeKvTuple(tx, key, newVal, s, w, true);
                },
                ClientMessageUnpacker::unpackBoolean,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> ser.writeKvTuple(tx, key, val, s, w, false),
                ClientTupleSerializer::readValueTuple,
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, key));
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

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<Entry<Tuple, Tuple>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var provider = new KeyValueTupleStreamerPartitionAwarenessProvider(tbl);
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<Entry<Tuple, Tuple>, String> batchSender = (nodeName, items) -> tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> ser.writeKvTuples(null, items, s, w),
                r -> null,
                PartitionAwarenessProvider.of(nodeName),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()));

        //noinspection resource
        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }
}
