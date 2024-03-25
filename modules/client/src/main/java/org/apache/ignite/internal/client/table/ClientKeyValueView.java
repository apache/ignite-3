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
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;
import static org.apache.ignite.internal.util.CompletableFutures.emptyCollectionCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyMapCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.TupleReader;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation.
 */
public class ClientKeyValueView<K, V> extends AbstractClientView<Entry<K, V>> implements KeyValueView<K, V> {
    /** Key serializer.  */
    private final ClientRecordSerializer<K> keySer;

    /** Value serializer.  */
    private final ClientRecordSerializer<V> valSer;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param sql Sql.
     * @param keyMapper Key mapper.
     * @param valMapper value mapper.
     */
    ClientKeyValueView(ClientTable tbl, ClientSql sql, Mapper<K> keyMapper, Mapper<V> valMapper) {
        super(tbl, sql);

        assert keyMapper != null;
        assert valMapper != null;

        keySer = new ClientRecordSerializer<>(tbl.tableId(), keyMapper);
        valSer = new ClientRecordSerializer<>(tbl.tableId(), valMapper);
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
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

        // Null means row does not exist, NullableValue.NULL means row exists, but mapped value column is null.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> valSer.readRec(s, r.in(), TuplePart.VAL),
                defaultValue,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return emptyMapCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> keySer.writeRecs(tx, keys, s, w, TuplePart.KEY),
                this::readGetAllResponse,
                Collections.emptyMap(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), keys.iterator().next()));
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

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, K key, V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
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

        if (pairs.isEmpty()) {
            return nullCompletedFuture();
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> {
                    writeSchemaAndTx(s, w, tx);
                    w.out().packInt(pairs.size());

                    for (Entry<K, V> e : pairs.entrySet()) {
                        writeKeyValueRaw(s, w, e.getKey(), e.getValue());
                    }
                },
                r -> null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), pairs.keySet().iterator().next()));
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, K key, V val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, V val) {
        return sync(getNullableAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return emptyCollectionCompletedFuture();
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> keySer.writeRecs(tx, keys, s, w, TuplePart.KEY),
                (s, r) -> keySer.readRecs(s, r.in(), false, TuplePart.KEY),
                Collections.emptyList(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), keys.iterator().next()));
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    writeSchemaAndTx(s, w, tx);
                    writeKeyValueRaw(s, w, key, oldVal);
                    writeKeyValueRaw(s, w, key, newVal);
                },
                r -> r.in().unpackBoolean(),
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, K key, V val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, V val) {
        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL)),
                null,
                ClientTupleSerializer.getPartitionAwarenessProvider(tx, keySer.mapper(), key));
    }

    private void writeKeyValue(ClientSchema s, PayloadOutputChannel w, @Nullable Transaction tx, K key, @Nullable V val) {
        writeSchemaAndTx(s, w, tx);
        writeKeyValueRaw(s, w, key, val);
    }

    private void writeKeyValueRaw(ClientSchema s, PayloadOutputChannel w, K key, @Nullable V val) {
        var builder = new BinaryTupleBuilder(s.columns().length);
        var noValueSet = new BitSet();
        ClientMarshallerWriter writer = new ClientMarshallerWriter(builder, noValueSet);

        try {
            s.getMarshaller(keySer.mapper(), TuplePart.KEY, false).writeObject(key, writer);
            s.getMarshaller(valSer.mapper(), TuplePart.VAL, false).writeObject(val, writer);
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }

        w.out().packBinaryTuple(builder, noValueSet);
    }

    private void writeSchemaAndTx(ClientSchema s, PayloadOutputChannel w, @Nullable Transaction tx) {
        w.out().packInt(tbl.tableId());
        writeTx(tx, w);
        w.out().packInt(s.version());
    }

    private HashMap<K, V> readGetAllResponse(ClientSchema schema, PayloadInputChannel in) {
        var cnt = in.in().unpackInt();

        var res = new LinkedHashMap<K, V>(cnt);

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL);

        try {
            for (int i = 0; i < cnt; i++) {
                // TODO: Optimize (IGNITE-16022).
                if (in.in().unpackBoolean()) {
                    var tupleReader = new BinaryTupleReader(schema.columns().length, in.in().readBinaryUnsafe());
                    var reader = new ClientMarshallerReader(tupleReader, schema.columns());
                    res.put((K) keyMarsh.readObject(reader, null), (V) valMarsh.readObject(reader, null));
                }
            }

            return res;
        } catch (MarshallerException e) {
            throw new IgniteException(INTERNAL_ERR, e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<Entry<K, V>>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher);

        var provider = new KeyValuePojoStreamerPartitionAwarenessProvider<K, V>(tbl, keySer.mapper());
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<Entry<K, V>, Integer> batchSender = (partition, items, deleted) -> tbl.doSchemaOutOpAsync(
                ClientOp.STREAMER_BATCH_SEND,
                (s, w) -> {
                    w.out().packInt(tbl.tableId());
                    w.out().packInt(partition);
                    w.out().packBitSetNullable(deleted);
                    w.out().packInt(s.version());
                    w.out().packInt(items.size());

                    int i = 0;

                    Marshaller keyMarsh = s.getMarshaller(keySer.mapper(), TuplePart.KEY, false);
                    Marshaller valMarsh = s.getMarshaller(valSer.mapper(), TuplePart.VAL, false);
                    var noValueSet = new BitSet();

                    for (Entry<K, V> e : items) {
                        boolean del = deleted != null && deleted.get(i++);
                        int colCount = del ? s.keyColumns().length : s.columns().length;

                        noValueSet.clear();
                        var builder = new BinaryTupleBuilder(colCount);
                        ClientMarshallerWriter writer = new ClientMarshallerWriter(builder, noValueSet);

                        try {
                            keyMarsh.writeObject(e.getKey(), writer);

                            if (!del) {
                                valMarsh.writeObject(e.getValue(), writer);
                            }
                        } catch (MarshallerException ex) {
                            throw new IgniteException(INTERNAL_ERR, ex.getMessage(), ex);
                        }

                        w.out().packBinaryTuple(builder, noValueSet);
                    }
                },
                r -> null,
                PartitionAwarenessProvider.of(partition),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()));

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, Entry<K, V>> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        String[] keyCols = columnNames(schema.keyColumns());
        String[] valCols = columnNames(schema.valColumns());

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY, true);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL, true);

        return (row) -> {
            try {
                return new IgniteBiTuple<>(
                        (K) keyMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, keyCols)), null),
                        (V) valMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, valCols)), null)
                );
            } catch (MarshallerException e) {
                throw new org.apache.ignite.lang.MarshallerException(e);
            }
        };
    }

    private static <T> T throwIfNull(T obj) {
        if (obj == null) {
            throw new UnexpectedNullValueException("Got unexpected null value: use `getNullable` sibling method instead.");
        }

        return obj;
    }
}
