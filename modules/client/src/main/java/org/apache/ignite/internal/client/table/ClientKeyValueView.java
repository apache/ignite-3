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

import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getColocationHash;
import static org.apache.ignite.internal.client.table.ClientTupleSerializer.getPartitionAwarenessProvider;
import static org.apache.ignite.internal.client.tx.DirectTxUtils.writeTx;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.marshaller.ValidationUtils.validateNullableOperation;
import static org.apache.ignite.internal.marshaller.ValidationUtils.validateNullableValue;
import static org.apache.ignite.internal.util.CompletableFutures.emptyCollectionCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.emptyMapCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ViewUtils.checkKeysForNulls;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.TupleReader;
import org.apache.ignite.internal.streamer.StreamerBatchSender;
import org.apache.ignite.internal.table.criteria.SqlRowProjection;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
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
        Objects.requireNonNull(key, "key");

        return sync(doGet(tx, key, "getNullable"));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        return doGet(tx, key, "getNullableAsync");
    }

    private CompletableFuture<V> doGet(@Nullable Transaction tx, K key, String altMethod) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL), altMethod),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullable");

        return sync(doGetNullable(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAsync");

        return doGetNullable(tx, key);
    }

    private CompletableFuture<NullableValue<V>> doGetNullable(@Nullable Transaction tx, K key) {
        // Null means row does not exist, NullableValue.NULL means row exists, but mapped value column is null.
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL)),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public V getOrDefault(@Nullable Transaction tx, K key, V defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, V defaultValue) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                (s, r) -> valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL),
                defaultValue,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
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

        if (keys.isEmpty()) {
            return emptyMapCompletedFuture();
        }

        BiFunction<Collection<K>, PartitionAwarenessProvider, CompletableFuture<Map<K, V>>> clo = (batch, provider) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_GET_ALL,
                    (s, w, n) -> keySer.writeRecs(tx, batch, s, w, n, TuplePart.KEY),
                    this::readGetAllResponse,
                    Collections.emptyMap(),
                    provider,
                    tx);
        };

        return tbl.split(tx, keys, clo, new HashMap<>(), (agg, cur) -> {
            agg.putAll(cur);
            return agg;
        }, (schema, entry) -> getColocationHash(schema, keySer.mapper(), entry));
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(containsAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return trueCompletedFuture();
        }

        BiFunction<Collection<K>, PartitionAwarenessProvider, CompletableFuture<Boolean>> clo = (batch, provider) -> {
            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_CONTAINS_ALL_KEYS,
                    (s, w, n) -> keySer.writeRecs(tx, batch, s, w, n, TuplePart.KEY),
                    r -> r.in().unpackBoolean(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(keys, getPartitionAwarenessProvider(keySer.mapper(), keys.iterator().next()));
        }

        return tbl.split(tx, keys, clo, Boolean.TRUE, (agg, cur) -> agg && cur,
                (schema, entry) -> getColocationHash(schema, keySer.mapper(), entry));
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, K key, V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                r -> null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        Objects.requireNonNull(pairs, "pairs");

        if (pairs.isEmpty()) {
            return nullCompletedFuture();
        }

        for (Entry<K, V> e : pairs.entrySet()) {
            Objects.requireNonNull(e.getKey(), "key");
            validateNullableValue(e.getValue(), valSer.mapper().targetType());
        }

        BiFunction<Collection<Entry<K, V>>, PartitionAwarenessProvider, CompletableFuture<Void>> clo = (batch, provider) -> {
            return tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_UPSERT_ALL,
                    (s, w, n) -> {
                        writeSchemaAndTx(s, w, n, tx);
                        w.out().packInt(batch.size());

                        for (Entry<K, V> e : batch) {
                            writeKeyValueRaw(s, w, e.getKey(), e.getValue());
                        }
                    },
                    r -> null,
                    provider,
                    tx);
        };

        if (tx == null) {
            return clo.apply(pairs.entrySet(), getPartitionAwarenessProvider(keySer.mapper(), pairs.keySet().iterator().next()));
        }

        return tbl.split(tx, pairs.entrySet(), clo, null, (agg, cur) -> null,
                (schema, entry) -> getColocationHash(schema, keySer.mapper(), entry.getKey()));
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return sync(doGetAndPut(tx, key, val, "getNullableAndPut"));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return doGetAndPut(tx, key, val, "getNullableAndPutAsync");
    }

    private CompletableFuture<V> doGetAndPut(@Nullable Transaction tx, K key, @Nullable V val, String altMethod) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL), altMethod),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndPut");
        validateNullableValue(val, valSer.mapper().targetType());

        return sync(doGetNullableAndPut(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndPutAsync");
        validateNullableValue(val, valSer.mapper().targetType());

        return doGetNullableAndPut(tx, key, val);
    }

    private CompletableFuture<NullableValue<V>> doGetNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL)),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
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
        Objects.requireNonNull(key, "key");

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    @Override
    public void removeAll(@Nullable Transaction tx) {
        sync(removeAllAsync(tx));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        checkKeysForNulls(keys);

        if (keys.isEmpty()) {
            return emptyCollectionCompletedFuture();
        }

        BiFunction<Collection<K>, PartitionAwarenessProvider, CompletableFuture<Collection<K>>> batchFunc = (batch, provider) -> {
            return tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_DELETE_ALL,
                    (s, w, n) -> keySer.writeRecs(tx, batch, s, w, n, TuplePart.KEY),
                    (s, r) -> keySer.readRecs(s, r.in(), false, TuplePart.KEY),
                    Collections.emptyList(),
                    provider,
                    tx);
        };

        if (tx == null) {
            return batchFunc.apply(keys, getPartitionAwarenessProvider(keySer.mapper(), keys.iterator().next()));
        }

        return tbl.split(tx, keys, batchFunc, new HashSet<>(), (agg, cur) -> {
            agg.addAll(cur);
            return agg;
        }, (schema, entry) -> getColocationHash(schema, keySer.mapper(), entry));
    }

    @Override
    public CompletableFuture<Void> removeAllAsync(@Nullable Transaction tx) {
        return sql.executeAsync(tx, "DELETE FROM " + tbl.name()).thenApply(r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        return sync(doGetAndRemove(tx, key, "getNullableAndRemove"));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        return doGetAndRemove(tx, key, "getNullableAndRemoveAsync");
    }

    private CompletableFuture<V> doGetAndRemove(@Nullable Transaction tx, K key, String altMethod) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL), altMethod),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndRemove");

        return sync(doGetNullableAndRemove(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndRemoveAsync");

        return doGetNullableAndRemove(tx, key);
    }

    private CompletableFuture<NullableValue<V>> doGetNullableAndRemove(@Nullable Transaction tx, K key) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w, n) -> keySer.writeRec(tx, key, s, w, n, TuplePart.KEY),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL)),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldVal, V newVal) {
        Objects.requireNonNull(key, "key");

        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, V oldVal, V newVal) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(oldVal, valSer.mapper().targetType());
        validateNullableValue(newVal, valSer.mapper().targetType());

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w, n) -> {
                    writeSchemaAndTx(s, w, n, tx);
                    writeKeyValueRaw(s, w, key, oldVal);
                    writeKeyValueRaw(s, w, key, newVal);
                },
                r -> r.in().unpackBoolean(),
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return sync(doGetAndReplace(tx, key, val, "getNullableAndReplace"));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableValue(val, valSer.mapper().targetType());

        return doGetAndReplace(tx, key, val, "getNullableAndReplaceAsync");
    }

    private CompletableFuture<V> doGetAndReplace(@Nullable Transaction tx, K key, V val, String altMethod) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                (s, r) -> throwIfNull(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL), altMethod),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndReplace");
        validateNullableValue(val, valSer.mapper().targetType());

        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, V val) {
        Objects.requireNonNull(key, "key");

        validateNullableOperation(valSer.mapper().targetType(), "getNullableAndReplaceAsync");
        validateNullableValue(val, valSer.mapper().targetType());

        return doGetNullableAndReplace(tx, key, val);
    }

    private CompletableFuture<NullableValue<V>> doGetNullableAndReplace(@Nullable Transaction tx, K key, V val) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w, n) -> writeKeyValue(s, w, n, tx, key, val),
                (s, r) -> NullableValue.of(valSer.readRec(s, r.in(), TuplePart.VAL, TuplePart.KEY_AND_VAL)),
                null,
                getPartitionAwarenessProvider(keySer.mapper(), key),
                tx);
    }

    private void writeKeyValue(
            ClientSchema s,
            PayloadOutputChannel w,
            WriteContext ctx,
            @Nullable Transaction tx,
            K key,
            @Nullable V val
    ) {
        writeSchemaAndTx(s, w, ctx, tx);
        writeKeyValueRaw(s, w, key, val);
    }

    private void writeKeyValueRaw(ClientSchema s, PayloadOutputChannel w, K key, @Nullable V val) {
        var builder = new BinaryTupleBuilder(s.columns().length);
        var noValueSet = new BitSet();
        ClientMarshallerWriter writer = new ClientMarshallerWriter(builder, noValueSet);

        Marshaller keyMarsh = s.getMarshaller(keySer.mapper(), TuplePart.KEY, false);
        Marshaller valMarsh = s.getMarshaller(valSer.mapper(), TuplePart.VAL, false);

        for (var column : s.columns()) {
            if (column.key()) {
                keyMarsh.writeField(key, writer, column.keyIndex());
            } else {
                valMarsh.writeField(val, writer, column.valIndex());
            }
        }

        w.out().packBinaryTuple(builder, noValueSet);
    }

    private void writeSchemaAndTx(ClientSchema s, PayloadOutputChannel w, WriteContext ctx, @Nullable Transaction tx) {
        w.out().packInt(tbl.tableId());
        writeTx(tx, w, ctx);
        w.out().packInt(s.version());
    }

    private HashMap<K, V> readGetAllResponse(ClientSchema schema, PayloadInputChannel in) {
        var cnt = in.in().unpackInt();

        var res = new LinkedHashMap<K, V>(cnt);

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY, false);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL, false);

        for (int i = 0; i < cnt; i++) {
            // TODO: Optimize (IGNITE-16022).
            if (in.in().unpackBoolean()) {
                var tupleReader = new BinaryTupleReader(schema.columns().length, in.in().readBinaryUnsafe());
                var keyReader = new ClientMarshallerReader(tupleReader, schema.keyColumns(), TuplePart.KEY_AND_VAL);
                var valReader = new ClientMarshallerReader(tupleReader, schema.valColumns(), TuplePart.KEY_AND_VAL);
                res.put((K) keyMarsh.readObject(keyReader, null), (V) valMarsh.readObject(valReader, null));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<Entry<K, V>>> publisher, @Nullable DataStreamerOptions options) {
        Objects.requireNonNull(publisher, "publisher");

        var provider = new KeyValuePojoStreamerPartitionAwarenessProvider<K, V>(tbl, keySer.mapper());
        var opts = options == null ? DataStreamerOptions.DEFAULT : options;

        // Partition-aware (best effort) sender with retries.
        // The batch may go to a different node when a direct connection is not available.
        StreamerBatchSender<Entry<K, V>, Integer, Void> batchSender = (partition, items, deleted) -> tbl.doSchemaOutOpAsync(
                ClientOp.STREAMER_BATCH_SEND,
                (s, w, n) -> {
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

                        keyMarsh.writeObject(e.getKey(), writer);

                        if (!del) {
                            valMarsh.writeObject(e.getValue(), writer);
                        }

                        w.out().packBinaryTuple(builder, noValueSet);
                    }
                },
                r -> null,
                PartitionAwarenessProvider.of(partition),
                new RetryLimitPolicy().retryLimit(opts.retryLimit()),
                null);

        return ClientDataStreamer.streamData(publisher, opts, batchSender, provider, tbl);
    }

    @Override
    public <E, P, A, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<P, A, R> receiver,
            Function<E, Entry<K, V>> keyFunc,
            Function<E, P> payloadFunc,
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
                new KeyValuePojoStreamerPartitionAwarenessProvider<>(tbl, keySer.mapper()),
                tbl,
                resultSubscriber,
                receiver,
                receiverArg
        );
    }

    /** {@inheritDoc} */
    @Override
    protected Function<SqlRow, Entry<K, V>> queryMapper(ResultSetMetadata meta, ClientSchema schema) {
        String[] keyCols = columnNames(schema.keyColumns());
        String[] valCols = columnNames(schema.valColumns());

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY, true);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL, true);

        return (row) -> new IgniteBiTuple<>(
                (K) keyMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, keyCols)), null),
                (V) valMarsh.readObject(new TupleReader(new SqlRowProjection(row, meta, valCols)), null)
        );
    }

    private static <T> T throwIfNull(T obj, String altMethod) {
        if (obj == null) {
            throw new UnexpectedNullValueException(format("Got unexpected null value: use `{}` sibling method instead.", altMethod));
        }

        return obj;
    }
}
