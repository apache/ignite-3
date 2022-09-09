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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.ClientUtils.sync;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;
import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation.
 */
public class ClientKeyValueView<K, V> implements KeyValueView<K, V> {
    /** Underlying table. */
    private final ClientTable tbl;

    /** Key serializer.  */
    private final ClientRecordSerializer<K> keySer;

    /** Value serializer.  */
    private final ClientRecordSerializer<V> valSer;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param keyMapper Key mapper.
     * @param valMapper value mapper.
     */
    public ClientKeyValueView(ClientTable tbl, Mapper<K> keyMapper, Mapper<V> valMapper) {
        assert tbl != null;
        assert keyMapper != null;
        assert valMapper != null;

        this.tbl = tbl;

        keySer = new ClientRecordSerializer<>(tbl.tableId(), keyMapper);
        valSer = new ClientRecordSerializer<>(tbl.tableId(), valMapper);
    }

    /** {@inheritDoc} */
    @Override
    public V get(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, @NotNull K key) {
        return sync(getNullableAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, @NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public V getOrDefault(@Nullable Transaction tx, @NotNull K key, V defaultValue) {
        return sync(getOrDefaultAsync(tx, key, defaultValue));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, @NotNull K key, V defaultValue) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> keySer.writeRecs(tx, keys, s, w, TuplePart.KEY),
                this::readGetAllResponse,
                Collections.emptyMap());
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, @NotNull K key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, @NotNull K key, V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        Objects.requireNonNull(pairs);

        if (pairs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
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
                r -> null);
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getNullableAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                ClientMessageUnpacker::unpackBoolean);
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
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> keySer.writeRecs(tx, keys, s, w, TuplePart.KEY),
                (s, r) -> keySer.readRecs(s, r, false, TuplePart.KEY),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> keySer.writeRec(tx, key, s, w, TuplePart.KEY),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return sync(getNullableAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    writeSchemaAndTx(s, w, tx);
                    writeKeyValueRaw(s, w, key, oldVal);
                    writeKeyValueRaw(s, w, key, newVal);
                },
                ClientMessageUnpacker::unpackBoolean);
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeKeyValue(s, w, tx, key, val),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getNullableAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
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

    private void writeKeyValue(ClientSchema s, PayloadOutputChannel w, @Nullable Transaction tx, @NotNull K key, V val) {
        writeSchemaAndTx(s, w, tx);
        writeKeyValueRaw(s, w, key, val);
    }

    private void writeKeyValueRaw(ClientSchema s, PayloadOutputChannel w, @NotNull K key, V val) {
        var builder = BinaryTupleBuilder.create(s.columns().length, true);
        var noValueSet = new BitSet();
        ClientMarshallerWriter writer = new ClientMarshallerWriter(builder, noValueSet);

        try {
            s.getMarshaller(keySer.mapper(), TuplePart.KEY).writeObject(key, writer);
            s.getMarshaller(valSer.mapper(), TuplePart.VAL).writeObject(val, writer);
        } catch (MarshallerException e) {
            throw new IgniteException(UNKNOWN_ERR, e.getMessage(), e);
        }

        w.out().packBinaryTuple(builder, noValueSet);
    }

    private void writeSchemaAndTx(ClientSchema s, PayloadOutputChannel w, @Nullable Transaction tx) {
        w.out().packUuid(tbl.tableId());
        writeTx(tx, w);
        w.out().packInt(s.version());
    }

    private HashMap<K, V> readGetAllResponse(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();

        var res = new LinkedHashMap<K, V>(cnt);

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL);

        try {
            for (int i = 0; i < cnt; i++) {
                in.unpackBoolean(); // TODO: Optimize (IGNITE-16022).

                var tupleReader = new BinaryTupleReader(schema.columns().length, in.readBinaryUnsafe());
                var reader = new ClientMarshallerReader(tupleReader);
                res.put((K) keyMarsh.readObject(reader, null), (V) valMarsh.readObject(reader, null));
            }

            return res;
        } catch (MarshallerException e) {
            throw new IgniteException(UNKNOWN_ERR, e.getMessage(), e);
        }
    }
}
