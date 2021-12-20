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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
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
    public V get(@NotNull K key) {
        return getAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAsync(@NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> keySer.writeRec(key, schema, out, TuplePart.KEY),
                (inSchema, in) -> valSer.readRec(inSchema, in, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@NotNull Collection<K> keys) {
        return getAllAsync(keys).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys) {
        Objects.requireNonNull(keys);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (schema, out) -> keySer.writeRecs(keys, schema, out, TuplePart.KEY),
                (schema, in) -> readGetAllResponse(keys, schema, in),
                Collections.emptyMap());
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@NotNull K key) {
        // TODO
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@NotNull K key) {
        // TODO
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void put(@NotNull K key, V val) {
        putAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@NotNull Map<K, V> pairs) {
        putAllAsync(pairs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs) {
        Objects.requireNonNull(pairs);

        if (pairs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> {
                    w.packIgniteUuid(tbl.tableId());
                    w.packInt(s.version());
                    w.packInt(pairs.size());

                    for (Entry<K, V> e : pairs.entrySet()) {
                        keySer.writeRecRaw(e.getKey(), s, w, TuplePart.KEY);
                        valSer.writeRecRaw(e.getValue(), s, w, TuplePart.VAL);
                    }
                },
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@NotNull K key, V val) {
        return getAndPutAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@NotNull K key, @NotNull V val) {
        return putIfAbsentAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@NotNull K key) {
        return removeAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@NotNull K key, V val) {
        return removeAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> keySer.writeRec(key, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@NotNull Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@NotNull K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull K key, V val) {
        return replaceAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull K key, V oldVal, V newVal) {
        return replaceAsync(key, oldVal, newVal).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(oldVal, s, w, TuplePart.VAL);

                    keySer.writeRecRaw(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(newVal, s, w, TuplePart.VAL);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@NotNull K key, V val) {
        return getAndReplaceAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(val, s, w, TuplePart.VAL);
                },
                (inSchema, in) -> valSer.readRec(inSchema, in, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(@NotNull K key, InvokeProcessor<K, V, R> proc,
            Serializable... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> Map<K, R> invokeAll(@NotNull Collection<K> keys, InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(@NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Transaction transaction() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public KeyValueView<K, V> withTransaction(Transaction tx) {
        return null;
    }

    private HashMap<K, V> readGetAllResponse(@NotNull Collection<K> keys, ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        assert cnt == keys.size();

        var res = new LinkedHashMap<K, V>(cnt);

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL);

        var reader = new ClientMarshallerReader(in);

        try {
            for (K key : keys) {
                if (!in.unpackBoolean()) {
                    res.put(key, null);
                } else {
                    res.put((K) keyMarsh.readObject(reader, null), (V) valMarsh.readObject(reader, null));
                }
            }
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }

        return res;
    }
}
