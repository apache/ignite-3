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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
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

    @Override
    public V get(@NotNull K key) {
        return getAsync(key).join();
    }

    @Override
    public @NotNull CompletableFuture<V> getAsync(@NotNull K key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> keySer.writeRec(key, schema, out, TuplePart.KEY),
                (inSchema, in) -> valSer.readRec(inSchema, in, TuplePart.VAL));
    }

    @Override
    public Map<K, V> getAll(@NotNull Collection<K> keys) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys) {
        return null;
    }

    @Override
    public boolean contains(@NotNull K key) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@NotNull K key) {
        return null;
    }

    @Override
    public void put(@NotNull K key, V val) {
        putAsync(key, val).join();
    }

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

    @Override
    public void putAll(@NotNull Map<K, V> pairs) {

    }

    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs) {
        return null;
    }

    @Override
    public V getAndPut(@NotNull K key, V val) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val) {
        return null;
    }

    @Override
    public boolean putIfAbsent(@NotNull K key, @NotNull V val) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val) {
        return null;
    }

    @Override
    public boolean remove(@NotNull K key) {
        return false;
    }

    @Override
    public boolean remove(@NotNull K key, V val) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, V val) {
        return null;
    }

    @Override
    public Collection<K> removeAll(@NotNull Collection<K> keys) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys) {
        return null;
    }

    @Override
    public V getAndRemove(@NotNull K key) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key) {
        return null;
    }

    @Override
    public boolean replace(@NotNull K key, V val) {
        return false;
    }

    @Override
    public boolean replace(@NotNull K key, V oldVal, V newVal) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal) {
        return null;
    }

    @Override
    public V getAndReplace(@NotNull K key, V val) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val) {
        return null;
    }

    @Override
    public <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(@NotNull K key, InvokeProcessor<K, V, R> proc,
            Serializable... args) {
        return null;
    }

    @Override
    public <R extends Serializable> Map<K, R> invokeAll(@NotNull Collection<K> keys, InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(@NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    @Override
    public @Nullable Transaction transaction() {
        return null;
    }

    @Override
    public KeyValueView<K, V> withTransaction(Transaction tx) {
        return null;
    }
}
