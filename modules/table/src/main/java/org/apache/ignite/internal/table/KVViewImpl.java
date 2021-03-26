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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation.
 */
public class KVViewImpl<K, V> implements KeyValueView<K, V> {
    /** Underlying storage. */
    private final InternalTable tbl;

    /** Schema manager. */
    private final TableSchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaMgr Schema manager.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KVViewImpl(InternalTable tbl, TableSchemaManager schemaMgr, KeyMapper<K> keyMapper,
        ValueMapper<V> valueMapper) {
        this.tbl = tbl;
        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        Objects.requireNonNull(key);

        final Marshaller marsh = marshaller();

        try {
            Row kRow = marsh.serialize(key); // Convert to portable format to pass TX/storage layer.

            final CompletableFuture<V> fut = tbl.get(kRow)  // Load async.
                .thenApply(this::wrap) // Binary -> schema-aware row
                .thenApply(marsh::deserializeValue); // Deserialize.

            return fut.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<V> getAsync(K key) {
//        Objects.requireNonNull(key);
//
//        final Marshaller marsh = marshaller();
//
//        Row kRow = marsh.serialize(key); // Convert to portable format to pass TX/storage layer.
//
//        final CompletableFuture<BinaryRow> fut = tbl.get(kRow);
//
        //Binary -> schema-aware row -> deserialize.
//        return fut.thenApply(r -> wrap(r))
//            .thenApply(r -> marsh.deserializeValue(r));
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Map<K, V>> getAllAsync(Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(K key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<K, V> pairs) {

    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Void> putAllAsync(Map<K, V> pairs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<V> getAndPutAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> removeAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<K> removeAll(Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<K> removeAllAsync(Collection<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<V> getAndRemoveAsync(K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<R> invokeAsync(
        K key,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<K, R> invokeAll(
        Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> IgniteFuture<Map<K, R>> invokeAllAsync(
        Collection<K> keys,
        InvokeProcessor<K, V, R> proc, Serializable... args
    ) {
        return null;
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;        // table.schemaManager().marshaller();
    }

    /**
     * @param row Binary row.
     * @return Schema-aware row.
     */
    private Row wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor rowSchema = schemaMgr.schema(row.schemaVersion()); // Get a schema for row.

        return new Row(rowSchema, row);
    }

    /**
     * @param e Exception.
     * @return Runtime exception.
     */
    private RuntimeException convertException(Exception e) {
        if (e instanceof InterruptedException)
            Thread.currentThread().interrupt(); // Restore interrupt flag.

        return new RuntimeException(e);
    }
}
