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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.KVSerializer;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class KeyValueViewImpl<K, V> extends AbstractTableView implements KeyValueView<K, V> {
    /** Key object mapper. */
    private Mapper<K> keyMapper;

    /** Value object mapper. */
    private Mapper<V> valueMapper;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     * @param tx The transaction.
     */
    public KeyValueViewImpl(InternalTable tbl, SchemaRegistry schemaReg, Mapper<K> keyMapper,
                            Mapper<V> valueMapper, @Nullable Transaction tx) {
        super(tbl, schemaReg, tx);

        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    /** {@inheritDoc} */
    @Override public V get(@NotNull K key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.get(kRow, tx)
            .thenApply(this::unmarshalValue); // row -> deserialized obj.
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@NotNull Collection<K> keys) {
        return sync(getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull K key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> containsAsync(@NotNull K key) {
        return getAsync(key).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull K key, V val) {
        sync(putAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.upsert(kRow, tx).thenAccept(ignore -> {
        });
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<K, V> pairs) {
        sync(putAllAsync(pairs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(@NotNull K key, V val) {
        return sync(getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.getAndUpsert(kRow, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull K key, V val) {
        return sync(putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.insert(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key) {
        return sync(removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.delete(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key, @NotNull V val) {
        return sync(removeAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, @NotNull V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.deleteExact(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public Collection<K> removeAll(@NotNull Collection<K> keys) {
        return sync(removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(@NotNull K key) {
        return sync(getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.getAndDelete(kRow, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V val) {
        return sync(replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val) {
        BinaryRow row = marshal(key, val);

        return tbl.replace(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V oldVal, V newVal) {
        return sync(replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal) {
        BinaryRow oldRow = marshal(key, oldVal);
        BinaryRow newRow = marshal(key, newVal);

        return tbl.replace(oldRow, newRow, tx);
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(@NotNull K key, V val) {
        return sync(getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val) {
        BinaryRow row = marshal(key, val);

        return tbl.getAndReplace(row, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
        @NotNull K key,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<K, R> invokeAll(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc, Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public KeyValueViewImpl<K, V> withTransaction(Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * @param schemaVersion Schema version.
     * @return Marshaller.
     */
    private KVSerializer<K, V> marshaller(int schemaVersion) {
        SerializerFactory factory = SerializerFactory.createJavaSerializerFactory();

        // TODO: Cache marshaller for schema or upgrade row?
        return new KVSerializer<K, V>() {
            Serializer s = factory.create(schemaReg.schema(schemaVersion), keyMapper.getType(), valueMapper.getType());

            @Override public BinaryRow serialize(@NotNull K key, V val) {
                try {
                    return new ByteBufferRow(ByteBuffer.wrap(s.serialize(key, val)).order(ByteOrder.LITTLE_ENDIAN));
                } catch (SerializationException e) {
                    throw new IgniteException(e);
                }
            }

            @NotNull @Override public K deserializeKey(@NotNull BinaryRow row) {
                try {
                    return s.deserializeKey(row.bytes());
                } catch (SerializationException e) {
                    throw new IgniteException(e);
                }
            }

            @Nullable @Override public V deserializeValue(@NotNull BinaryRow row) {
                try {
                    return s.deserializeValue(row.bytes());
                } catch (SerializationException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    private V unmarshalValue(BinaryRow v) {
        return v == null ? null : marshaller(v.schemaVersion()).deserializeValue(v);
    }

    private BinaryRow marshal(@NotNull K key, V o) {
        final KVSerializer<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        return marsh.serialize(key, o);
    }
}
