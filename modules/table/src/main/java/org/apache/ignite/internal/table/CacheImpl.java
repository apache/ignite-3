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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.Cache;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.TypeConverter;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class CacheImpl<K, V> extends KeyValueBinaryViewImpl implements Cache<K, V> {
    private static final byte[] TOMBSTONE = new byte[0];

    public static final String KEY_COL = "KEY";
    public static final String VAL_COL = "VALUE";
    public static final String TTL_COL = "TTL";

    private final TypeConverter<K, byte[]> keySerializer;
    private final TypeConverter<V, byte[]> valueSerializer;

    private final CacheLoader<K, V> loader;
    private final CacheWriter<K, V> writer;

    private final IgniteTransactions transactions;
    private final int schemaVersion;

    public CacheImpl(
            InternalTable tbl,
            SchemaVersions schemaVersions,
            SchemaRegistry schemaReg,
            IgniteTransactions transactions,
            @Nullable CacheLoader<K, V> loader,
            @Nullable CacheWriter<K, V> writer,
            @Nullable TypeConverter<K, byte[]> keyConverter,
            @Nullable TypeConverter<V, byte[]> valueConverter
    ) {
        super(tbl, schemaReg, schemaVersions);

        // Schema is immutable for cache.
        this.schemaVersion = rowConverter.registry().lastKnownSchemaVersion();

        this.transactions = transactions;
        this.loader = loader;
        this.writer = writer;
        this.keySerializer = keyConverter == null ? new SerializingConverter<>() : keyConverter;
        this.valueSerializer = valueConverter == null ? new SerializingConverter<>() : valueConverter;
    }

    @Override
    public @Nullable V get(K key) {
        InternalTransaction tx = (InternalTransaction) transactions.begin();

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            Row keyRow = marshal(keyTup, null, schemaVersion);

            Tuple valTup = tbl.getForCache(keyRow, tx).thenApply(row -> unmarshalValue(row, schemaVersion)).join();

            if (valTup == null) {
                if (loader != null) {
                    V val = loader.load(key);
                    if (val == null) {
                        valTup = Tuple.create().set(VAL_COL, TOMBSTONE).set(TTL_COL, 0L); // TODO ttl from policy
                    } else {
                        valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(val)).set(TTL_COL, 0L);
                    }

                    Row row = marshal(keyTup, valTup, schemaVersion);
                    tbl.putForCache(row, tx).join();
                }

                return null;
            }

            // TODO use readResolve for tombstone.
            byte[] val = valTup.value(VAL_COL);

            assert val != null;

            // Tombstone.
            if (val.length == 0) {
                return null;
            }

            return (V) keySerializer.toObjectType(val);
        } catch (TransactionException e) {
            tx.safeCleanup(false);

            throw e;
        } catch (Exception e) {
            tx.safeCleanup(false);

            // TODO mapper error has no codes. Need to refactor converter exceptions.
            throw new TransactionException(e);
        } finally {
            tx.safeCleanup(true);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {

    }

    @Override
    public void put(K key, V value) {
        InternalTransaction tx = (InternalTransaction) transactions.begin();

        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));
            Tuple valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(value)).set(TTL_COL, 0L);

            Row row = marshal(keyTup, valTup, schemaVersion);
            tbl.putForCache(row, tx).join();

            if (writer != null) {
                writer.write(new CacheEntryImpl<>(key, value));
            }

            tx.safeCleanup(true);
        } catch (TransactionException e) {
            tx.safeCleanup(false); // Async cleanup.

            throw e;
        } catch (Exception e) {
            tx.safeCleanup(false); // Async cleanup.

            throw new TransactionException(e);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return false;
    }

    @Override
    public boolean remove(K key) {
        InternalTransaction tx = (InternalTransaction) transactions.begin();

        Objects.requireNonNull(key);

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            Row row = marshal(keyTup, null, schemaVersion);

            boolean removed = tbl.removeForCache(row, tx).join();

            // Always call external delete.
            if (writer != null) {
                writer.delete(key);
            }

            tx.safeCleanup(true); // Safe to call in final block.

            return removed;
        } catch (TransactionException e) {
            tx.safeCleanup(false); // Async cleanup.

            throw e;
        } catch (Exception e) {
            tx.safeCleanup(false); // Async cleanup.

            throw new TransactionException(e);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        return false;
    }

    @Override
    public V getAndRemove(K key) {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public boolean replace(K key, V value) {
        return false;
    }

    @Override
    public V getAndReplace(K key, V value) {
        return null;
    }

    @Override
    public void removeAll(Set<? extends K> keys) {

    }

    @Override
    public void removeAll() {

    }

    @Override
    public void clear() {

    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return null;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        return null;
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public CacheManager getCacheManager() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return null;
    }

    private static class SerializingConverter<T> implements TypeConverter<T, byte[]> {
        /** {@inheritDoc} */
        @Override
        public byte[] toColumnType(T obj) throws Exception {
            ByteArrayOutputStream out = new ByteArrayOutputStream(512);

            try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
                oos.writeObject(obj);
            }

            return out.toByteArray();
        }

        /** {@inheritDoc} */
        @Override
        public T toObjectType(byte[] data) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                return (T) ois.readObject();
            }
        }
    }
}

