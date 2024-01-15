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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.IgniteCache;
import org.apache.ignite.cache.CacheTransaction;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.TypeConverter;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class IgniteCacheImpl<K, V> extends AbstractTableView implements IgniteCache<K, V> {
    private static final byte[] TOMBSTONE = new byte[0];

    public static final String KEY_COL = "KEY";
    public static final String VAL_COL = "VALUE";
    public static final String TTL_COL = "TTL";

    private final TypeConverter<K, byte[]> keySerializer;
    private final TypeConverter<V, byte[]> valueSerializer;

    private final CacheLoader<K, V> loader;
    private final CacheWriter<K, V> writer;

    private final TxManager txManager;

    private final TupleMarshaller marshaller;

    public IgniteCacheImpl(
            InternalTable tbl,
            SchemaVersions schemaVersions,
            SchemaRegistry schemaReg,
            TxManager txManager,
            @Nullable CacheLoader<K, V> loader,
            @Nullable CacheWriter<K, V> writer,
            @Nullable TypeConverter<K, byte[]> keyConverter,
            @Nullable TypeConverter<V, byte[]> valueConverter
    ) {
        super(tbl, schemaVersions, schemaReg, null);

        this.txManager = txManager; // TODO get from internal table.
        this.loader = loader;
        this.writer = writer;
        this.keySerializer = keyConverter == null ? new SerializingConverter<>() : keyConverter;
        this.valueSerializer = valueConverter == null ? new SerializingConverter<>() : valueConverter;

        marshaller = new TupleMarshallerImpl(schemaReg.lastKnownSchema());
    }

    @Override
    public @Nullable V get(K key) {
        CacheTransaction tx = beginTransaction();

        try {
            return get(tx, key);
        } catch (TransactionException e) {
            try {
                tx.rollback();
            } catch (TransactionException ex) {
                e.addSuppressed(ex);
            }

            throw e;
        } finally {
            tx.commit();
        }
    }

    @Override
    public @Nullable V get(CacheTransaction tx, K key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        if (writer != null) {
            Object val0 = tx0.dirtyCache().get(key);

            if (val0 != null) {
                return val0 == TOMBSTONE ? null : (V) val0;
            }
        }

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            Row keyRow = marshaller.marshal(keyTup, null);

            Tuple valTup = tbl.getForCache(keyRow, tx0).thenApply(row -> unmarshalValue(row, marshaller.schemaVersion())).join();

            if (valTup == null) {
                V val = null;

                if (loader != null) {
                    val = loader.load(key);
                    if (val == null) {
                        // TODO need ttl for tombstones even if no ttl configured by user
                        valTup = Tuple.create().set(VAL_COL, TOMBSTONE).set(TTL_COL, 0L);
                    } else {
                        // TODO set ttl defined by user
                        valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(val)).set(TTL_COL, 0L);
                    }

                    Row row = marshaller.marshal(keyTup, valTup);
                    tbl.putForCache(row, tx0).join();
                }

                return val;
            }

            // TODO use readResolve for tombstone.
            byte[] val = valTup.value(VAL_COL);

            assert val != null;

            // Tombstone.
            if (val.length == 0) {
                return null;
            }

            return (V) keySerializer.toObjectType(val);
        } catch (Exception e) {
            throw new TransactionException(e);
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
        CacheTransaction tx = beginTransaction();

        try {
            put(tx, key, value);
        } catch (TransactionException e) {
            try {
                tx.rollback();
            } catch (TransactionException ex) {
                e.addSuppressed(ex);
            }

            throw e;
        } finally {
            tx.commit();
        }
    }

    @Override
    public void put(CacheTransaction tx, K key, V value) {
        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));
            Tuple valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(value)).set(TTL_COL, 0L);

            InternalTransaction tx0 = (InternalTransaction) tx;

            Row row = marshaller.marshal(keyTup, valTup);
            tbl.putForCache(row, tx0).join();

            if (writer != null) {
                tx0.dirtyCache().put(key, value); // Safe for external commit.
            }
        } catch (Exception e) {
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
        CacheTransaction tx = beginTransaction();

        try {
            return remove(tx, key);
        } catch (TransactionException e) {
            try {
                tx.rollback();
            } catch (TransactionException ex) {
                e.addSuppressed(ex);
            }

            throw e;
        } finally {
            tx.commit();
        }
    }

    @Override
    public boolean remove(CacheTransaction tx, K key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        if (writer != null) {
            Object val = tx0.dirtyCache().get(key);

            if (val == TOMBSTONE) {
                return false; // Already removed.
            }
        }

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            Row row = marshaller.marshalKey(keyTup);

            boolean removed = tbl.removeForCache(row, tx0).join();

            // Always call external delete.
            if (writer != null) {
                tx0.dirtyCache().put(key, TOMBSTONE);
            }

            return removed;
        } catch (Exception e) {
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

    @Override
    public CacheTransaction beginTransaction() {
        // TODO use correct observable tracker.
        return txManager.beginForCache(new HybridTimestampTracker(), (writer == null) ? null : txn -> {
            if (txn.dirtyCache().isEmpty()) {
                return CompletableFutures.nullCompletedFuture();
            }

            Set<Map.Entry<Object, Object>> entries = txn.dirtyCache().entrySet();

            List<CompletableFuture<Void>> futs = new ArrayList<>();

            // TODO make batch
            for (Map.Entry<Object, Object> entry : entries) {
                if (entry.getValue() == TOMBSTONE) {
                    writer.delete(entry.getKey());
                } else {
                    CacheEntryImpl<Object, Object> cacheEntry = new CacheEntryImpl<>(entry.getKey(), entry.getValue());
                    writer.write((Entry<? extends K, ? extends V>) cacheEntry);
                }

                futs.add(CompletableFutures.nullCompletedFuture());
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
        });
    }

    /**
     * Returns value tuple of given row.
     *
     * @param row Binary row.
     * @param schemaVersion The version to use when unmarshalling.
     * @return Value tuple.
     */
    protected @Nullable Tuple unmarshalValue(BinaryRow row, int schemaVersion) {
        if (row == null) {
            return null;
        }

        return TableRow.valueTuple(rowConverter.resolveRow(row, schemaVersion));
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

