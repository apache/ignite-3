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
import java.util.function.Supplier;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.IgniteCache;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.client.tx.ClientTransactions;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.TypeConverter;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;

/**
 * Client cache implementation.
 * TODO reduce copy&paste comparing to EmbeddedCache.
 */
public class ClientCache<K, V> implements IgniteCache<K, V> {
    private static final byte[] TOMBSTONE = new byte[0];
    private static final long TOMBSTONE_TTL = 5 * 60 * 1000L;
    private static final String KEY_COL = "KEY";
    private static final String VAL_COL = "VALUE";
    private static final String TTL_COL = "TTL";

    /** Underlying table. */
    private final ClientTable tbl;

    /** Tuple serializer. */
    private final ClientTupleSerializer ser;

    /** Loader. */
    private final CacheLoader<K, V> loader;

    /** Writer. */
    private final CacheWriter<K, V> writer;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPolicy;

    /** Key serializer. */
    private final TypeConverter<K, byte[]> keySerializer;

    /** Value serializer. */
    private final TypeConverter<V, byte[]> valueSerializer;

    /** Transactions factory. */
    private final ClientTransactions transactions;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    ClientCache(
            ClientTable tbl,
            CacheLoader<K, V> loader,
            CacheWriter<K, V> writer,
            TypeConverter<K, byte[]> keySerializer,
            TypeConverter<V, byte[]> valueSerializer,
            ExpiryPolicy expiryPolicy
    ) {
        assert tbl != null;

        this.tbl = tbl;
        this.loader = loader;
        this.writer = writer;
        this.expiryPolicy = expiryPolicy;
        this.keySerializer = keySerializer == null ? new SerializingConverter<>() : keySerializer;
        this.valueSerializer = valueSerializer == null ? new SerializingConverter<>() : valueSerializer;
        ser = new ClientTupleSerializer(tbl.tableId());
        transactions = new ClientTransactions(tbl.channel());
    }

    private @Nullable V get(Transaction tx, K key) {
        ClientTransaction tx0 = (ClientTransaction) tx;

        if (writer != null) {
            Object val0 = tx0.dirtyCache().get(key);

            if (val0 != null) {
                return val0 == TOMBSTONE ? null : (V) val0;
            }
        }

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            Tuple valTup = tbl.doSchemaOutInOpAsync(
                    ClientOp.TUPLE_GET,
                    (s, w) -> ser.writeTuple(tx, keyTup, s, w, true),
                    (s, r) -> ClientTupleSerializer.readTuple(s, r.in(), false),
                    null,
                    ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyTup)).join();

            if (valTup == null) {
                V val = null;

                if (loader != null) {
                    val = loader.load(key);
                    if (val == null) {
                        valTup = Tuple.create().set(VAL_COL, TOMBSTONE).set(TTL_COL, TOMBSTONE_TTL);
                    } else {
                        // TODO set ttl defined by user
                        valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(val)).set(TTL_COL, getExpiration());
                    }

                    Tuple finalValTup = valTup;
                    tbl.doSchemaOutOpAsync(
                            ClientOp.TUPLE_UPSERT,
                            (s, w) -> ser.writeKvTuple(tx, keyTup, finalValTup, s, w, false),
                            r -> null,
                            ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyTup)).join();
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

    private long getExpiration() {
        if (expiryPolicy != null && expiryPolicy.getExpiryForCreation() != null) {
            Duration dur = expiryPolicy.getExpiryForCreation();
            // TODO use caching clocks.
            return System.currentTimeMillis() + dur.getTimeUnit().toMillis(dur.getDurationAmount());
        }

        return 0;
    }

    private void put(Transaction tx, K key, V value) {
        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));
            Tuple valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(value)).set(TTL_COL, getExpiration());

            ClientTransaction tx0 = (ClientTransaction) tx;

            tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_UPSERT,
                    (s, w) -> ser.writeKvTuple(tx, keyTup, valTup, s, w, false),
                    r -> null,
                    ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyTup)).join();

            if (writer != null) {
                tx0.dirtyCache().put(key, value); // Safe for external commit.
            }
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    private boolean remove(Transaction tx, K key) {
        ClientTransaction tx0 = (ClientTransaction) tx;

        if (writer != null) {
            Object val = tx0.dirtyCache().get(key);

            if (val == TOMBSTONE) {
                return false; // Already removed.
            }
        }

        try {
            Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

            boolean removed = tbl.doSchemaOutOpAsync(
                    ClientOp.TUPLE_DELETE,
                    (s, w) -> ser.writeTuple(tx, keyTup, s, w, true),
                    r -> r.in().unpackBoolean(),
                    ClientTupleSerializer.getPartitionAwarenessProvider(tx, keyTup)).join();

            // Always call external delete.
            if (writer != null) {
                tx0.dirtyCache().put(key, TOMBSTONE);
            }

            return removed;
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    private Transaction beginTransaction() {
        return transactions.beginForCache(new TransactionOptions(), (writer == null) ? null : txn -> {
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
                    ClientCacheEntry<Object, Object> cacheEntry = new ClientCacheEntry<>(entry.getKey(), entry.getValue());
                    writer.write((Entry<? extends K, ? extends V>) cacheEntry);
                }

                futs.add(CompletableFutures.nullCompletedFuture());
            }

            return CompletableFuture.allOf(futs.toArray(new CompletableFuture[0]));
        });
    }

    @Override
    public V get(K key) {
        Transaction tx = beginTransaction();

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
        Transaction tx = beginTransaction();

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
        Transaction tx = beginTransaction();

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
    public void runAtomically(Runnable clo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> R runAtomically(Supplier<R> clo) {
        throw new UnsupportedOperationException();
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
