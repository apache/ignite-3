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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded cache implementation.
 */
public class EmbeddedCache<K, V> extends AbstractCache<K, V, InternalTransaction> {
    private final TxManager txManager;
    private final TupleMarshaller marshaller;
    private final InternalTable tbl;
    private final TableViewRowConverter rowConverter;

    EmbeddedCache(
            InternalTable tbl,
            SchemaVersions schemaVersions,
            SchemaRegistry schemaReg,
            TxManager txManager,
            @Nullable CacheLoader<K, V> loader,
            @Nullable CacheWriter<K, V> writer,
            @Nullable TypeConverter<K, byte[]> keyConverter,
            @Nullable TypeConverter<V, byte[]> valueConverter,
            @Nullable ExpiryPolicy expiryPolicy
    ) {
        super(keyConverter, valueConverter, loader, writer, expiryPolicy);
        this.tbl = tbl;
        this.rowConverter = new TableViewRowConverter(schemaReg);
        this.txManager = txManager; // TODO get from internal table.
        this.marshaller = new TupleMarshallerImpl(schemaReg.lastKnownSchema());
    }

    @Override
    protected @Nullable V get(InternalTransaction tx, K key) throws Exception {
        assert tx.external() == (writer != null) : "Illegal tx state: " + tx;

        if (writer != null) {
            Object val0 = tx.dirtyCache().get(key);

            if (val0 != null) {
                return val0 == TOMBSTONE ? null : (V) val0;
            }
        }

        Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

        Row keyRow = marshaller.marshal(keyTup, null);

        Tuple valTup = tbl.get(keyRow, tx).thenApply(row -> unmarshalValue(row, marshaller.schemaVersion())).join();

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

                Row row = marshaller.marshal(keyTup, valTup);
                tbl.insert(row, tx).join();
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
    protected void put(InternalTransaction tx, K key, V value) throws Exception {
        Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));
        Tuple valTup = Tuple.create().set(VAL_COL, valueSerializer.toColumnType(value)).set(TTL_COL, getExpiration());

        assert tx.external() == (writer != null) : "Illegal tx state: " + tx;

        Row row = marshaller.marshal(keyTup, valTup);
        tbl.upsert(row, tx).join();

        if (writer != null) {
            tx.dirtyCache().put(key, value); // Safe for external commit.
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
    protected boolean remove(InternalTransaction tx, K key) throws Exception {
        assert tx.external() == (writer != null) : "Illegal tx state: " + tx;

        if (writer != null) {
            Object val = tx.dirtyCache().get(key);

            if (val == TOMBSTONE) {
                return false; // Already removed.
            }
        }

        Tuple keyTup = Tuple.create().set(KEY_COL, keySerializer.toColumnType(key));

        Row row = marshaller.marshalKey(keyTup);

        boolean removed = tbl.delete(row, tx).join();

        // Always call external delete.
        if (writer != null) {
            tx.dirtyCache().put(key, TOMBSTONE);
        }

        return removed;
    }

    @Override
    protected boolean isValidState(InternalTransaction tx) {
        return tx.state() != TxState.PENDING;
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
    protected InternalTransaction beginTransaction0() {
        return txManager.beginExternal(new HybridTimestampTracker(), (writer == null) ? null : txn -> {
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
                    CacheEntry<Object, Object> cacheEntry = new CacheEntry<>(entry.getKey(), entry.getValue());
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


}
