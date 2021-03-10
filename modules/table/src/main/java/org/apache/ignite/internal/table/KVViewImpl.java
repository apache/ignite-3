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

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KVView;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Key-value view implementation provides functionality to access table
 * transparently map user defined classes to binary row and vice versa.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class KVViewImpl<K, V> implements KVView<K, V> {
    /** Table. */
    private final TableStorage table;


    /**
     * Constructor.
     *
     * @param table Table.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KVViewImpl(TableStorage table, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        final Marshaller marsh = marshaller();

        TableRow kRow = marsh.toKeyRow(key);

        TableRow row = table.get(kRow);

        return marsh.unmarshallValue(row);
    }

    /** {@inheritDoc} */
    @Override public List<V> getAll(List<K> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {

    }

    /** {@inheritDoc} */
    @Override public void putAll(SortedMap<K, V> pairs) {

    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<K> keys) {

    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R> R invoke(K key, InvokeProcessor<MutableEntry<K, V>, R> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <R> List<R> invokeAll(List<K> keys, InvokeProcessor<MutableEntry<K, V>, R> proc) {
        return Collections.emptyList();
    }

    /**
     * @return Marshaller.
     */
    private Marshaller marshaller() {
        return null;        // table.schemaManager().marshaller();
    }
}
