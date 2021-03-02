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

package org.apache.ignite.internal.storage;

import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.storage.KVView;
import org.apache.ignite.storage.TableStorage;
import org.apache.ignite.storage.mapper.KeyMapper;
import org.apache.ignite.storage.mapper.ValueMapper;

public class KVViewImpl<K, V> implements KVView<K, V> {

    private final TableStorage table;
    private final KeyMapper<K> keyMapper;
    private final ValueMapper<V> valueMapper;
    Marshaller marsh;

    public KVViewImpl(TableStorage table, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        this.table = table;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override public V get(K key) {
//        marsh = table.schemaManager().marshaller();

        TableRow kRow = marsh.toKeyRow(key);

        TableRow row = table.get(kRow);

       return marsh.unmarshallValue(row);
    }

    @Override public boolean containsKey(K key) {
        return false;
    }

    @Override public void put(K key, V val) {

    }

    @Override public boolean putIfAbsent(K key, V val) {
        return false;
    }

    @Override public V getAndPut(K key, V val) {
        return null;
    }

    @Override public boolean remove(K key) {
        return false;
    }

    @Override public boolean remove(K key, V val) {
        return false;
    }

    @Override public V getAndRemove(K key) {
        return null;
    }

    @Override public boolean replace(K key, V val) {
        return false;
    }

    @Override public boolean getAndReplace(K key, V val) {
        return false;
    }
}
