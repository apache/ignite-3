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

package org.apache.ignite.internal.configuration.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simplified map interfaces with better control over the keys order.
 *
 * @param <V> Type of the value.
 */
public class OrderedMap<V> {
    /** Underlying linked hash map. */
    private final Map<String, V> map = new LinkedHashMap<>();

    /** Default constructor. */
    public OrderedMap() {
    }

    /**
     * Copy constructor.
     *
     * @param other Source of keys/values to copy from.
     */
    public OrderedMap(OrderedMap<V> other) {
        map.putAll(other.map);
    }

    /**
     * Same as {@link Map#containsKey(Object)}.
     *
     * @param key Key to check.
     * @return {@code true} if map contains the key.
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Same as {@link Map#get(Object)}.
     *
     * @param key Key to search.
     * @return Value assiciated with the key or {@code null} is it's not found.
     */
    public V get(String key) {
        return map.get(key);
    }

    /**
     * Same as {@link Map#remove(Object)}.
     *
     * @param key Key to remove.
     * @return Provious value assiciated with the key or {@code null} if map had no such key.
     */
    public V remove(String key) {
        return map.remove(key);
    }

    /**
     * Put value to the map. Key will be the last if it didn't exist in map before. If key did exist then the same
     * ordering index will be used.
     *
     * @param key Key to put.
     * @param value Value associated with the key.
     */
    public void put(String key, V value) {
        map.put(key, value);
    }

    /**
     * Put value to the map.
     *
     * @param idx Ordering index for the key. Treated as {@code 0} if negative. Treated as last index if out of bounds.
     * @param key Key to put.
     * @param value Value associated with the key.
     *
     * @throws IllegalArgumentException If key already exists in the map.
     */
    public void putByIndex(int idx, String key, V value) {
        if (map.containsKey(key))
            throw new IllegalArgumentException("Key " + key + " already exists.");

        Map<String, V> copy = new LinkedHashMap<>();
        int curIdx = 0;

        for (Iterator<Map.Entry<String, V>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, V> entry = iterator.next();

            String nextKey = entry.getKey();

            if (++curIdx > idx) {
                V nextValue = entry.getValue();

                iterator.remove();

                copy.put(nextKey, nextValue);
            }
        }

        map.put(key, value);

        for (Map.Entry<String, V> entry : copy.entrySet())
            map.put(entry.getKey(), entry.getValue());
    }

    /**
     * Put value to the map.
     *
     * @param base Previous key for the new key. Last key will be used if this one is missing from the map.
     * @param key Key to put.
     * @param value Value associated with the key.
     *
     * @throws IllegalArgumentException If key already exists in the map.
     */
    public void putAfter(String base, String key, V value) {
        if (map.containsKey(key))
            throw new IllegalArgumentException("Key " + key + " already exists.");

        if (map.containsKey(base)) {
            Map<String, V> copy = new LinkedHashMap<>();
            boolean delete = false;

            for (Iterator<Map.Entry<String, V>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, V> entry = iterator.next();

                String nextKey = entry.getKey();


                if (delete) {
                    V nextValue = entry.getValue();

                    iterator.remove();

                    copy.put(nextKey, nextValue);
                }

                if (nextKey.equals(base))
                    delete = true;
            }

            map.put(key, value);

            for (Map.Entry<String, V> entry : copy.entrySet())
                map.put(entry.getKey(), entry.getValue());
        }
        else
            put(key, value);
    }

    /**
     * Put value associated with key {@code oldKey} updaer a new key {@code newKey}. Do nothing if {@code oldKey}
     * was missing from the map.
     *
     * @param oldKey Old key.
     * @param newKey New key.
     *
     * @throws IllegalArgumentException If both {@code oldKey} and {@code newKey} already exist in the map.
     */
    public void rename(String oldKey, String newKey) {
        if (!map.containsKey(oldKey))
            return;

        if (map.containsKey(newKey))
            throw new IllegalArgumentException();

        putAfter(oldKey, newKey, map.get(oldKey));

        map.remove(oldKey);
    }

    /**
     * @return List of keys.
     */
    public List<String> keys() {
        return new ArrayList<>(map.keySet());
    }

    /**
     * Reorders keys in the map.
     *
     * @param orderedKeys List of keys in new order. Must have the same set of keys in it.
     */
    public void reorderKeys(List<String> orderedKeys) {
        assert map.keySet().equals(new HashSet<>(orderedKeys)) : map.keySet() + " : " + orderedKeys;

        for (String key : orderedKeys) {
            V value = map.remove(key);

            map.put(key, value);
        }
    }

    /**
     * @return Size of the map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Clears the map.
     */
    public void clear() {
        map.clear();
    }
}
