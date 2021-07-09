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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OrderedMap<V> {
    /** */
    private final Map<String, V> map = new LinkedHashMap<>();

    public OrderedMap() {
    }

    public OrderedMap(OrderedMap<V> other) {
        map.putAll(other.map);
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public V get(String key) {
        return map.get(key);
    }

    public V remove(String key) {
        return map.remove(key);
    }

    public void put(String key, V value) {
        map.put(key, value);
    }

    public void putByIndex(int idx, String key, V value) {
        map.remove(key);

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

    public void putAfter(String after, String key, V value) {
        map.remove(key);

        if (map.containsKey(after)) {
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

                if (nextKey.equals(after))
                    delete = true;
            }

            map.put(key, value);

            for (Map.Entry<String, V> entry : copy.entrySet())
                map.put(entry.getKey(), entry.getValue());
        }
        else
            put(key, value);
    }

    public void rename(String oldKey, String newKey) {
        if (!map.containsKey(oldKey))
            return;

        if (map.containsKey(newKey))
            throw new IllegalStateException("Fuck");

        putAfter(oldKey, newKey, map.get(oldKey));

        map.remove(oldKey);
    }

    public Set<String> keySet() {
        return new LinkedHashSet<>(map.keySet());
    }

    public int size() {
        return map.size();
    }

    public void clear() {
        map.clear();
    }

    public void reorderKeys(List<String> orderedKeys) {
        assert map.keySet().equals(new HashSet<>(orderedKeys)) : map.keySet() + " : " + orderedKeys;

        for (String key : orderedKeys) {
            V value = map.remove(key);

            map.put(key, value);
        }
    }
}
