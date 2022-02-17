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

package org.apache.ignite.lang;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This map view is constructed from a map and a single change applied to it, like put, remove or clear.
 * It is not thread-safe.
 */
public class PatchedMapView<K, V> implements Map<K, V> {
    private static final int DEFAULT_MAX_DEPTH = 5;

    private final Map<K, V> internalMap;
    private final boolean isCleared;
    private final K removed;
    private final IgniteBiTuple<K, V> added;
    private final IgniteBiTuple<K, V> replaced;

    private PatchedMapView(Map<K, V> map, boolean isCleared, K removed, IgniteBiTuple<K, V> added, IgniteBiTuple<K, V> replaced) {
        assert isCleared || removed != null || added != null || replaced != null;
        internalMap = map;
        this.isCleared = isCleared;
        this.removed = removed;
        this.added = added;
        this.replaced = replaced;
    }

    @Override
    public int size() {
        if (added != null) {
            return internalMap.size() + 1;
        } else if (replaced != null) {
            return internalMap.size();
        } else if (removed != null) {
            return internalMap.size() - 1;
        } else if (isCleared) {
            return 0;
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (added != null) {
            return internalMap.containsKey(key) && added.containsKey(key);
        } else if (replaced != null) {
            return internalMap.containsKey(key);
        } else if (removed != null) {
            return internalMap.containsKey(key) && !removed.equals(key);
        } else if (isCleared) {
            return false;
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        return values().stream().anyMatch(v -> Objects.equals(v, value));
    }

    @Override
    public V get(Object key) {
        if (added != null) {
            return (added.containsKey(key)) ? added.getValue() : internalMap.get(key);
        } else if (replaced != null) {
            return (replaced.containsKey(key)) ? replaced.getValue() : internalMap.get(key);
        } else if (removed != null) {
            return (removed.equals(key)) ? null : internalMap.get(key);
        } else if (isCleared) {
            return null;
        } else {
            throw new AssertionError();
        }
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a PatchedMapView builder from the map.
     *
     * @param map Map.
     * @return PatchedMapView builder.
     */
    public static <K, V> PatchedMapViewBuilder<K, V> of(Map<K, V> map) {
        return new PatchedMapViewBuilder<>(map);
    }

    /**
     * Creates a PatchedMapView builder from the map.
     *
     * @param map Map.
     * @param maxDepth Maximum count of patches in PatchedMapView.
     * @return PatchedMapView builder.
     */
    public static <K, V> PatchedMapViewBuilder<K, V> of(Map<K, V> map, int maxDepth) {
        return new PatchedMapViewBuilder<>(map, maxDepth);
    }

    private int depth() {
        return internalMap instanceof PatchedMapView ? ((PatchedMapView<K, V>) internalMap).depth() + 1 : 1;
    }

    private Map<K, V> squash() {
        Map<K, V> res = internalMap instanceof PatchedMapView ? ((PatchedMapView<K, V>) internalMap).squash() : new HashMap<>(internalMap);

        if (added != null) {
            res.put(added.getKey(), added.getValue());
        } else if (replaced != null) {
            res.put(replaced.getKey(), replaced.getValue());
        } else if (removed != null) {
            res.remove(removed);
        } else if (isCleared) {
            res.clear();
        }

        return res;
    }

    /**
     * Builder for {@link PatchedMapView}.
     */
    public static class PatchedMapViewBuilder<K, V> {
        private final Map<K, V> map;

        private PatchedMapViewBuilder(Map<K, V> map) {
            this(map, DEFAULT_MAX_DEPTH);
        }

        private PatchedMapViewBuilder(Map<K, V> map, int maxDepth) {
            if (map instanceof PatchedMapView && ((PatchedMapView<K, V>) map).depth() >= maxDepth) {
                this.map = ((PatchedMapView<K, V>)map).squash();
            } else {
                this.map = map;
            }
        }

        /**
         * Create a map view with clearing update.
         *
         * @return Map view.
         */
        public Map<K, V> clear() {
            return new PatchedMapView<>(map, true, null, null, null);
        }

        /**
         * Create a map view with one key-value pair added to it or replaced in it.
         *
         * @return Map view.
         */
        public Map<K, V> put(K k, V v) {
            boolean contains = map.containsKey(k);
            return new PatchedMapView<>(
                map,
                false,
                null,
                contains ? null : new IgniteBiTuple<>(k, v),
                contains ? new IgniteBiTuple<>(k, v) : null
            );
        }

        /**
         * Create a map view with put update, or returns the original map if the key is present and not mapped to {@code null}.
         *
         * @return Map view.
         */
        public Map<K, V> putIfAbsent(K k, V v) {
            return map.get(k) == null ? new PatchedMapView<>(map, false, null, new IgniteBiTuple<>(k, v), null) : map;
        }

        /**
         * Create a map view with put update using mapping function, or returns the original map if the key is present and not
         * mapped to {@code null}.
         *
         * @return Map view.
         */
        public Map<K, V> computeIfAbsent(K k, Function<? super K, ? extends V> mappingFunction) {
            Objects.requireNonNull(mappingFunction);
            if ((map.get(k)) == null) {
                V v;
                if ((v = mappingFunction.apply(k)) != null)
                    return new PatchedMapView<>(map, false, null, new IgniteBiTuple<>(k, v), null);
            }

            return map;
        }

        /**
         * Create a map view with one key-value pair removed from it.
         *
         * @return Map view.
         */
        public Map<K, V> remove(K k) {
            if (map.containsKey(k)) {
                return new PatchedMapView<>(map, true, k, null, null);
            } else {
                return map;
            }
        }

        /**
         * Returns original map, or squashed map view, if the original map is {@link PatchedMapView}. Squashed map view is
         * {@link PatchedMapView} where the original map is not a {@link PatchedMapView}.
         *
         * @return Map view.
         */
        public Map<K, V> map() {
            return map instanceof PatchedMapView ? ((PatchedMapView<K, V>) map).squash() : map;
        }
    }
}
