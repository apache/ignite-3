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

package org.apache.ignite.internal.catalog;

import java.util.Arrays;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.Nullable;

/**
 * An immutable map-like structure that maps long keys to {@link Catalog} values.
 * <p>
 * The keys are stored in a sorted array to allow efficient binary search lookups.
 */
class CatalogByIndexMap {
    private final long[] keys;

    private final Catalog[] values;

    CatalogByIndexMap() {
        this(new long[0], new Catalog[0]);
    }

    private CatalogByIndexMap(long[] keys, Catalog[] values) {
        this.keys = keys;
        this.values = values;
    }

    /**
     * Returns the first (lowest) key in the map.
     */
    long firstKey() {
        checkNotEmpty();

        return keys[0];
    }

    /**
     * Returns the last (highest) key in the map.
     */
    long lastKey() {
        checkNotEmpty();

        return keys[keys.length - 1];
    }

    /**
     * Returns the first (earliest) value in the map.
     */
    Catalog firstValue() {
        checkNotEmpty();

        return values[0];
    }

    /**
     * Returns the last (latest) value in the map.
     */
    Catalog lastValue() {
        checkNotEmpty();

        return values[values.length - 1];
    }

    /**
     * Returns the value associated with the given key, or {@code null} if the key is not present.
     */
    @Nullable Catalog get(long key) {
        int idx = Arrays.binarySearch(keys, key);

        return idx >= 0 ? values[idx] : null;
    }

    /**
     * Returns the greatest value associated with a key less than or equal to the given key,
     */
    @Nullable Catalog floorValue(long key) {
        int idx = Arrays.binarySearch(keys, key);

        if (idx < 0) {
            // Get an index before the insertion point.
            idx = ~idx - 1;
        }

        return idx >= 0 ? values[idx] : null;
    }

    /**
     * Returns a new map with the given key and catalog added as the latest. If the key already exists, its value is updated.
     */
    CatalogByIndexMap appendOrUpdate(long key, Catalog catalog) {
        int length = this.keys.length;

        if (length == 0) {
            return new CatalogByIndexMap(new long[] {key}, new Catalog[] {catalog});
        } else {
            int idx = Arrays.binarySearch(keys, key);
            assert idx >= 0 || key > lastKey()
                    : "Keys must be inserted in ascending order [keys=" + Arrays.toString(keys) + ", newKey=" + key + ']';

            if (idx < 0) {
                long[] keys = Arrays.copyOf(this.keys, length + 1);
                keys[length] = key;

                Catalog[] values = Arrays.copyOf(this.values, length + 1);
                values[length] = catalog;

                return new CatalogByIndexMap(keys, values);
            } else {
                Catalog[] values = this.values.clone();
                values[idx] = catalog;

                return new CatalogByIndexMap(keys, values);
            }
        }
    }

    /**
     * Returns a new map with all entries strictly before the given key removed.
     */
    CatalogByIndexMap clearHead(long keyToLeave) {
        int idx = Arrays.binarySearch(keys, keyToLeave);

        assert idx >= 0;

        if (idx == 0) {
            return this;
        } else {
            int length = this.keys.length;

            long[] keys = Arrays.copyOfRange(this.keys, idx, length);
            Catalog[] values = Arrays.copyOfRange(this.values, idx, length);

            return new CatalogByIndexMap(keys, values);
        }
    }

    private void checkNotEmpty() {
        if (keys.length == 0) {
            throw new NoSuchElementException();
        }
    }
}
