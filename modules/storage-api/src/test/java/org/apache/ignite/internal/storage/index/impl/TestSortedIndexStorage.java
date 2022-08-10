/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index.impl;

import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.ToIntFunction;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowDeserializer;
import org.apache.ignite.internal.storage.index.IndexRowSerializer;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestSortedIndexStorage implements SortedIndexStorage {
    private final ConcurrentNavigableMap<BinaryTuple, Set<RowId>> index;

    private final SortedIndexDescriptor descriptor;

    /**
     * Constructor.
     */
    public TestSortedIndexStorage(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
        this.index = new ConcurrentSkipListMap<>(BinaryTupleComparator.newComparator(descriptor));
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public IndexRowSerializer indexRowSerializer() {
        return new BinaryTupleRowSerializer(descriptor);
    }

    @Override
    public IndexRowDeserializer indexRowDeserializer() {
        return new BinaryTupleRowDeserializer(descriptor);
    }

    @Override
    public void put(IndexRow row) {
        index.compute(row.indexColumns(), (k, v) -> {
            if (v == null) {
                return Set.of(row.rowId());
            } else {
                var result = new HashSet<RowId>(capacity(v.size() + 1));

                result.addAll(v);
                result.add(row.rowId());

                return result;
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        index.computeIfPresent(row.indexColumns(), (k, v) -> {
            if (v.contains(row.rowId())) {
                if (v.size() == 1) {
                    return null;
                } else {
                    var result = new HashSet<>(v);

                    result.remove(row.rowId());

                    return result;
                }
            } else {
                return v;
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<IndexRow> scan(
            @Nullable BinaryTuple lowerBound,
            @Nullable BinaryTuple upperBound,
            int flags
    ) {
        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        NavigableMap<BinaryTuple, Set<RowId>> index = this.index;
        int direction = 1;

        // Swap bounds and flip index for backwards scan.
        if ((flags & BACKWARDS) != 0) {
            index = index.descendingMap();
            direction = -1;

            boolean tempBoolean = includeLower;
            includeLower = includeUpper;
            includeUpper = tempBoolean;

            BinaryTuple tempBound = lowerBound;
            lowerBound = upperBound;
            upperBound = tempBound;
        }

        ToIntFunction<BinaryTuple> lowerCmp = lowerBound == null ? row -> 1 : boundComparator(lowerBound, direction, includeLower ? 0 : -1);
        ToIntFunction<BinaryTuple> upperCmp = upperBound == null ? row -> -1 : boundComparator(upperBound, direction, includeUpper ? 0 : 1);

        Iterator<? extends IndexRow> iterator = index.entrySet().stream()
                .dropWhile(e -> lowerCmp.applyAsInt(e.getKey()) < 0)
                .takeWhile(e -> upperCmp.applyAsInt(e.getKey()) <= 0)
                .flatMap(e -> e.getValue().stream().map(rowId -> new IndexRowImpl(e.getKey(), rowId)))
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    private ToIntFunction<BinaryTuple> boundComparator(BinaryTuple bound, int direction, int equals) {
        BinaryTupleComparator comparator = BinaryTupleComparator.newPrefixComparator(descriptor, bound.count());

        return tuple -> comparator.compare(tuple, bound, direction, equals);
    }
}
