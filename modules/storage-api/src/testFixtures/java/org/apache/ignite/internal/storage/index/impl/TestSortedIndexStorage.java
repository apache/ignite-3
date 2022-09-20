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

package org.apache.ignite.internal.storage.index.impl;

import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestSortedIndexStorage implements SortedIndexStorage {
    private final ConcurrentNavigableMap<ByteBuffer, Set<RowId>> index;

    private final Comparator<ByteBuffer> binaryTupleComparator;

    private final SortedIndexDescriptor descriptor;

    /**
     * Constructor.
     */
    public TestSortedIndexStorage(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
        this.binaryTupleComparator = new BinaryTupleComparator(descriptor);
        this.index = new ConcurrentSkipListMap<>(binaryTupleComparator);
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public void put(IndexRow row) {
        index.compute(row.indexColumns().byteBuffer(), (k, v) -> {
            if (v == null) {
                return Set.of(row.rowId());
            } else if (v.contains(row.rowId())) {
                return v;
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
        index.computeIfPresent(row.indexColumns().byteBuffer(), (k, v) -> {
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

    @Override
    public Cursor<IndexRow> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags
    ) {
        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        Stream<Map.Entry<ByteBuffer, Set<RowId>>> data = index.entrySet().stream();

        if (lowerBound != null) {
            ToIntFunction<ByteBuffer> lowerCmp = boundComparator(lowerBound, includeLower ? 0 : -1);

            data = data.dropWhile(e -> lowerCmp.applyAsInt(e.getKey()) < 0);
        }

        if (upperBound != null) {
            ToIntFunction<ByteBuffer> upperCmp = boundComparator(upperBound, includeUpper ? 0 : 1);

            data = data.takeWhile(e -> upperCmp.applyAsInt(e.getKey()) <= 0);
        }

        Iterator<? extends IndexRow> iterator = data
                .flatMap(e -> {
                    var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), e.getKey());

                    return e.getValue().stream().map(rowId -> new IndexRowImpl(tuple, rowId));
                })
                .iterator();

        return Cursor.fromIterator(iterator);
    }

    private ToIntFunction<ByteBuffer> boundComparator(BinaryTuplePrefix bound, int equals) {
        ByteBuffer boundBuffer = bound.byteBuffer();

        return tuple -> {
            int compare = binaryTupleComparator.compare(tuple, boundBuffer);

            return compare == 0 ? equals : compare;
        };
    }
}
