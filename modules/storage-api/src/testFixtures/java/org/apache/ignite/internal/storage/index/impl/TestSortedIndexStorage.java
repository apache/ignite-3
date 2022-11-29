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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
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

    private final SortedIndexDescriptor descriptor;

    private volatile boolean closed;

    /**
     * Constructor.
     */
    public TestSortedIndexStorage(SortedIndexDescriptor descriptor) {
        this.descriptor = descriptor;
        this.index = new ConcurrentSkipListMap<>(new BinaryTupleComparator(descriptor));
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        checkClosed();

        Iterator<RowId> iterator = index.getOrDefault(key.byteBuffer(), Set.of()).stream()
                .peek(rowId -> checkClosed())
                .iterator();

        return Cursor.fromBareIterator(iterator);
    }

    @Override
    public void put(IndexRow row) {
        checkClosed();

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
        checkClosed();

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
        checkClosed();

        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        if (!includeLower && lowerBound != null) {
            setEqualityFlag(lowerBound);
        }

        if (includeUpper && upperBound != null) {
            setEqualityFlag(upperBound);
        }

        SortedMap<ByteBuffer, Set<RowId>> data;

        if (lowerBound == null && upperBound == null) {
            data = index;
        } else if (lowerBound == null) {
            data = index.headMap(upperBound.byteBuffer());
        } else if (upperBound == null) {
            data = index.tailMap(lowerBound.byteBuffer());
        } else {
            try {
                data = index.subMap(lowerBound.byteBuffer(), upperBound.byteBuffer());
            } catch (IllegalArgumentException e) {
                data = Collections.emptySortedMap();
            }
        }

        Iterator<? extends IndexRow> iterator = data.entrySet().stream()
                .flatMap(e -> {
                    var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), e.getKey());

                    return e.getValue().stream().map(rowId -> new IndexRowImpl(tuple, rowId));
                })
                .iterator();

        return new Cursor<>() {
            @Override
            public void close() {
                // No-op.
            }

            @Override
            public boolean hasNext() {
                checkClosed();

                return iterator.hasNext();
            }

            @Override
            public IndexRow next() {
                return iterator.next();
            }
        };
    }

    private static void setEqualityFlag(BinaryTuplePrefix prefix) {
        ByteBuffer buffer = prefix.byteBuffer();

        byte flags = buffer.get(0);

        buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
    }

    /**
     * Destroys the storage and the data in it.
     */
    public void destroy() {
        closed = true;

        index.clear();
    }

    /**
     * Removes all index data.
     */
    public void clear() {
        index.clear();
    }

    private void checkClosed() {
        if (closed) {
            throw new StorageClosedException("Storage is already closed");
        }
    }
}
