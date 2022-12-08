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

import static java.util.Collections.emptyNavigableMap;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
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
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestSortedIndexStorage implements SortedIndexStorage {
    private static final Object NULL = new Object();

    private final ConcurrentNavigableMap<ByteBuffer, NavigableMap<RowId, Object>> index;

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

        Iterator<RowId> iterator = index.getOrDefault(key.byteBuffer(), emptyNavigableMap()).keySet().iterator();

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
            public RowId next() {
                checkClosed();

                return iterator.next();
            }
        };
    }

    @Override
    public void put(IndexRow row) {
        checkClosed();

        index.compute(row.indexColumns().byteBuffer(), (k, v) -> {
            NavigableMap<RowId, Object> rowIds = v == null ? new ConcurrentSkipListMap<>() : v;

            rowIds.put(row.rowId(), NULL);

            return rowIds;
        });
    }

    @Override
    public void remove(IndexRow row) {
        checkClosed();

        index.computeIfPresent(row.indexColumns().byteBuffer(), (k, v) -> {
            v.remove(row.rowId());

            return v.isEmpty() ? null : v;
        });
    }

    @Override
    public PeekCursor<IndexRow> scan(
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

        NavigableMap<ByteBuffer, NavigableMap<RowId, Object>> navigableMap;

        if (lowerBound == null && upperBound == null) {
            navigableMap = index;
        } else if (lowerBound == null) {
            navigableMap = index.headMap(upperBound.byteBuffer());
        } else if (upperBound == null) {
            navigableMap = index.tailMap(lowerBound.byteBuffer());
        } else {
            try {
                navigableMap = index.subMap(lowerBound.byteBuffer(), upperBound.byteBuffer());
            } catch (IllegalArgumentException e) {
                navigableMap = emptyNavigableMap();
            }
        }

        return new ScanCursor(navigableMap);
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

    private class ScanCursor implements PeekCursor<IndexRow> {
        private final NavigableMap<ByteBuffer, NavigableMap<RowId, Object>> indexMap;

        @Nullable
        private Boolean hasNext;

        @Nullable
        private Entry<ByteBuffer, NavigableMap<RowId, Object>> indexMapEntry;

        @Nullable
        private RowId rowId;

        private ScanCursor(NavigableMap<ByteBuffer, NavigableMap<RowId, Object>> indexMap) {
            this.indexMap = indexMap;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            checkClosed();

            advanceIfNeeded();

            return hasNext;
        }

        @Override
        public IndexRow next() {
            checkClosed();

            advanceIfNeeded();

            boolean hasNext = this.hasNext;

            if (!hasNext) {
                throw new NoSuchElementException();
            }

            this.hasNext = null;

            return new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), indexMapEntry.getKey()), rowId);
        }

        @Override
        public @Nullable IndexRow peek() {
            Entry<ByteBuffer, NavigableMap<RowId, Object>> entry0 = indexMapEntry == null ? indexMap.firstEntry() : indexMapEntry;

            RowId nextRowId = null;

            if (rowId == null) {
                if (entry0 != null) {
                    nextRowId = getRowId(entry0.getValue().firstEntry());
                }
            } else {
                Entry<RowId, Object> entry1 = entry0.getValue().higherEntry(rowId);

                if (entry1 != null) {
                    nextRowId = entry1.getKey();
                } else {
                    entry0 = indexMap.higherEntry(entry0.getKey());

                    if (entry0 != null) {
                        nextRowId = getRowId(entry0.getValue().firstEntry());
                    }
                }
            }

            return nextRowId == null ? null : new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), entry0.getKey()), nextRowId);
        }

        private void advanceIfNeeded() {
            if (hasNext != null) {
                return;
            }

            if (indexMapEntry == null) {
                indexMapEntry = indexMap.firstEntry();
            }

            if (rowId == null) {
                if (indexMapEntry != null) {
                    rowId = getRowId(indexMapEntry.getValue().firstEntry());
                }
            } else {
                Entry<RowId, Object> nextRowIdEntry = indexMapEntry.getValue().higherEntry(rowId);

                if (nextRowIdEntry != null) {
                    rowId = nextRowIdEntry.getKey();
                } else {
                    Entry<ByteBuffer, NavigableMap<RowId, Object>> nextIndexMapEntry = indexMap.higherEntry(indexMapEntry.getKey());

                    if (nextIndexMapEntry == null) {
                        hasNext = false;

                        return;
                    } else {
                        indexMapEntry = nextIndexMapEntry;

                        rowId = getRowId(indexMapEntry.getValue().firstEntry());
                    }
                }
            }

            hasNext = rowId != null;
        }

        @Nullable
        private RowId getRowId(@Nullable Entry<RowId, ?> rowIdEntry) {
            return rowIdEntry == null ? null : rowIdEntry.getKey();
        }
    }
}
