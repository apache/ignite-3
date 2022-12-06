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

import static java.util.Collections.emptyIterator;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

        Iterator<RowId> iterator = index.getOrDefault(key.byteBuffer(), Set.of()).iterator();

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
            Set<RowId> rowIds = v == null ? ConcurrentHashMap.newKeySet() : v;

            rowIds.add(row.rowId());

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

        NavigableMap<ByteBuffer, Set<RowId>> navigableMap;

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
                navigableMap = Collections.emptyNavigableMap();
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

    /**
     * Sorted index storage scan cursor.
     */
    private class ScanCursor implements Cursor<IndexRow> {
        private final NavigableMap<ByteBuffer, Set<RowId>> map;

        @Nullable
        private Entry<ByteBuffer, Set<RowId>> currentEntry;

        private Iterator<RowId> currentRowIdIterator;

        @Nullable
        private IndexRow nextRow;

        private ScanCursor(NavigableMap<ByteBuffer, Set<RowId>> map) {
            this.map = map;

            applyEntry(map.firstEntry());

            advance();
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            checkClosed();

            return nextRow != null;
        }

        @Override
        public IndexRow next() {
            checkClosed();

            if (nextRow == null) {
                throw new NoSuchElementException();
            }

            IndexRow indexRow = nextRow;

            advance();

            return indexRow;
        }

        private void advance() {
            nextRow = null;

            while (currentEntry != null) {
                if (currentRowIdIterator.hasNext()) {
                    RowId rowId = currentRowIdIterator.next();

                    nextRow = new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), currentEntry.getKey()), rowId);

                    break;
                }

                applyEntry(map.higherEntry(currentEntry.getKey()));
            }
        }

        private void applyEntry(@Nullable Entry<ByteBuffer, Set<RowId>> entry) {
            currentEntry = entry;

            currentRowIdIterator = entry == null ? emptyIterator() : currentEntry.getValue().iterator();
        }
    }
}
