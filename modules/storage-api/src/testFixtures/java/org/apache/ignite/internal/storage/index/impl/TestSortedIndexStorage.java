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
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
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

    /**
     * {@code NavigableMap<RowId, Object>} is used as a {@link NavigableSet}, but map was chosen because methods like
     * {@link NavigableSet#first()} throw an {@link NoSuchElementException} if the set is empty.
     */
    private final ConcurrentNavigableMap<ByteBuffer, NavigableMap<RowId, Object>> index;

    private final SortedIndexDescriptor descriptor;

    private volatile boolean closed;

    private volatile boolean rebalance;

    private volatile @Nullable RowId lastBuiltRowId;

    /**
     * Constructor.
     */
    public TestSortedIndexStorage(SortedIndexDescriptor descriptor, int partitionId) {
        this.descriptor = descriptor;
        this.index = new ConcurrentSkipListMap<>(new BinaryTupleComparator(descriptor));

        lastBuiltRowId = RowId.lowestRowId(partitionId);
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        checkStorageClosedOrInProcessOfRebalance();

        Iterator<RowId> iterator = index.getOrDefault(key.byteBuffer(), emptyNavigableMap()).keySet().iterator();

        return new Cursor<>() {
            @Override
            public void close() {
                // No-op.
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProcessOfRebalance();

                return iterator.hasNext();
            }

            @Override
            public RowId next() {
                checkStorageClosedOrInProcessOfRebalance();

                return iterator.next();
            }
        };
    }

    @Override
    public void put(IndexRow row) {
        checkStorageClosed();

        index.compute(row.indexColumns().byteBuffer(), (k, v) -> {
            NavigableMap<RowId, Object> rowIds = v == null ? new ConcurrentSkipListMap<>() : v;

            rowIds.put(row.rowId(), NULL);

            return rowIds;
        });
    }

    @Override
    public void remove(IndexRow row) {
        checkStorageClosedOrInProcessOfRebalance();

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
        checkStorageClosedOrInProcessOfRebalance();

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

        clear0();
    }

    /**
     * Removes all index data.
     */
    public void clear() {
        checkStorageClosedOrInProcessOfRebalance();

        index.clear();
    }

    private void clear0() {
        index.clear();
    }

    private class ScanCursor implements PeekCursor<IndexRow> {
        private final NavigableMap<ByteBuffer, NavigableMap<RowId, Object>> indexMap;

        @Nullable
        private Boolean hasNext;

        @Nullable
        private Entry<ByteBuffer, NavigableMap<RowId, Object>> currentEntry;

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
            checkStorageClosedOrInProcessOfRebalance();

            advanceIfNeeded();

            return hasNext;
        }

        @Override
        public IndexRow next() {
            checkStorageClosedOrInProcessOfRebalance();

            advanceIfNeeded();

            boolean hasNext = this.hasNext;

            if (!hasNext) {
                throw new NoSuchElementException();
            }

            this.hasNext = null;

            return new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), currentEntry.getKey()), rowId);
        }

        @Override
        public @Nullable IndexRow peek() {
            checkStorageClosedOrInProcessOfRebalance();

            if (hasNext != null) {
                if (hasNext) {
                    return new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), currentEntry.getKey()), rowId);
                }

                return null;
            }

            Entry<ByteBuffer, NavigableMap<RowId, Object>> indexMapEntry0 = currentEntry == null ? indexMap.firstEntry() : currentEntry;

            RowId nextRowId = null;

            if (rowId == null) {
                if (indexMapEntry0 != null) {
                    nextRowId = getRowId(indexMapEntry0.getValue().firstEntry());
                }
            } else {
                Entry<RowId, Object> nextRowIdEntry = indexMapEntry0.getValue().higherEntry(rowId);

                if (nextRowIdEntry != null) {
                    nextRowId = nextRowIdEntry.getKey();
                } else {
                    indexMapEntry0 = indexMap.higherEntry(indexMapEntry0.getKey());

                    if (indexMapEntry0 != null) {
                        nextRowId = getRowId(indexMapEntry0.getValue().firstEntry());
                    }
                }
            }

            return nextRowId == null
                    ? null : new IndexRowImpl(new BinaryTuple(descriptor.binaryTupleSchema(), indexMapEntry0.getKey()), nextRowId);
        }

        private void advanceIfNeeded() {
            if (hasNext != null) {
                return;
            }

            if (currentEntry == null) {
                currentEntry = indexMap.firstEntry();
            }

            if (rowId == null) {
                if (currentEntry != null) {
                    rowId = getRowId(currentEntry.getValue().firstEntry());
                }
            } else {
                Entry<RowId, Object> nextRowIdEntry = currentEntry.getValue().higherEntry(rowId);

                if (nextRowIdEntry != null) {
                    rowId = nextRowIdEntry.getKey();
                } else {
                    Entry<ByteBuffer, NavigableMap<RowId, Object>> nextIndexMapEntry = indexMap.higherEntry(currentEntry.getKey());

                    if (nextIndexMapEntry == null) {
                        hasNext = false;

                        return;
                    } else {
                        currentEntry = nextIndexMapEntry;

                        rowId = getRowId(currentEntry.getValue().firstEntry());
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

    private void checkStorageClosed() {
        if (closed) {
            throw new StorageClosedException();
        }
    }

    private void checkStorageClosedOrInProcessOfRebalance() {
        checkStorageClosed();

        if (rebalance) {
            throw new StorageRebalanceException("Storage in the process of rebalancing");
        }
    }

    /**
     * Starts rebalancing for the storage.
     */
    public void startRebalance() {
        checkStorageClosed();

        rebalance = true;

        clear0();
    }

    /**
     * Aborts rebalance of the storage.
     */
    public void abortRebalance() {
        checkStorageClosed();

        if (!rebalance) {
            return;
        }

        rebalance = false;

        clear0();
    }

    /**
     * Completes rebalance of the storage.
     */
    public void finishRebalance() {
        checkStorageClosed();

        assert rebalance;

        rebalance = false;
    }

    /**
     * Returns all indexed row ids.
     */
    public Set<RowId> allRowsIds() {
        return index.values().stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    }

    @Override
    public @Nullable RowId getLastBuiltRowId() {
        checkStorageClosedOrInProcessOfRebalance();

        return lastBuiltRowId;
    }

    @Override
    public void setLastBuiltRowId(@Nullable RowId rowId) {
        checkStorageClosedOrInProcessOfRebalance();

        lastBuiltRowId = rowId;
    }
}
