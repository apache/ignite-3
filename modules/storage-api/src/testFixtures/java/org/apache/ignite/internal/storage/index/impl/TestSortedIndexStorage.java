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

import static java.util.Collections.emptyNavigableSet;
import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.storage.RowId.highestRowId;
import static org.apache.ignite.internal.storage.RowId.lowestRowId;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.util.TransformingIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of MV sorted index storage.
 */
public class TestSortedIndexStorage extends AbstractTestIndexStorage implements SortedIndexStorage {
    private final int partitionId;

    private final NavigableSet<IndexRow> index;

    private final StorageSortedIndexDescriptor descriptor;

    /**
     * Constructor.
     */
    public TestSortedIndexStorage(int partitionId, StorageSortedIndexDescriptor descriptor) {
        super(partitionId, descriptor.isPk());

        BinaryTupleComparator binaryTupleComparator = new BinaryTupleComparator(descriptor.columns());

        this.partitionId = partitionId;
        this.descriptor = descriptor;
        this.index = new ConcurrentSkipListSet<>(
                comparing((IndexRow indexRow) -> indexRow.indexColumns().byteBuffer(), binaryTupleComparator)
                        .thenComparing(IndexRow::rowId)
        );
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    Iterator<RowId> getRowIdIteratorForGetByBinaryTuple(BinaryTuple key) {
        // These must be two different instances, because "scan" call messes up headers.
        BinaryTuplePrefix lowerBound = BinaryTuplePrefix.fromBinaryTuple(key);
        BinaryTuplePrefix higherBound = BinaryTuplePrefix.fromBinaryTuple(key);

        PeekCursor<IndexRow> peekCursor = scan(lowerBound, higherBound, GREATER_OR_EQUAL | LESS_OR_EQUAL);

        return new TransformingIterator<>(peekCursor, IndexRow::rowId);
    }

    @Override
    public void put(IndexRow row) {
        checkStorageClosed(false);

        index.add(row);
    }

    @Override
    public void remove(IndexRow row) {
        checkStorageClosedOrInProcessOfRebalance(false);

        index.remove(row);
    }

    @Override
    public PeekCursor<IndexRow> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags
    ) {
        checkStorageClosedOrInProcessOfRebalance(true);

        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        if (!includeLower && lowerBound != null) {
            setEqualityFlag(lowerBound);
        }

        if (includeUpper && upperBound != null) {
            setEqualityFlag(upperBound);
        }

        NavigableSet<IndexRow> navigableSet;

        if (lowerBound == null && upperBound == null) {
            navigableSet = index;
        } else if (lowerBound == null) {
            navigableSet = index.headSet(prefixToIndexRow(upperBound, highestRowId(partitionId)), true);
        } else if (upperBound == null) {
            navigableSet = index.tailSet(prefixToIndexRow(lowerBound, lowestRowId(partitionId)), true);
        } else {
            try {
                navigableSet = index.subSet(
                        prefixToIndexRow(lowerBound, lowestRowId(partitionId)),
                        true,
                        prefixToIndexRow(upperBound, highestRowId(partitionId)),
                        true
                );
            } catch (IllegalArgumentException e) {
                // Upper bound is below the lower bound.
                navigableSet = emptyNavigableSet();
            }
        }

        pendingCursors.incrementAndGet();

        return new ScanCursor(navigableSet);
    }

    private IndexRowImpl prefixToIndexRow(BinaryTuplePrefix prefix, RowId rowId) {
        var binaryTuple = new BinaryTuple(descriptor.binaryTupleSchema().elementCount(), prefix.byteBuffer());

        return new IndexRowImpl(binaryTuple, rowId);
    }

    private static void setEqualityFlag(BinaryTuplePrefix prefix) {
        ByteBuffer buffer = prefix.byteBuffer();

        byte flags = buffer.get(0);

        buffer.put(0, (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG));
    }

    @Override
    void clear0() {
        index.clear();
    }

    private static final IndexRow NO_PEEKED_ROW = new IndexRowImpl(null, null);

    private class ScanCursor implements PeekCursor<IndexRow> {
        private final NavigableSet<IndexRow> indexSet;

        @Nullable
        private Boolean hasNext;

        @Nullable
        private IndexRow currentRow;

        @Nullable
        private IndexRow peekedRow = NO_PEEKED_ROW;

        private ScanCursor(NavigableSet<IndexRow> indexSet) {
            this.indexSet = indexSet;
        }

        @Override
        public void close() {
            pendingCursors.decrementAndGet();
        }

        @Override
        public boolean hasNext() {
            checkStorageClosedOrInProcessOfRebalance(true);

            if (hasNext != null) {
                return hasNext;
            }

            currentRow = peekedRow == NO_PEEKED_ROW ? peek() : peekedRow;
            peekedRow = NO_PEEKED_ROW;

            hasNext = currentRow != null;
            return hasNext;
        }

        @Override
        public IndexRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            this.hasNext = null;

            return currentRow;
        }

        @Override
        public @Nullable IndexRow peek() {
            checkStorageClosedOrInProcessOfRebalance(true);

            if (hasNext != null) {
                return currentRow;
            }

            if (currentRow == null) {
                try {
                    peekedRow = indexSet.first();
                } catch (NoSuchElementException e) {
                    peekedRow = null;
                }
            } else {
                peekedRow = indexSet.higher(this.currentRow);
            }

            return peekedRow;
        }
    }

    /**
     * Returns all indexed row ids.
     */
    public Set<RowId> allRowsIds() {
        return index.stream().map(IndexRow::rowId).collect(Collectors.toSet());
    }
}
