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

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.canFullyInline;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.jetbrains.annotations.Nullable;

/**
 * Insert closure that inserts corresponding {@link IndexColumns} into a {@link FreeListImpl} before writing to the {@link
 * SortedIndexTree}.
 */
class InsertSortedIndexRowInvokeClosure implements InvokeClosure<SortedIndexRow> {
    /** Sorted index row instance for insertion. */
    private final SortedIndexRow sortedIndexRow;

    /** Free list to insert data into in case of necessity. */
    private final FreeListImpl freeList;

    /** Operation type, either {@link OperationType#PUT} or {@link OperationType#NOOP} depending on the tree state. */
    private OperationType operationType = OperationType.PUT;

    /** Inline size in bytes. */
    private final int inlineSize;

    /**
     * Constructor.
     *
     * @param sortedIndexRow Sorted index row instance for insertion.
     * @param freeList Free list to insert data into in case of necessity.
     * @param inlineSize Inline size in bytes.
     */
    public InsertSortedIndexRowInvokeClosure(SortedIndexRow sortedIndexRow, FreeListImpl freeList, int inlineSize) {
        assert sortedIndexRow.indexColumns().link() == NULL_LINK;

        this.sortedIndexRow = sortedIndexRow;
        this.freeList = freeList;
        this.inlineSize = inlineSize;
    }

    @Override
    public void call(@Nullable SortedIndexRow oldRow) throws IgniteInternalCheckedException {
        if (oldRow != null) {
            operationType = OperationType.NOOP;

            return;
        }

        if (!canFullyInline(sortedIndexRow.indexColumns().valueSize(), inlineSize)) {
            freeList.insertDataRow(sortedIndexRow.indexColumns());
        }
    }

    @Override
    public @Nullable SortedIndexRow newRow() {
        return sortedIndexRow;
    }

    @Override
    public OperationType operationType() {
        return operationType;
    }
}
