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

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.jetbrains.annotations.Nullable;

/**
 * Insert closure that removes corresponding {@link IndexColumns} from a {@link FreeListImpl} after removing it from the {@link
 * SortedIndexTree}.
 */
class RemoveSortedIndexRowInvokeClosure implements InvokeClosure<SortedIndexRow> {
    /** Sorted index row instance for removal. */
    private final SortedIndexRow sortedIndexRow;

    /** Free list to insert data into in case of necessity. */
    private final FreeListImpl freeList;

    /** Operation type, either {@link OperationType#REMOVE} or {@link OperationType#NOOP} if row is missing. */
    private OperationType operationType = OperationType.REMOVE;

    /**
     * Constructor.
     *
     * @param sortedIndexRow Sorted index row instance for removal.
     * @param freeList Free list to insert data into in case of necessity.
     */
    public RemoveSortedIndexRowInvokeClosure(SortedIndexRow sortedIndexRow, FreeListImpl freeList) {
        assert sortedIndexRow.indexColumns().link() == 0L;

        this.sortedIndexRow = sortedIndexRow;
        this.freeList = freeList;
    }

    @Override
    public void call(@Nullable SortedIndexRow oldRow) {
        if (oldRow == null) {
            operationType = OperationType.NOOP;
        } else {
            sortedIndexRow.indexColumns().link(oldRow.indexColumns().link());
        }
    }

    @Override
    public @Nullable SortedIndexRow newRow() {
        return null;
    }

    @Override
    public OperationType operationType() {
        return operationType;
    }

    /**
     * Method to call after {@link BplusTree#invoke(Object, Object, InvokeClosure)} has completed.
     *
     * @throws IgniteInternalCheckedException If failed to remove data from the free list.
     */
    public void afterCompletion() throws IgniteInternalCheckedException {
        IndexColumns indexColumns = sortedIndexRow.indexColumns();

        if (indexColumns.link() != NULL_LINK) {
            assert operationType == OperationType.REMOVE;

            freeList.removeDataRowByLink(indexColumns.link());

            indexColumns.link(NULL_LINK);
        }
    }
}
