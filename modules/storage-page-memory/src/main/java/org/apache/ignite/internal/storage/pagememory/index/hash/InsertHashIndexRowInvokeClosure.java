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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.canFullyInline;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.jetbrains.annotations.Nullable;

/**
 * Insert closure that inserts corresponding {@link IndexColumns} into a {@link IndexColumnsFreeList} before writing to the {@link
 * HashIndexTree}.
 */
class InsertHashIndexRowInvokeClosure implements InvokeClosure<HashIndexRow> {
    /** Hash index row instance for insertion. */
    private final HashIndexRow hashIndexRow;

    /** Free list to insert data into in case of necessity. */
    private final IndexColumnsFreeList freeList;

    /** Inline size in bytes. */
    private final int inlineSize;

    /** Operation type, either {@link OperationType#PUT} or {@link OperationType#NOOP} depending on the tree state. */
    private OperationType operationType = OperationType.PUT;

    /**
     * Constructor.
     *
     * @param hashIndexRow Hash index row instance for insertion.
     * @param freeList Free list to insert data into in case of necessity.
     * @param inlineSize Inline size in bytes.
     */
    public InsertHashIndexRowInvokeClosure(HashIndexRow hashIndexRow, IndexColumnsFreeList freeList, int inlineSize) {
        assert hashIndexRow.indexColumns().link() == NULL_LINK;

        this.hashIndexRow = hashIndexRow;
        this.freeList = freeList;
        this.inlineSize = inlineSize;
    }

    @Override
    public void call(@Nullable HashIndexRow oldRow) throws IgniteInternalCheckedException {
        if (oldRow != null) {
            operationType = OperationType.NOOP;

            return;
        }

        if (!canFullyInline(hashIndexRow.indexColumns().valueSize(), inlineSize)) {
            freeList.insertDataRow(hashIndexRow.indexColumns());
        }
    }

    @Override
    public @Nullable HashIndexRow newRow() {
        return hashIndexRow;
    }

    @Override
    public OperationType operationType() {
        return operationType;
    }
}
