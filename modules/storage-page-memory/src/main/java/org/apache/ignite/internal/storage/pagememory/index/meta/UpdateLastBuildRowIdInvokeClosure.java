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

package org.apache.ignite.internal.storage.pagememory.index.meta;

import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Closure for updating the last row ID for which the index was built.
 */
public class UpdateLastBuildRowIdInvokeClosure implements InvokeClosure<IndexMeta> {
    private final @Nullable RowId newRowId;

    private IndexMeta newRow;

    /**
     * Constructor.
     *
     * @param newRowId New last row ID for which the index was built, {@code null} means index building is finished.
     */
    public UpdateLastBuildRowIdInvokeClosure(@Nullable RowId newRowId) {
        this.newRowId = newRowId;
    }

    @Override
    public void call(@Nullable IndexMeta oldRow) throws IgniteInternalCheckedException {
        assert oldRow != null;

        newRow = new IndexMeta(oldRow.indexId(), oldRow.metaPageId(), newRowId == null ? null : newRowId.uuid());
    }

    @Override
    public @Nullable IndexMeta newRow() {
        return newRow;
    }

    @Override
    public OperationType operationType() {
        return OperationType.PUT;
    }
}
