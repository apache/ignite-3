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

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.jetbrains.annotations.Nullable;

/**
 * Closure for update {@link IndexMeta#nextRowIdUuidToBuild()}.
 */
public class UpdateLastRowIdUuidToBuildInvokeClosure implements InvokeClosure<IndexMeta> {
    private final @Nullable UUID newLastRowIdUuidToBuilt;

    private IndexMeta newRow;

    /**
     * Constructor.
     *
     * @param newLastRowIdUuidToBuilt Row ID uuid for which the index needs to be built, {@code null} means that the index building has
     *      completed.
     */
    public UpdateLastRowIdUuidToBuildInvokeClosure(@Nullable UUID newLastRowIdUuidToBuilt) {
        this.newLastRowIdUuidToBuilt = newLastRowIdUuidToBuilt;
    }

    @Override
    public void call(@Nullable IndexMeta oldRow) throws IgniteInternalCheckedException {
        assert oldRow != null;

        newRow = new IndexMeta(oldRow.indexId(), oldRow.indexType(), oldRow.metaPageId(), newLastRowIdUuidToBuilt);
    }

    @Override
    public IndexMeta newRow() {
        return newRow;
    }

    @Override
    public OperationType operationType() {
        return OperationType.PUT;
    }
}
