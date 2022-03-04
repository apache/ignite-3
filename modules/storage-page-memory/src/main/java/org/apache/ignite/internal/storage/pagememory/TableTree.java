/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storage-page-memory module.
 */
public class TableTree extends BplusTree<SearchRow, DataRow> {
    /**
     * Constructor.
     *
     * @param name Tree name.
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     */
    public TableTree(
            String name,
            int grpId,
            @Nullable String grpName,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) {
        super(name, grpId, grpName, pageMem, lockLsnr, FLAG_AUX, globalRmvId, metaPageId, reuseList);
    }

    /** {@inheritDoc} */
    @Override
    protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    protected int compare(BplusIo<SearchRow> io, long pageAddr, int idx, SearchRow row) throws IgniteInternalCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public DataRow getRow(BplusIo<SearchRow> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        return null;
    }
}
