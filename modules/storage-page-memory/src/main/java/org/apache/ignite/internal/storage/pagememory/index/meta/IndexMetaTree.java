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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.meta.io.IndexMetaTreeMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storing {@link IndexMeta}.
 */
public class IndexMetaTree extends BplusTree<IndexMetaKey, IndexMeta> {
    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteInternalCheckedException If failed.
     */
    public IndexMetaTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super("IndexMetaTree", grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList);

        setIos(IndexMetaInnerIo.VERSIONS, IndexMetaLeafIo.VERSIONS, IndexMetaTreeMetaIo.VERSIONS);

        initTree(initNew);
    }

    @Override
    protected int compare(BplusIo<IndexMetaKey> io, long pageAddr, int idx, IndexMetaKey row) {
        IndexMetaIo indexMetaIo = (IndexMetaIo) io;

        return indexMetaIo.compare(pageAddr, idx, row);
    }

    @Override
    public IndexMeta getRow(BplusIo<IndexMetaKey> io, long pageAddr, int idx, @Nullable Object x) {
        IndexMetaIo indexMetaIo = (IndexMetaIo) io;

        return indexMetaIo.getRow(pageAddr, idx);
    }
}
