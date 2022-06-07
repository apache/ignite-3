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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainMetaIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storing version chains.
 */
public class VersionChainTree extends BplusTree<VersionChainLink, VersionChain> {
    private final int partitionId;

    private final VersionChainDataPageReader dataPageReader;

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param partitionId Partition id.
     * @param initNew {@code True} if new tree should be created.
     */
    public VersionChainTree(
            int grpId,
            String grpName,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            int partitionId,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super(
                "VersionChainTree_" + grpId,
                grpId,
                grpName,
                pageMem,
                lockLsnr,
                FLAG_AUX,
                globalRmvId,
                metaPageId,
                reuseList
        );

        this.partitionId = partitionId;

        dataPageReader = new VersionChainDataPageReader(pageMem, grpId, IoStatisticsHolderNoOp.INSTANCE);

        setIos(VersionChainInnerIo.VERSIONS, VersionChainLeafIo.VERSIONS, VersionChainMetaIo.VERSIONS);

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override
    protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
        return pageMem.allocatePage(grpId, partitionId, defaultPageFlag);
    }

    /** {@inheritDoc} */
    @Override
    protected int compare(BplusIo<VersionChainLink> io, long pageAddr, int idx, VersionChainLink row) {
        VersionChainIo versionChainIo = (VersionChainIo) io;

        long thisLink = versionChainIo.link(pageAddr, idx);
        long thatLink = row.link();
        return Long.compare(thisLink, thatLink);
    }

    /** {@inheritDoc} */
    @Override
    public VersionChain getRow(BplusIo<VersionChainLink> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        VersionChainIo rowIo = (VersionChainIo) io;

        long link = rowIo.link(pageAddr, idx);

        return dataPageReader.getRowByLink(link);
    }
}
