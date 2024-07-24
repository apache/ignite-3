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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainInnerIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storing version chains.
 */
public class VersionChainTree extends BplusTree<VersionChainKey, VersionChain> {
    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition id.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteInternalCheckedException If failed.
     */
    public VersionChainTree(
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
        super(
                "VersionChainTree",
                grpId,
                grpName,
                partId,
                pageMem,
                lockLsnr,
                globalRmvId,
                metaPageId,
                reuseList
        );

        setIos(VersionChainInnerIo.VERSIONS, VersionChainLeafIo.VERSIONS, VersionChainMetaIo.VERSIONS);

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override
    protected int compare(BplusIo<VersionChainKey> io, long pageAddr, int idx, VersionChainKey row) {
        VersionChainIo versionChainIo = (VersionChainIo) io;

        return versionChainIo.compare(pageAddr, idx, row);
    }

    /** {@inheritDoc} */
    @Override
    public VersionChain getRow(BplusIo<VersionChainKey> io, long pageAddr, int idx, Object x) {
        VersionChainIo versionChainIo = (VersionChainIo) io;

        return versionChainIo.getRow(pageAddr, idx, partId);
    }
}
