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

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_VERSION_CHAIN_LEAF_IO;

import java.util.function.Consumer;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChain;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainKey;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;

/**
 * IO routines for {@link VersionChainTree} leaf pages.
 *
 * <p>Structure: link(long).
 */
public final class VersionChainLeafIo extends BplusLeafIo<VersionChainKey> implements VersionChainIo {
    /** I/O versions. */
    public static final IoVersions<VersionChainLeafIo> VERSIONS = new IoVersions<>(new VersionChainLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private VersionChainLeafIo(int ver) {
        super(T_VERSION_CHAIN_LEAF_IO, ver, SIZE_IN_BYTES);
    }

    /** {@inheritDoc} */
    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<VersionChainKey> srcIo, long srcPageAddr, int srcIdx) {
        VersionChainIo.super.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    /** {@inheritDoc} */
    @Override
    public void storeByOffset(long pageAddr, int off, VersionChainKey row) {
        VersionChainIo.super.storeByOffset(pageAddr, off, row);
    }

    /** {@inheritDoc} */
    @Override
    public VersionChainKey getLookupRow(BplusTree<VersionChainKey, ?> tree, long pageAddr, int idx) {
        return getRow(pageAddr, idx, getPartitionId(pageAddr));
    }

    @Override
    public void visit(BplusTree<VersionChainKey, ?> tree, long pageAddr, Consumer<VersionChainKey> c) {
        int partitionId = getPartitionId(pageAddr);

        int count = getCount(pageAddr);

        for (int i = 0; i < count; i++) {
            VersionChain chain = getRow(pageAddr, i, partitionId);

            c.accept(chain);
        }
    }

    private static int getPartitionId(long pageAddr) {
        long pageId = getPageId(pageAddr);
        return PageIdUtils.partitionId(pageId);
    }
}
