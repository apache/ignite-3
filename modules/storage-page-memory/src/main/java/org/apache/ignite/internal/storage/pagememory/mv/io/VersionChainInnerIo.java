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

import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_VERSION_CHAIN_INNER_IO;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainKey;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;

/**
 * IO routines for {@link VersionChainTree} inner pages.
 *
 * <p>Structure: link(long).
 */
public final class VersionChainInnerIo extends BplusInnerIo<VersionChainKey> implements VersionChainIo {
    /** I/O versions. */
    public static final IoVersions<VersionChainInnerIo> VERSIONS = new IoVersions<>(new VersionChainInnerIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private VersionChainInnerIo(int ver) {
        super(T_VERSION_CHAIN_INNER_IO, ver, true, SIZE_IN_BYTES);
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
        return getRow(pageAddr, idx, 0xFFFF);
    }
}
