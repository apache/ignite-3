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

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainLink;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChainTree;

/**
 * IO routines for {@link VersionChainTree} leaf pages.
 *
 * <p>Structure: link(long).
 */
public class VersionChainLeafIo extends BplusLeafIo<VersionChainLink> implements VersionChainIo {
    /** Page IO type. */
    public static final short T_VERSION_CHAIN_LEAF_IO = 10;

    /** I/O versions. */
    public static final IoVersions<VersionChainLeafIo> VERSIONS = new IoVersions<>(new VersionChainLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected VersionChainLeafIo(int ver) {
        super(T_VERSION_CHAIN_LEAF_IO, ver, VersionChainLink.SIZE_IN_BYTES);
    }

    /** {@inheritDoc} */
    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<VersionChainLink> srcIo, long srcPageAddr, int srcIdx) {
        assertPageType(dstPageAddr);

        long srcLink = link(srcPageAddr, srcIdx);

        int dstOff = offset(dstIdx);

        putLong(dstPageAddr, dstOff, srcLink);
    }

    /** {@inheritDoc} */
    @Override
    public void storeByOffset(long pageAddr, int off, VersionChainLink row) {
        assertPageType(pageAddr);

        putLong(pageAddr, off, row.link());
    }

    /** {@inheritDoc} */
    @Override
    public VersionChainLink getLookupRow(BplusTree<VersionChainLink, ?> tree, long pageAddr, int idx) {
        long link = link(pageAddr, idx);

        return new VersionChainLink(link);
    }

    /** {@inheritDoc} */
    @Override
    public long link(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return getLong(pageAddr, offset(idx));
    }
}
