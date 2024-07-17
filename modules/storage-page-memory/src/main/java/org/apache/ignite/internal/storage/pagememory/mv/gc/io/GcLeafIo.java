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

package org.apache.ignite.internal.storage.pagememory.mv.gc.io;

import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_GC_LEAF_IO;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcRowVersion;

/**
 * IO routines for {@link GcQueue} leaf pages.
 */
public class GcLeafIo extends BplusLeafIo<GcRowVersion> implements GcIo {
    /** I/O versions. */
    public static final IoVersions<GcLeafIo> VERSIONS = new IoVersions<>(new GcLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private GcLeafIo(int ver) {
        super(T_GC_LEAF_IO, ver, SIZE_IN_BYTES);
    }

    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<GcRowVersion> srcIo, long srcPageAddr, int srcIdx) {
        GcIo.super.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, GcRowVersion row) {
        GcIo.super.storeByOffset(pageAddr, off, row);
    }

    @Override
    public GcRowVersion getLookupRow(BplusTree<GcRowVersion, ?> tree, long pageAddr, int idx) {
        return getRow(pageAddr, idx, getPartitionId(pageAddr));
    }

    private static int getPartitionId(long pageAddr) {
        long pageId = getPageId(pageAddr);
        return PageIdUtils.partitionId(pageId);
    }
}
