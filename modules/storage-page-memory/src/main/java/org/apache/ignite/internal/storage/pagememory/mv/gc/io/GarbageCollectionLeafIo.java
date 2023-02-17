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

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GarbageCollectionRowVersion;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GarbageCollectionTree;

/**
 * IO routines for {@link GarbageCollectionTree} leaf pages.
 */
public class GarbageCollectionLeafIo extends BplusLeafIo<GarbageCollectionRowVersion> implements GarbageCollectionIo {
    /** Page IO type. */
    public static final short T_GARBAGE_COLLECTION_LEAF_IO = 16;

    /** I/O versions. */
    public static final IoVersions<GarbageCollectionLeafIo> VERSIONS = new IoVersions<>(new GarbageCollectionLeafIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private GarbageCollectionLeafIo(int ver) {
        super(T_GARBAGE_COLLECTION_LEAF_IO, ver, SIZE_IN_BYTES);
    }

    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<GarbageCollectionRowVersion> srcIo, long srcPageAddr, int srcIdx) {
        GarbageCollectionIo.super.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, GarbageCollectionRowVersion row) {
        GarbageCollectionIo.super.storeByOffset(pageAddr, off, row);
    }

    @Override
    public GarbageCollectionRowVersion getLookupRow(BplusTree<GarbageCollectionRowVersion, ?> tree, long pageAddr, int idx) {
        return getRow(pageAddr, idx, getPartitionId(pageAddr));
    }

    private static int getPartitionId(long pageAddr) {
        long pageId = getPageId(pageAddr);
        return PageIdUtils.partitionId(pageId);
    }
}
