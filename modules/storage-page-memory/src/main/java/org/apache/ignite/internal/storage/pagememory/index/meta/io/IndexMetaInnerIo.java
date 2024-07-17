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

package org.apache.ignite.internal.storage.pagememory.index.meta.io;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.storage.pagememory.index.IndexPageTypes;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;

/**
 * IO routines for {@link IndexMetaTree} inner pages.
 */
public class IndexMetaInnerIo extends BplusInnerIo<IndexMetaKey> implements IndexMetaIo {
    /** I/O versions. */
    public static final IoVersions<IndexMetaInnerIo> VERSIONS = new IoVersions<>(new IndexMetaInnerIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private IndexMetaInnerIo(int ver) {
        super(IndexPageTypes.T_INDEX_META_INNER_IO, ver, true, SIZE_IN_BYTES);
    }

    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<IndexMetaKey> srcIo, long srcPageAddr, int srcIdx) {
        IndexMetaIo.super.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, IndexMetaKey row) {
        IndexMetaIo.super.storeByOffset(pageAddr, off, row);
    }

    @Override
    public IndexMeta getLookupRow(BplusTree<IndexMetaKey, ?> tree, long pageAddr, int idx) {
        return getRow(pageAddr, idx);
    }
}
