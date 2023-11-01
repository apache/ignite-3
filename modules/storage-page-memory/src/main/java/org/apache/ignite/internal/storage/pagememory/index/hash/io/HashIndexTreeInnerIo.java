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

package org.apache.ignite.internal.storage.pagememory.index.hash.io;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.storage.pagememory.index.IndexPageTypes.T_HASH_INDEX_INNER_IO_START;
import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.MAX_BINARY_TUPLE_INLINE_SIZE;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.storage.pagememory.index.InlineUtils;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexTree;

/**
 * {@link BplusInnerIo} implementation for {@link HashIndexTree}.
 */
public class HashIndexTreeInnerIo extends BplusInnerIo<HashIndexRowKey> implements HashIndexTreeIo {
    /** I/O versions for each inline size up to the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}. */
    public static final List<IoVersions<HashIndexTreeInnerIo>> VERSIONS = IntStream.rangeClosed(0, MAX_BINARY_TUPLE_INLINE_SIZE)
            .mapToObj(inlineSize -> new IoVersions<>(new HashIndexTreeInnerIo(1, inlineSize)))
            .collect(toUnmodifiableList());

    /**
     * Constructor.
     *
     * @param ver Page format version.
     * @param inlineSize Inline size in bytes.
     */
    private HashIndexTreeInnerIo(int ver, int inlineSize) {
        super(T_HASH_INDEX_INNER_IO_START + inlineSize, ver, true, inlineSize + ITEM_SIZE_WITHOUT_COLUMNS);
    }

    @Override
    public void store(long dstPageAddr, int dstIdx, BplusIo<HashIndexRowKey> srcIo, long srcPageAddr, int srcIdx) {
        HashIndexTreeIo.super.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
    }

    @Override
    public void storeByOffset(long pageAddr, int off, HashIndexRowKey row) {
        HashIndexTreeIo.super.storeByOffset(pageAddr, off, row);
    }

    @Override
    public HashIndexRowKey getLookupRow(BplusTree<HashIndexRowKey, ?> tree, long pageAddr, int idx) throws IgniteInternalCheckedException {
        HashIndexTree hashIndexTree = (HashIndexTree) tree;

        return getRow(hashIndexTree.dataPageReader(), hashIndexTree.partitionId(), pageAddr, idx);
    }
}
