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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.binaryTupleInlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeIo.ITEM_SIZE_WITHOUT_COLUMNS;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.io.HashIndexTreeMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storing {@link HashIndexRow}.
 */
public class HashIndexTree extends BplusTree<HashIndexRowKey, HashIndexRow> {
    /** Data page reader instance to read payload from data pages. */
    private final DataPageReader dataPageReader;

    /** Inline size in bytes. */
    private final int inlineSize;

    /**
     * Constructor used to restore an existing tree.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @throws IgniteInternalCheckedException If failed.
     */
    private HashIndexTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) throws IgniteInternalCheckedException {
        super("HashIndexTree", grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList);

        this.inlineSize = readInlineSizeFromMetaIo();
        this.dataPageReader = new DataPageReader(pageMem, grpId, statisticsHolder());

        init(false);
    }

    /**
     * Constructor used to create a new tree.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param indexDescriptor Index descriptor.
     * @throws IgniteInternalCheckedException If failed.
     */
    private HashIndexTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            StorageHashIndexDescriptor indexDescriptor
    ) throws IgniteInternalCheckedException {
        super("HashIndexTree", grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList);

        this.inlineSize = binaryTupleInlineSize(pageSize(), ITEM_SIZE_WITHOUT_COLUMNS, indexDescriptor);
        this.dataPageReader = new DataPageReader(pageMem, grpId, statisticsHolder());

        init(true);

        writeInlineSizeToMetaIo(inlineSize);
    }

    private void init(boolean initNew) throws IgniteInternalCheckedException {
        setIos(
                HashIndexTreeInnerIo.VERSIONS.get(inlineSize),
                HashIndexTreeLeafIo.VERSIONS.get(inlineSize),
                HashIndexTreeMetaIo.VERSIONS
        );

        initTree(initNew);
    }

    /**
     * Creates a new Hash Index tree.
     */
    public static HashIndexTree createNew(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            StorageHashIndexDescriptor indexDescriptor
    ) throws IgniteInternalCheckedException {
        return new HashIndexTree(grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList, indexDescriptor);
    }

    /**
     * Restores a Hash Index tree that has already been created before.
     */
    public static HashIndexTree restoreExisting(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) throws IgniteInternalCheckedException {
        return new HashIndexTree(grpId, grpName, partId, pageMem, lockLsnr, globalRmvId, metaPageId, reuseList);
    }

    /**
     * Returns a partition id.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * Returns a data page reader instance to read payload from data pages.
     */
    public DataPageReader dataPageReader() {
        return dataPageReader;
    }

    @Override
    protected int compare(BplusIo<HashIndexRowKey> io, long pageAddr, int idx, HashIndexRowKey row) throws IgniteInternalCheckedException {
        HashIndexTreeIo hashIndexTreeIo = (HashIndexTreeIo) io;

        return hashIndexTreeIo.compare(dataPageReader, partId, pageAddr, idx, row);
    }

    @Override
    public HashIndexRow getRow(BplusIo<HashIndexRowKey> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        HashIndexTreeIo hashIndexTreeIo = (HashIndexTreeIo) io;

        return hashIndexTreeIo.getRow(dataPageReader, partId, pageAddr, idx);
    }

    /**
     * Returns inline size in bytes.
     */
    public int inlineSize() {
        return inlineSize;
    }

    private int readInlineSizeFromMetaIo() throws IgniteInternalCheckedException {
        Integer inlineSize = read(
                metaPageId,
                (groupId, pageId, page, pageAddr, io, arg, intArg, statHolder) -> ((HashIndexTreeMetaIo) io).getInlineSize(pageAddr),
                null,
                0,
                -1
        );

        assert inlineSize != -1;

        return inlineSize;
    }

    private void writeInlineSizeToMetaIo(int inlineSize) throws IgniteInternalCheckedException {
        Boolean result = write(
                metaPageId,
                (groupId, pageId, page, pageAddr, io, arg, intArg, statHolder) -> {
                    ((HashIndexTreeMetaIo) io).setInlineSize(pageAddr, inlineSize);

                    return Boolean.TRUE;
                },
                0,
                Boolean.FALSE,
                statisticsHolder()
        );

        assert result == Boolean.TRUE : result;
    }
}
