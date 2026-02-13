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

package org.apache.ignite.internal.storage.pagememory.index.sorted;

import static org.apache.ignite.internal.storage.pagememory.index.InlineUtils.binaryTupleInlineSize;
import static org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeIo.ITEM_SIZE_WITHOUT_COLUMNS;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleComparator;
import org.apache.ignite.internal.schema.PartialBinaryTupleMatcher;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.sorted.comparator.JitComparator;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeInnerIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeLeafIo;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeMetaIo;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BplusTree} implementation for storing {@link SortedIndexRow}.
 */
public class SortedIndexTree extends BplusTree<SortedIndexRowKey, SortedIndexRow> {
    /** Data page reader instance to read payload from data pages. */
    private final DataPageReader dataPageReader;

    /**
     * Comparator of index columns {@link BinaryTuple}s.
     *
     * <p>Can be {@code null} only during recovery.
     */
    @Nullable
    private final BinaryTupleComparator binaryTupleComparator;

    @Nullable
    private final PartialBinaryTupleMatcher partialBinaryTupleMatcher;

    @Nullable
    private JitComparator jitComparator;

    /** Inline size in bytes. */
    private final int inlineSize;

    /**
     * Constructor used to create a new tree or restore an existing one.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param indexDescriptor Index descriptor.
     * @param jitComparator Optional optimized binary tuple comparator to be used by the tree. {@code null} if {@link BinaryTupleComparator}
     *      derived from {@code indexDescriptor} should be used instead.
     * @param initNew {@code True} if need to create and fill in special pages for working with a tree (for example, when creating
     *         it for the first time), {@code false} if not necessary (for example, when restoring a tree).
     * @throws IgniteInternalCheckedException If failed.
     */
    private SortedIndexTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            StorageSortedIndexDescriptor indexDescriptor,
            @Nullable JitComparator jitComparator,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        super("SortedIndexTree", grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList);

        this.inlineSize = initNew
                ? binaryTupleInlineSize(pageSize(), ITEM_SIZE_WITHOUT_COLUMNS, indexDescriptor)
                : readInlineSizeFromMetaIo();
        this.dataPageReader = new DataPageReader(pageMem, grpId);
        this.binaryTupleComparator = StorageUtils.binaryTupleComparator(indexDescriptor.columns());
        this.partialBinaryTupleMatcher = StorageUtils.partialBinaryTupleComparator(indexDescriptor.columns());
        this.jitComparator = jitComparator;

        init(initNew);
    }

    /**
     * Constructor used during the recovery phase to destroy an existing tree.
     *
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @throws IgniteInternalCheckedException If failed.
     */
    private SortedIndexTree(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) throws IgniteInternalCheckedException {
        super("SortedIndexTree", grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList);

        this.inlineSize = readInlineSizeFromMetaIo();
        this.dataPageReader = new DataPageReader(pageMem, grpId);
        this.binaryTupleComparator = null;
        this.partialBinaryTupleMatcher = null;
        this.jitComparator = null;

        init(false);
    }

    /**
     * Creates a new Sorted Index tree.
     */
    public static SortedIndexTree createNew(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            StorageSortedIndexDescriptor indexDescriptor,
            @Nullable JitComparator jitComparator
    ) throws IgniteInternalCheckedException {
        return new SortedIndexTree(
                grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList, indexDescriptor, jitComparator, true
        );
    }

    /**
     * Restores a Sorted Index tree that has already been created before.
     */
    public static SortedIndexTree restoreExisting(
            int grpId,
            @Nullable String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            StorageSortedIndexDescriptor indexDescriptor,
            @Nullable JitComparator jitComparator
    ) throws IgniteInternalCheckedException {
        return new SortedIndexTree(
                grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList, indexDescriptor, jitComparator, false
        );
    }

    /**
     * Restores a Sorted Index tree that will be immediately destroyed. Used during recovery.
     */
    public static SortedIndexTree restoreForDestroy(
            int grpId,
            String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) throws IgniteInternalCheckedException {
        return new SortedIndexTree(grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList);
    }

    private void init(boolean initNew) throws IgniteInternalCheckedException {
        setIos(
                SortedIndexTreeInnerIo.VERSIONS.get(inlineSize),
                SortedIndexTreeLeafIo.VERSIONS.get(inlineSize),
                SortedIndexTreeMetaIo.VERSIONS
        );

        initTree(initNew);

        if (initNew) {
            writeInlineSizeToMetaIo(inlineSize);
        }
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
    protected int compare(BplusIo<SortedIndexRowKey> io, long pageAddr, int idx, SortedIndexRowKey row)
            throws IgniteInternalCheckedException {
        SortedIndexTreeIo sortedIndexTreeIo = (SortedIndexTreeIo) io;

        if (jitComparator != null) {
            return sortedIndexTreeIo.compare(
                    dataPageReader,
                    jitComparator,
                    partId,
                    pageAddr,
                    idx,
                    row
            );
        }

        return sortedIndexTreeIo.compare(
                dataPageReader,
                getBinaryTupleComparator(),
                getPartialBinaryTupleComparator(),
                partId,
                pageAddr,
                idx,
                row
        );
    }

    @Override
    public SortedIndexRow getRow(BplusIo<SortedIndexRowKey> io, long pageAddr, int idx, Object x) throws IgniteInternalCheckedException {
        SortedIndexTreeIo sortedIndexTreeIo = (SortedIndexTreeIo) io;

        return sortedIndexTreeIo.getRow(dataPageReader, partId, pageAddr, idx);
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
                (groupId, pageId, page, pageAddr, io, arg, intArg) -> ((SortedIndexTreeMetaIo) io).getInlineSize(pageAddr),
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
                (groupId, pageId, page, pageAddr, io, arg, intArg) -> {
                    ((SortedIndexTreeMetaIo) io).setInlineSize(pageAddr, inlineSize);

                    return Boolean.TRUE;
                },
                0,
                Boolean.FALSE
        );

        assert result == Boolean.TRUE : result;
    }

    /**
     * Returns comparator of index columns {@link BinaryTuple}s.
     */
    BinaryTupleComparator getBinaryTupleComparator() {
        assert binaryTupleComparator != null : "This index tree must only be used for destruction during recovery";

        return binaryTupleComparator;
    }

    /**
     * Returns comparator of index columns {@link BinaryTuple}s.
     */
    PartialBinaryTupleMatcher getPartialBinaryTupleComparator() {
        assert partialBinaryTupleMatcher != null : "This index tree must only be used for destruction during recovery";

        return partialBinaryTupleMatcher;
    }
}
