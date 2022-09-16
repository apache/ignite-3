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

package org.apache.ignite.internal.pagememory.tree.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.copyMemory;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract IO routines for B+Tree inner pages.
 *
 * <p>The structure of the page is the following:
 * |ITEMS_OFF|w|A|x|B|y|C|z|
 * where capital letters are data items, lowercase letters are 8 byte page references.
 * </p>
 */
public abstract class BplusInnerIo<L> extends BplusIo<L> {
    /** Offset of the left page ID of the item. */
    private static final int SHIFT_LEFT = ITEMS_OFF;

    /** Offset of the link. */
    private static final int SHIFT_LINK = SHIFT_LEFT + 8;

    /** Offset of the right page ID of the item. */
    private final int shiftRight = SHIFT_LINK + itemSize;

    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver Page format version.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize Single item size on page.
     */
    protected BplusInnerIo(int type, int ver, boolean canGetRow, int itemSize) {
        super(type, ver, false, canGetRow, itemSize);
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxCount(long pageAddr, int pageSize) {
        return (pageSize - ITEMS_OFF - 8) / (getItemSize() + 8);
    }

    /**
     * Returns left page id for item.
     *
     * @param pageAddr Page address.
     * @param idx Index of item.
     * @return Page ID.
     */
    public final long getLeft(long pageAddr, int idx) {
        return getLong(pageAddr, (offset0(idx, SHIFT_LEFT)));
    }

    /**
     * Sets left page id for item.
     *
     * @param pageAddr Page address.
     * @param idx Index of item.
     * @param pageId Page ID.
     */
    public final void setLeft(long pageAddr, int idx, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, offset0(idx, SHIFT_LEFT), pageId);

        assert pageId == getLeft(pageAddr, idx);
    }

    /**
     * Returns right page id for item.
     *
     * @param pageAddr Page address.
     * @param idx Index of item.
     */
    public final long getRight(long pageAddr, int idx) {
        return getLong(pageAddr, offset0(idx, shiftRight));
    }

    /**
     * Returns right page id for item.
     *
     * @param pageAddr Page address.
     * @param idx Index of item.
     * @param pageId Page ID.
     */
    private void setRight(long pageAddr, int idx, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, offset0(idx, shiftRight), pageId);

        assert pageId == getRight(pageAddr, idx);
    }

    /** {@inheritDoc} */
    @Override
    public final void copyItems(
            long srcPageAddr,
            long dstPageAddr,
            int srcIdx,
            int dstIdx,
            int cnt,
            boolean cpLeft
    ) {
        assertPageType(dstPageAddr);

        assert srcIdx != dstIdx || srcPageAddr != dstPageAddr;

        cnt *= getItemSize() + 8; // From items to bytes.

        if (dstIdx > srcIdx) {
            copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt);

            if (cpLeft) {
                putLong(dstPageAddr, offset0(dstIdx, SHIFT_LEFT), getLong(srcPageAddr, (offset0(srcIdx, SHIFT_LEFT))));
            }
        } else {
            if (cpLeft) {
                putLong(dstPageAddr, offset0(dstIdx, SHIFT_LEFT), getLong(srcPageAddr, (offset0(srcIdx, SHIFT_LEFT))));
            }

            copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt);
        }
    }

    /**
     * Returns offset from byte buffer begin in bytes.
     *
     * @param idx Index of element.
     * @param shift It can be either link itself or left or right page ID.
     */
    private int offset0(int idx, int shift) {
        return shift + (8 + getItemSize()) * idx;
    }

    /** {@inheritDoc} */
    @Override
    public final int offset(int idx) {
        return offset0(idx, SHIFT_LINK);
    }

    // Methods for B+Tree logic.

    /** {@inheritDoc} */
    @Override
    public @Nullable byte[] insert(
            long pageAddr,
            int idx,
            L row,
            @Nullable byte[] rowBytes,
            long rightId,
            boolean needRowBytes
    ) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        rowBytes = super.insert(pageAddr, idx, row, rowBytes, rightId, needRowBytes);

        // Setup reference to the right page on split.
        setRight(pageAddr, idx, rightId);

        return rowBytes;
    }

    /**
     * Initializes a new root.
     *
     * @param newRootPageAddr New root page address.
     * @param newRootId New root ID.
     * @param leftChildId Left child ID.
     * @param row Moved up row.
     * @param rowBytes Bytes.
     * @param rightChildId Right child ID.
     * @param pageSize Page size.
     * @param needRowBytes If we need row bytes back.
     * @return Row bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public @Nullable byte[] initNewRoot(
            long newRootPageAddr,
            long newRootId,
            long leftChildId,
            L row,
            @Nullable byte[] rowBytes,
            long rightChildId,
            int pageSize,
            boolean needRowBytes
    ) throws IgniteInternalCheckedException {
        initNewPage(newRootPageAddr, newRootId, pageSize);

        setCount(newRootPageAddr, 1);
        setLeft(newRootPageAddr, 0, leftChildId);
        rowBytes = store(newRootPageAddr, 0, row, rowBytes, needRowBytes);
        setRight(newRootPageAddr, 0, rightChildId);

        return rowBytes;
    }
}
