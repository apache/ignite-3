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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;

import java.util.function.Consumer;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract IO routines for B+Tree pages.
 * <p/>
 * Every B+Tree page has a similar structure:
 * <pre><code>
 *     | HEADER | count | forwardId | removeId | items... |
 * </code></pre>
 * {@code HEADER} is a common structure that's present in every page. Please refer to {@link PageIo} and {@link PageIo#COMMON_HEADER_END}
 * specifically for more details.
 * <p/>
 * {@code count} ({@link #getCount(long)}) is an unsigned short value that represents a number of {@code items} in the page. What the {@code
 * item} is exactly is defined by specific implementations. Item size is defined by a {@link #itemSize} constant. Two implementations of the
 * IO handle items list differently:
 * <ul>
 *     <li>
 *         {@link BplusLeafIo} uses an array to store all items, with no gaps inbetween:
 *         <pre><code>
 * | item0 | item1 | ... | itemN-2 | itemN-1 |
 *         </code></pre>
 *     </li>
 *     <li>
 *         {@link BplusInnerIo} interlaces items arrays with links array. It looks like this:
 *         <pre><code>
 * | link0 | item0 | link1 | item1 | ... | linkN-1 | itemN-1 | linkN |
 *         </code></pre>
 *         This layout affects the way offset is calculated and the total amount of items that can be put into a single
 *         page.
 *     </li>
 * </ul>
 * {@code forwardId} ({@link #getForward(long, int)}) is a link to the forward page, please refer to {@link BplusTree} for
 * the explanation.
 * <p/>
 * {@code removeId} ({@link #getRemoveId(long)}) is a special value that's used to check tree invariants during
 * deletions. Please refer to {@link BplusTree} for better explanation.
 *
 * @see BplusTree
 */
public abstract class BplusIo<L> extends PageIo {
    /** Items count in the page offset - short. */
    private static final int CNT_OFF = COMMON_HEADER_END;

    /** Forward page ID offset - {@link PartitionlessLinks#PARTITIONLESS_LINK_SIZE_BYTES}. */
    private static final int FORWARD_OFF = CNT_OFF + Short.BYTES;

    /** Remove ID offset - long. */
    private static final int REMOVE_ID_OFF = FORWARD_OFF + PARTITIONLESS_LINK_SIZE_BYTES;

    /** Offset start storing items in a page. */
    protected static final int ITEMS_OFF = REMOVE_ID_OFF + Long.BYTES;

    /** Header size in bytes. */
    public static final int HEADER_SIZE = ITEMS_OFF;

    /**
     * Sign that we can get the full row from this page. Must always be {@code true} for leaf pages.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-16350
     */
    private final boolean canGetRow;

    /** If this is a leaf IO. */
    private final boolean leaf;

    /** All the items must be of fixed size (in bytes). */
    protected final int itemSize;

    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver Page format version.
     * @param leaf If this is a leaf IO.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize Item size in bytes.
     */
    protected BplusIo(int type, int ver, boolean leaf, boolean canGetRow, int itemSize) {
        super(type, ver, FLAG_AUX);

        assert itemSize > 0 : itemSize;
        assert canGetRow || !leaf : "leaf page always must be able to get full row";

        this.leaf = leaf;
        this.canGetRow = canGetRow;
        this.itemSize = itemSize;
    }

    /**
     * Returns item size in bytes.
     */
    public final int getItemSize() {
        return itemSize;
    }

    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setCount(pageAddr, 0);
        setForward(pageAddr, 0);
        setRemoveId(pageAddr, 0);
    }

    /**
     * Returns forward page ID.
     *
     * @param pageAddr Page address.
     * @param partId Partition ID.
     */
    public final long getForward(long pageAddr, int partId) {
        return readPartitionless(partId, pageAddr, FORWARD_OFF);
    }

    /**
     * Sets forward page ID.
     *
     * @param pageAddr Page address.
     * @param pageId Forward page ID.
     */
    public final void setForward(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        writePartitionless(pageAddr + FORWARD_OFF, pageId);

        assert getForward(pageAddr, partitionId(pageId)) == pageId;
    }

    /**
     * Returns remove ID.
     *
     * @param pageAddr Page address.
     */
    public final long getRemoveId(long pageAddr) {
        return getLong(pageAddr, REMOVE_ID_OFF);
    }

    /**
     * Sets remove ID.
     *
     * @param pageAddr Page address.
     * @param rmvId Remove ID.
     */
    public final void setRemoveId(long pageAddr, long rmvId) {
        assertPageType(pageAddr);

        putLong(pageAddr, REMOVE_ID_OFF, rmvId);

        assert getRemoveId(pageAddr) == rmvId;
    }

    /**
     * Returns items count in the page.
     *
     * @param pageAddr Page address.
     */
    public final int getCount(long pageAddr) {
        return getShort(pageAddr, CNT_OFF) & 0xFFFF;
    }

    /**
     * Sets items count in the page.
     *
     * @param pageAddr Page address.
     * @param cnt Count.
     */
    public final void setCount(long pageAddr, int cnt) {
        assertPageType(pageAddr);

        assert cnt >= 0 : cnt;

        putShort(pageAddr, CNT_OFF, (short) cnt);

        assert getCount(pageAddr) == cnt;
    }

    /**
     * Returns {@code true} if we can get the full row from this page using method {@link BplusTree#getRow(BplusIo, long, int)}.
     *
     * <p>NOTE: Must always be {@code true} for leaf pages.
     */
    public final boolean canGetRow() {
        return canGetRow;
    }

    /**
     * Returns {@code true} if it is a leaf page.
     */
    public final boolean isLeaf() {
        return leaf;
    }

    /**
     * Returns max items count.
     *
     * @param pageSize Page size without encryption overhead.
     */
    public abstract int getMaxCount(int pageSize);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     * @param row Lookup or full row.
     * @param rowBytes Row bytes.
     * @param needRowBytes If we need stored row bytes.
     * @return Stored row bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final byte @Nullable [] store(
            long pageAddr,
            int idx,
            @Nullable L row,
            byte @Nullable [] rowBytes,
            boolean needRowBytes
    ) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        int off = offset(idx);

        if (rowBytes == null) {
            storeByOffset(pageAddr, off, row);

            if (needRowBytes) {
                rowBytes = getBytes(pageAddr, off, getItemSize());
            }
        } else {
            putBytes(pageAddr, off, rowBytes);
        }

        return rowBytes;
    }

    /**
     * Store row info from the given source.
     *
     * @param dstPageAddr Destination page address.
     * @param dstIdx Destination index.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     * @throws IgniteInternalCheckedException If failed.
     */
    public abstract void store(
            long dstPageAddr,
            int dstIdx,
            BplusIo<L> srcIo,
            long srcPageAddr,
            int srcIdx
    ) throws IgniteInternalCheckedException;

    /**
     * Returns offset from byte buffer begin in bytes.
     *
     * @param idx Index of element.
     */
    public abstract int offset(int idx);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param pageAddr Page address.
     * @param off Offset in bytes.
     * @param row Lookup or full row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public abstract void storeByOffset(long pageAddr, int off, L row) throws IgniteInternalCheckedException;

    /**
     * Get lookup row.
     *
     * @param tree Tree.
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Lookup row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public abstract L getLookupRow(BplusTree<L, ?> tree, long pageAddr, int idx) throws IgniteInternalCheckedException;

    /**
     * Copy items from source page to destination page. Both pages must be of the same type and the same version.
     *
     * @param srcPageAddr Source page address.
     * @param dstPageAddr Destination page address.
     * @param srcIdx Source begin index.
     * @param dstIdx Destination begin index.
     * @param cnt Items count.
     * @param cpLeft Copy leftmost link (makes sense only for inner pages).
     * @throws IgniteInternalCheckedException If failed.
     */
    public abstract void copyItems(
            long srcPageAddr,
            long dstPageAddr,
            int srcIdx,
            int dstIdx,
            int cnt,
            boolean cpLeft
    ) throws IgniteInternalCheckedException;

    // Methods for B+Tree logic.

    /**
     * Inserts an row into the page.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     * @param row Row to insert.
     * @param rowBytes Row bytes.
     * @param rightId Page ID which will be to the right child for the inserted item.
     * @param needRowBytes If we need stored row bytes.
     * @return Row bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public byte @Nullable [] insert(
            long pageAddr,
            int idx,
            @Nullable L row,
            byte @Nullable [] rowBytes,
            long rightId,
            boolean needRowBytes
    ) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        int cnt = getCount(pageAddr);

        // Move right all the greater elements to make a free slot for a new row link.
        copyItems(pageAddr, pageAddr, idx, idx + 1, cnt - idx, false);

        setCount(pageAddr, cnt + 1);

        return store(pageAddr, idx, row, rowBytes, needRowBytes);
    }

    /**
     * Updates a forward page.
     *
     * @param pageAddr Splitting page address.
     * @param fwdId Forward page ID.
     * @param fwdPageAddr Forward page address.
     * @param mid Bisection index.
     * @param cnt Initial elements count in the page being split.
     * @param pageSize Page size.
     * @param partId Partition ID.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void splitForwardPage(
            long pageAddr,
            long fwdId,
            long fwdPageAddr,
            int mid,
            int cnt,
            int pageSize,
            int partId
    ) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        initNewPage(fwdPageAddr, fwdId, pageSize);

        cnt -= mid;

        copyItems(pageAddr, fwdPageAddr, mid, 0, cnt, true);

        setCount(fwdPageAddr, cnt);
        setForward(fwdPageAddr, getForward(pageAddr, partId));

        // Copy remove ID to make sure that if inner remove touched this page, then retry
        // will happen even for newly allocated forward page.
        setRemoveId(fwdPageAddr, getRemoveId(pageAddr));
    }

    /**
     * Updates an existing page.
     *
     * @param pageAddr Page address.
     * @param mid Bisection index.
     * @param fwdId New forward page ID.
     */
    public void splitExistingPage(long pageAddr, int mid, long fwdId) {
        assertPageType(pageAddr);

        setCount(pageAddr, mid);
        setForward(pageAddr, fwdId);
    }

    /**
     * Removes an item at its index in the page.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     * @param cnt Count.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void remove(long pageAddr, int idx, int cnt) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        cnt--;

        copyItems(pageAddr, pageAddr, idx + 1, idx, cnt - idx, false);
        setCount(pageAddr, cnt);
    }

    /**
     * Tries to merge pages.
     *
     * @param parentIo Parent IO.
     * @param parentPageAddr Parent page address.
     * @param parentIdx Split key index in parent.
     * @param leftPageAddr Left page address.
     * @param rightPageAddr Right page address.
     * @param emptyBranch We are merging an empty branch.
     * @param pageSize Page size without encryption overhead.
     * @return {@code false} If we were not able to merge.
     * @throws IgniteInternalCheckedException If failed.
     */
    public boolean merge(
            BplusIo<L> parentIo,
            long parentPageAddr,
            int parentIdx,
            long leftPageAddr,
            long rightPageAddr,
            boolean emptyBranch,
            int pageSize
    ) throws IgniteInternalCheckedException {
        assertPageType(leftPageAddr);

        int parentCnt = parentIo.getCount(parentPageAddr);
        int leftCnt = getCount(leftPageAddr);
        int rightCnt = getCount(rightPageAddr);

        int newCnt = leftCnt + rightCnt;

        // Need to move down split key in inner pages. For empty branch merge parent key will be just dropped.
        if (!isLeaf() && !emptyBranch) {
            newCnt++;
        }

        if (newCnt > getMaxCount(pageSize)) {
            assert !emptyBranch;

            return false;
        }

        setCount(leftPageAddr, newCnt);

        // Move down split key in inner pages.
        if (!isLeaf() && !emptyBranch) {
            assert parentIdx >= 0 && parentIdx < parentCnt : parentIdx; // It must be adjusted already.

            // We can be sure that we have enough free space to store split key here,
            // because we've done remove already and did not release child locks.
            store(leftPageAddr, leftCnt, parentIo, parentPageAddr, parentIdx);

            leftCnt++;
        }

        copyItems(rightPageAddr, leftPageAddr, 0, leftCnt, rightCnt, !emptyBranch);
        // Partition -1 since it won't be saved.
        setForward(leftPageAddr, getForward(rightPageAddr, -1));

        long rmvId = getRemoveId(rightPageAddr);

        // Need to have maximum remove ID.
        if (rmvId > getRemoveId(leftPageAddr)) {
            setRemoveId(leftPageAddr, rmvId);
        }

        return true;
    }

    /**
     * Puts bytes in the page.
     *
     * @param pageAddr Page address.
     * @param pos Position in page.
     * @param bytes Bytes.
     */
    private static void putBytes(long pageAddr, int pos, byte[] bytes) {
        PageUtils.putBytes(pageAddr, pos, bytes);
    }

    /**
     * Visits the page.
     *
     * @param tree The tree to which the page belongs.
     * @param pageAddr Page address.
     * @param c Consumer triggered for each element stored in the page.
     */
    public void visit(BplusTree<L, ?> tree, long pageAddr, Consumer<L> c) {
        // No-op.
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("BPlusIO [\n\tcanGetRow=").app(canGetRow)
                .app(",\n\tleaf=").app(leaf)
                .app(",\n\titemSize=").app(itemSize)
                .app(",\n\tcnt=").app(getCount(addr))
                .app(",\n\tforward=").appendHex(getForward(addr, partitionId(getPageId(addr))))
                .app(",\n\tremoveId=").appendHex(getRemoveId(addr))
                .app("\n]");
    }

    /**
     * Returns offset after the last item.
     *
     * @param pageAddr Page address.
     */
    public int getItemsEnd(long pageAddr) {
        int cnt = getCount(pageAddr);
        return offset(cnt);
    }
}
