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

package org.apache.ignite.internal.pagememory.io;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;

/**
 * Data pages IO.
 *
 * <p>Rows in a data page are organized into two arrays growing toward each other: items table and row data.
 *
 * <p>Items table contains direct or indirect items which locate a row within the page. Items table is stored at the beginning of a page.
 * Each item has an item ID which serves as an external item reference (see {@link PageIdUtils#link(long, int)}) and can be either direct or
 * indirect. The size of any item in the items table is 2 bytes. ID of a direct item is always the same as its index in items table so it is
 * not stored in the item itself. ID of an indirect item may differ from its index (see example below) so it is stored it the item along
 * with ID (index) of direct item.
 *
 * <p>Direct and indirect items are always placed in the items table in such a way that direct items are stored first, and indirect items
 * are always stored after direct items. A data page explicitly stores both direct and indirect items count
 * (see {@link #getDirectCount(long)} and {@link #getIndirectCount(long)}), so that the item type can be easily determined: items with
 * indexes {@code [0, directCnt)} are always direct and items with indexes {@code [directCnt, directCnt + indirectCnt)} are always indirect.
 * Having both direct and indirect items in a page allows page defragmentation without breaking external links. Total number of rows stored
 * in a page is equal to the number of direct items.
 *
 * <p>The row data is stored at the end of the page; newer rows are stored closer to the end of the items table.
 *
 * <h3>Direct Items</h3>
 * Direct items refer a stored row directly by offset in the page:
 * <pre>
 *     +-----------------------------------------------------------------------------+
 *     | Direct Items             ..... (rows data)                                  |
 *     | (4000), (3800), (3600)   ..... row_2_cccc  row_1_bbbb   row_0_aaaa          |
 *     |  |       |       |_____________^           ^            ^                   |
 *     |  |       |_________________________________|            |                   |
 *     |  |______________________________________________________|                   |
 *     | directCnt: 3                                                                |
 *     | indirectCnt: 0                                                              |
 *     +-----------------------------------------------------------------------------+
 * </pre>
 * Direct item ID always matches it's index in the items table. The value of a direct item in the table is an offset of the row data within
 * the page.
 * <h3>Indirect Items</h3>
 * An indirect item explicitly stores the indirect item ID (1 byte) and the index of the direct item it refers to (1 byte). The referred
 * direct item (referrent) stores the actual offset of the row in the data page:
 * <pre>
 *     +-----------------------------------------------------------------------------+
 *     |  Direct Items .. Indirect items .... (rows data)                            |
 *     |   ____________________                                                      |
 *     |  ⌄                    |                                                     |
 *     | (3600), (3800), (3 -> 0)        ....   row_2_cccc  row_1_bbbb               |
 *     |  |       |_____________________________^___________^                        |
 *     |  |_____________________________________|                                    |
 *     | directCnt: 2                                                                |
 *     | indirectCount: 1                                                            |
 *     +-----------------------------------------------------------------------------+
 * </pre>
 * An indirect item can only be created as a result of row deletion. Note that indirect item ID does not necessarily match the item index in
 * the items table, however, indirect item IDs are always stored in sorted order by construction. In the picture above, the page contains
 * two rows which are referred by two items:
 * <ul>
 *     <li>{@code 1} is a direct item which is stored at index 1 in the items table</li>
 *     <li>{@code 3} is an indirect item which is stored at index 2 in the items table and refers to the direct
 *     item {@code 0}</li>
 * </ul>
 *
 * <h2>Items insertion and deletion</h2>
 *
 * <p>When rows are added to an empty page or a page with only direct items, the items table grows naturally:
 * <pre>
 *     +---------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 |
 *     +---------------------------------------------------------------+
 *     | Item        |  a |  b |  c |  d |  e |  f |  g |  h |  i |  j |
 *     +---------------------------------------------------------------+
 * </pre>
 *
 * <p>When an item is removed, the items table is modified in such a way, that:
 * <ul>
 *     <li>Item of deleted row is modified to point to the row of the last direct item</li>
 *     <li>The last direct item is converted to an indirect item, pointing to the deleted item</li>
 * </ul>
 *
 * <pre>
 *     remove(itemId=07)
 *     +-----------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 |         09 |
 *     +-----------------------------------------------------------------------+
 *     | Item        |  a |  b |  c |  d |  e |  f |  g |  j |  i | (09 -> 07) |
 *     +-----------------------------------------------------------------------+
 *
 *      remove(itemId=02)
 *     +--------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 |          08 |         09 |
 *     +--------------------------------------------------------------------------------+
 *     | Item        |  a |  b |  i |  d |  e |  f |  g |  j |  (08 -> 02) | (09 -> 07) |
 *     +--------------------------------------------------------------------------------+
 * </pre>
 *
 * <p>If the last direct item is already referred by an indirect item, that indirect item is updated and
 * all indirect items are shifted to the left by 1 effectively dropping last direct item:
 * <pre>
 *      remove(itemId=00)
 *     +--------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 |          07 |         08 | 09 |
 *     +--------------------------------------------------------------------------------+
 *     | Item        |  j |  b |  i |  d |  e |  f |  g |  (08 -> 02) | (09 -> 00) |    |
 *     +--------------------------------------------------------------------------------+
 * </pre>
 *
 * <p>When adding a row to a page with indirect items, the item is added to the end of direct items in the table and
 * indirect items are shifted to the right:
 * <pre>
 *      add(k)
 *     +-------------------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 |         08 |         09 |
 *     +-------------------------------------------------------------------------------+
 *     | Item        |  j |  b |  i |  d |  e |  f |  g |  k | (08 -> 02) | (09 -> 00) |
 *     +-------------------------------------------------------------------------------+
 * </pre>
 *
 * <p>If during an insert a newly added direct item ID matches with an existing indirect item ID, the corresponding
 * indirect item is converted to a direct item, and the row being inserted is represented by a direct item
 * with the referrent ID:
 * <pre>
 *      add(l)
 *     +-----------------------------------------------------------------------+
 *     | Table index | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 |         09 |
 *     +-----------------------------------------------------------------------+
 *     | Item        |  j |  b |  l |  d |  e |  f |  g |  k |  i | (09 -> 00) |
 *     +-----------------------------------------------------------------------+
 * </pre>
 */
public class DataPageIo extends PageIo {
    /** Data page IO type. */
    private static final short T_DATA_PAGE_IO = 1000;

    private static final int SHOW_ITEM = 0b0001;

    private static final int SHOW_PAYLOAD_LEN = 0b0010;

    private static final int SHOW_LINK = 0b0100;

    private static final int FREE_LIST_PAGE_ID_OFF = COMMON_HEADER_END;

    private static final int FREE_SPACE_OFF = FREE_LIST_PAGE_ID_OFF + 8;

    private static final int DIRECT_CNT_OFF = FREE_SPACE_OFF + 2;

    private static final int INDIRECT_CNT_OFF = DIRECT_CNT_OFF + 1;

    private static final int FIRST_ENTRY_OFF = INDIRECT_CNT_OFF + 1;

    public static final int ITEMS_OFF = FIRST_ENTRY_OFF + 2;

    private static final int ITEM_SIZE = 2;

    private static final int PAYLOAD_LEN_SIZE = 2;

    private static final int LINK_SIZE = 8;

    private static final int FRAGMENTED_FLAG = 0b10000000_00000000;

    /** Minimum page overhead in bytes. */
    public static final int MIN_DATA_PAGE_OVERHEAD = ITEMS_OFF + ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

    /** Special index value that signals that the given itemId does not exist. */
    private static final int NON_EXISTENT_ITEM_INDEX = -1;

    /** Special offset value that signals that the given itemId does not exist. */
    private static final int NON_EXISTENT_ITEM_OFFSET = -1;

    /** I/O versions. */
    public static final IoVersions<DataPageIo> VERSIONS = new IoVersions<>(new DataPageIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected DataPageIo(int ver) {
        super(T_DATA_PAGE_IO, ver, FLAG_DATA);
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setEmptyPage(pageAddr, pageSize);
        setFreeListPageId(pageAddr, 0L);
    }

    /**
     * Resets the content of the page.
     *
     * @param pageAddr Page address.
     * @param pageSize Page size.
     */
    private void setEmptyPage(long pageAddr, int pageSize) {
        setDirectCount(pageAddr, 0);
        setIndirectCount(pageAddr, 0);
        setFirstEntryOffset(pageAddr, pageSize, pageSize);
        setRealFreeSpace(pageAddr, pageSize - ITEMS_OFF, pageSize);
    }

    /**
     * Writes free list page id.
     *
     * @param pageAddr Page address.
     * @param freeListPageId Free list page ID.
     */
    public void setFreeListPageId(long pageAddr, long freeListPageId) {
        assertPageType(pageAddr);

        PageUtils.putLong(pageAddr, FREE_LIST_PAGE_ID_OFF, freeListPageId);
    }

    /**
     * Returns Free list page ID.
     *
     * @param pageAddr Page address.
     * @return Free list page ID.
     */
    public long getFreeListPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, FREE_LIST_PAGE_ID_OFF);
    }

    /**
     * Returns data page entry size.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param show What elements of data page entry to show in the result.
     */
    private int getPageEntrySize(long pageAddr, int dataOff, int show) {
        int payloadLen = PageUtils.getShort(pageAddr, dataOff) & 0xFFFF;

        if ((payloadLen & FRAGMENTED_FLAG) != 0) {
            payloadLen &= ~FRAGMENTED_FLAG; // We are fragmented and have a link.
        } else {
            show &= ~SHOW_LINK; // We are not fragmented, never have a link.
        }

        return getPageEntrySize(payloadLen, show);
    }

    /**
     * Returns data page entry size.
     *
     * @param payloadLen Length of the payload, may be a full data row or a row fragment length.
     * @param show What elements of data page entry to show in the result.
     */
    private int getPageEntrySize(int payloadLen, int show) {
        assert payloadLen > 0 : payloadLen;

        int res = payloadLen;

        if ((show & SHOW_LINK) != 0) {
            res += LINK_SIZE;
        }

        if ((show & SHOW_ITEM) != 0) {
            res += ITEM_SIZE;
        }

        if ((show & SHOW_PAYLOAD_LEN) != 0) {
            res += PAYLOAD_LEN_SIZE;
        }

        return res;
    }

    /**
     * Writes entry data offset.
     *
     * @param pageAddr Page address.
     * @param dataOff Entry data offset.
     * @param pageSize Page size.
     */
    private void setFirstEntryOffset(long pageAddr, int dataOff, int pageSize) {
        assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff <= pageSize : dataOff;
        assertPageType(pageAddr);

        PageUtils.putShort(pageAddr, FIRST_ENTRY_OFF, (short) dataOff);
    }

    /**
     * Returns entry data offset.
     *
     * @param pageAddr Page address.
     */
    private int getFirstEntryOffset(long pageAddr) {
        return PageUtils.getShort(pageAddr, FIRST_ENTRY_OFF) & 0xFFFF;
    }

    /**
     * Writes free space refers to a "max row size (without any data page specific overhead) which is guaranteed to fit into this data
     * page".
     *
     * @param pageAddr Page address.
     * @param freeSpace Free space.
     * @param pageSize Page size.
     */
    private void setRealFreeSpace(long pageAddr, int freeSpace, int pageSize) {
        assert freeSpace == actualFreeSpace(pageAddr, pageSize) : freeSpace + " != " + actualFreeSpace(pageAddr, pageSize);
        assertPageType(pageAddr);

        PageUtils.putShort(pageAddr, FREE_SPACE_OFF, (short) freeSpace);
    }

    /**
     * Returns free space refers to a "max row size (without any data page specific overhead) which is guaranteed to fit into this data
     * page".
     *
     * @param pageAddr Page address.
     */
    public int getFreeSpace(long pageAddr) {
        if (getFreeItemSlots(pageAddr) == 0) {
            return 0;
        }

        int freeSpace = getRealFreeSpace(pageAddr);

        // We reserve size here because of getFreeSpace() method semantics (see method javadoc).
        // It means that we must be able to accommodate a row of size which is equal to getFreeSpace(),
        // plus we will have data page overhead: header of the page as well as item, payload length and
        // possibly a link to the next row fragment.
        freeSpace -= ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

        return Math.max(freeSpace, 0);
    }

    /**
     * Returns {@code true} If there is no useful data in this page.
     *
     * @param pageAddr Page address.
     */
    public boolean isEmpty(long pageAddr) {
        return getDirectCount(pageAddr) == 0;
    }

    /**
     * Equivalent for {@link #actualFreeSpace(long, int)} but reads saved value.
     *
     * @param pageAddr Page address.
     * @return Free space.
     */
    public int getRealFreeSpace(long pageAddr) {
        return PageUtils.getShort(pageAddr, FREE_SPACE_OFF);
    }

    /**
     * Writes direct count.
     *
     * @param pageAddr Page address.
     * @param cnt Direct count.
     */
    private void setDirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;
        assertPageType(pageAddr);

        PageUtils.putByte(pageAddr, DIRECT_CNT_OFF, (byte) cnt);
    }

    /**
     * Returns direct count.
     *
     * @param pageAddr Page address.
     */
    public int getDirectCount(long pageAddr) {
        return PageUtils.getByte(pageAddr, DIRECT_CNT_OFF) & 0xFF;
    }

    /**
     * Writes inderect count.
     *
     * @param pageAddr Page address.
     * @param cnt Indirect count.
     */
    private void setIndirectCount(long pageAddr, int cnt) {
        assert checkCount(cnt) : cnt;

        PageUtils.putByte(pageAddr, INDIRECT_CNT_OFF, (byte) cnt);
    }

    /**
     * Returns {@code true} If the index is valid.
     *
     * @param idx Index.
     */
    protected boolean checkIndex(int idx) {
        return idx >= 0 && idx < 0xFF;
    }

    /**
     * Returns {@code true} If the counter fits 1 byte.
     *
     * @param cnt Counter value.
     */
    private boolean checkCount(int cnt) {
        return cnt >= 0 && cnt <= 0xFF;
    }

    /**
     * Returns indirect count.
     *
     * @param pageAddr Page address.
     */
    private int getIndirectCount(long pageAddr) {
        return PageUtils.getByte(pageAddr, INDIRECT_CNT_OFF) & 0xFF;
    }

    /**
     * Returns number of free entry slots.
     *
     * @param pageAddr Page address.
     */
    private int getFreeItemSlots(long pageAddr) {
        return 0xFF - getDirectCount(pageAddr);
    }

    /**
     * Finds index of indirect item.
     *
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Found index of indirect item.
     */
    private int findIndirectItemIndex(long pageAddr, int itemId, int directCnt, int indirectCnt) {
        int maybeOffset = findIndirectItemIndexOrNotFoundMarker(pageAddr, itemId, directCnt, indirectCnt);

        if (maybeOffset == NON_EXISTENT_ITEM_INDEX) {
            throw new IllegalStateException("Item not found: " + itemId);
        }

        return maybeOffset;
    }

    private int findIndirectItemIndexOrNotFoundMarker(long pageAddr, int itemId, int directCnt, int indirectCnt) {
        int low = directCnt;
        int high = directCnt + indirectCnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = Integer.compare(itemId(getItem(pageAddr, mid)), itemId);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // found
            }
        }

        return NON_EXISTENT_ITEM_INDEX;
    }

    /**
     * Returns string representation of page.
     *
     * @param pageAddr Page address.
     * @param pageSize Page size.
     */
    private String printPageLayout(long pageAddr, int pageSize) {
        IgniteStringBuilder b = new IgniteStringBuilder();

        printPageLayout(pageAddr, pageSize, b);

        return b.toString();
    }

    /**
     * Prints page layout to string builder.
     *
     * @param pageAddr Page address.
     * @param pageSize Page size.
     * @param b String builder.
     */
    protected void printPageLayout(long pageAddr, int pageSize, IgniteStringBuilder b) {
        int directCnt = getDirectCount(pageAddr);
        int indirectCnt = getIndirectCount(pageAddr);
        int free = getRealFreeSpace(pageAddr);

        boolean valid = directCnt >= indirectCnt;

        b.appendHex(getPageId(pageAddr)).app(" [");

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            if (i != 0) {
                b.app(", ");
            }

            short item = getItem(pageAddr, i);

            if (item < ITEMS_OFF || item >= pageSize) {
                valid = false;
            }

            entriesSize += getPageEntrySize(pageAddr, item, SHOW_PAYLOAD_LEN | SHOW_LINK);

            b.app(item);
        }

        b.app("][");

        Collection<Integer> set = new HashSet<>();

        for (int i = directCnt; i < directCnt + indirectCnt; i++) {
            if (i != directCnt) {
                b.app(", ");
            }

            short item = getItem(pageAddr, i);

            int itemId = itemId(item);
            int directIdx = directItemIndex(item);

            if (!set.add(directIdx) || !set.add(itemId)) {
                valid = false;
            }

            assert indirectItem(itemId, directIdx) == item;

            if (itemId < directCnt || directIdx < 0 || directIdx >= directCnt) {
                valid = false;
            }

            if (i > directCnt && itemId(getItem(pageAddr, i - 1)) >= itemId) {
                valid = false;
            }

            b.app(itemId).app('^').app(directIdx);
        }

        b.app("][free=").app(free);

        int actualFree = pageSize - ITEMS_OFF - (entriesSize + (directCnt + indirectCnt) * ITEM_SIZE);

        if (free != actualFree) {
            b.app(", actualFree=").app(actualFree);

            valid = false;
        } else {
            b.app("]");
        }

        assert valid : b.toString();
    }

    /**
     * Returns data entry offset in bytes.
     *
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param pageSize Page size.
     */
    protected int getDataOffset(long pageAddr, int itemId, int pageSize) {
        assert checkIndex(itemId) : itemId;

        int directCnt = getDirectCount(pageAddr);

        assert directCnt > 0 : "itemId=" + itemId + ", directCnt=" + directCnt + ", page=" + printPageLayout(pageAddr, pageSize);

        final int directItemId;
        if (itemId >= directCnt) { // Need to do indirect lookup.
            int indirectCnt = getIndirectCount(pageAddr);

            // Must have indirect items here.
            assert indirectCnt > 0 : "itemId=" + itemId + ", directCnt=" + directCnt + ", indirectCnt=" + indirectCnt
                    + ", page=" + printPageLayout(pageAddr, pageSize);

            directItemId = resolveDirectItemIdFromIndirectItemId(pageAddr, itemId, directCnt, indirectCnt);
        } else {
            directItemId = itemId;
        }

        return directItemToOffset(getItem(pageAddr, directItemId));
    }

    private int resolveDirectItemIdFromIndirectItemId(long pageAddr, int itemId, int directCnt, int indirectCnt) {
        int indirectItemIdx = findIndirectItemIndex(pageAddr, itemId, directCnt, indirectCnt);

        assert indirectItemIdx >= directCnt : indirectItemIdx + " " + directCnt;
        assert indirectItemIdx < directCnt + indirectCnt : indirectItemIdx + " " + directCnt + " " + indirectCnt;

        int directItemId = directItemIndex(getItem(pageAddr, indirectItemIdx));

        assert directItemId >= 0 && directItemId < directCnt : directItemId + " " + directCnt + " " + indirectCnt; // Direct item.
        return directItemId;
    }

    protected int getDataOffsetOrNotFoundMarker(long pageAddr, int itemId, int pageSize) {
        assert checkIndex(itemId) : itemId;

        int directCnt = getDirectCount(pageAddr);

        if (directCnt <= 0) {
            return NON_EXISTENT_ITEM_OFFSET;
        }

        int indirectCnt = getIndirectCount(pageAddr);

        final int directItemId;
        if (itemId >= directCnt) { // Need to do indirect lookup.
            if (indirectCnt <= 0) {
                return NON_EXISTENT_ITEM_OFFSET;
            }

            int indirectItemIdx = findIndirectItemIndexOrNotFoundMarker(pageAddr, itemId, directCnt, indirectCnt);
            if (indirectItemIdx == NON_EXISTENT_ITEM_INDEX) {
                return NON_EXISTENT_ITEM_OFFSET;
            }

            directItemId = resolveDirectItemIdFromIndirectItemId(pageAddr, itemId, directCnt, indirectCnt);
        } else {
            // it is a direct item

            if (anyIndirectItemPointsAtDirectItem(pageAddr, itemId, directCnt, indirectCnt)) {
                return NON_EXISTENT_ITEM_OFFSET;
            }

            directItemId = itemId;
        }

        return directItemToOffset(getItem(pageAddr, directItemId));
    }

    private boolean anyIndirectItemPointsAtDirectItem(long pageAddr, int itemId, int directCnt, int indirectCnt) {
        int maybeIndirectItemIdx = findIndirectItemIndexOrNotFoundMarker(pageAddr, itemId, directCnt, indirectCnt);
        return maybeIndirectItemIdx != NON_EXISTENT_ITEM_INDEX;
    }

    /**
     * Returns Link to the next entry fragment or 0 if no fragments left or if entry is not fragmented.
     *
     * @param pageAddr Page address.
     * @param dataOff Points to the entry start.
     */
    private long getNextFragmentLink(long pageAddr, int dataOff) {
        assert isFragmented(pageAddr, dataOff);

        return PageUtils.getLong(pageAddr, dataOff + PAYLOAD_LEN_SIZE);
    }

    /**
     * Returns {@code true} If the data row is fragmented across multiple pages.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     */
    protected boolean isFragmented(long pageAddr, int dataOff) {
        return (PageUtils.getShort(pageAddr, dataOff) & FRAGMENTED_FLAG) != 0;
    }

    /**
     * Sets position to start of actual fragment data and limit to it's end.
     *
     * @param pageAddr Page address.
     * @param itemId Item to position on.
     * @param pageSize Page size.
     * @return {@link DataPagePayload} object.
     */
    public DataPagePayload readPayload(final long pageAddr, final int itemId, final int pageSize) {
        int dataOff = getDataOffset(pageAddr, itemId, pageSize);

        boolean fragmented = isFragmented(pageAddr, dataOff);
        long nextLink = fragmented ? getNextFragmentLink(pageAddr, dataOff) : 0;
        int payloadSize = getPageEntrySize(pageAddr, dataOff, 0);

        return new DataPagePayload(dataOff + PAYLOAD_LEN_SIZE + (fragmented ? LINK_SIZE : 0),
                payloadSize,
                nextLink);
    }

    /**
     * Returns {@code true} iff an item with given ID exists in a page at the given address.
     *
     * @param pageAddr address where page data begins in memory
     * @param itemId   item ID to check
     * @param pageSize size of the page in bytes
     * @return {@code true} iff an item with given ID exists in a page at the given address
     */
    public boolean itemExists(long pageAddr, int itemId, final int pageSize) {
        return getDataOffsetOrNotFoundMarker(pageAddr, itemId, pageSize) != NON_EXISTENT_ITEM_OFFSET;
    }

    /**
     * Returns Offset to start of actual fragment data.
     *
     * @param pageAddr Page address.
     * @param itemId Item to position on.
     * @param pageSize Page size.
     * @param reqLen Required payload length.
     */
    public int getPayloadOffset(final long pageAddr, final int itemId, final int pageSize, int reqLen) {
        int dataOff = getDataOffset(pageAddr, itemId, pageSize);

        int payloadSize = getPageEntrySize(pageAddr, dataOff, 0);

        assert payloadSize >= reqLen : payloadSize;

        return dataOff + PAYLOAD_LEN_SIZE + (isFragmented(pageAddr, dataOff) ? LINK_SIZE : 0);
    }

    /**
     * Returns item.
     *
     * @param pageAddr Page address.
     * @param idx Item index.
     */
    private short getItem(long pageAddr, int idx) {
        return PageUtils.getShort(pageAddr, itemOffset(idx));
    }

    /**
     * Writes item.
     *
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param item Item.
     */
    private void setItem(long pageAddr, int idx, short item) {
        assertPageType(pageAddr);

        PageUtils.putShort(pageAddr, itemOffset(idx), item);
    }

    /**
     * Returns offset in buffer.
     *
     * @param idx Index of the item.
     */
    private int itemOffset(int idx) {
        assert checkIndex(idx) : idx;

        return ITEMS_OFF + idx * ITEM_SIZE;
    }

    /**
     * Returns offset of an entry payload inside of the page.
     *
     * @param directItem Direct item.
     */
    private int directItemToOffset(short directItem) {
        return directItem & 0xFFFF;
    }

    /**
     * Returns direct item.
     *
     * @param dataOff Data offset.
     */
    private short directItemFromOffset(int dataOff) {
        assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff < Short.MAX_VALUE : dataOff;

        return (short) dataOff;
    }

    /**
     * Returns index of corresponding direct item.
     *
     * @param indirectItem Indirect item.
     */
    private int directItemIndex(short indirectItem) {
        return indirectItem & 0xFF;
    }

    /**
     * Returns fixed item ID (the index used for referencing an entry from the outside).
     *
     * @param indirectItem Indirect item.
     */
    private int itemId(short indirectItem) {
        return (indirectItem & 0xFFFF) >>> 8;
    }

    /**
     * Returns indirect item.
     *
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directItemIdx Index of corresponding direct item.
     */
    private short indirectItem(int itemId, int directItemIdx) {
        assert checkIndex(itemId) : itemId;
        assert checkIndex(directItemIdx) : directItemIdx;

        return (short) ((itemId << 8) | directItemIdx);
    }

    /**
     * Move the last direct item to the free slot and reference it with indirect item on the same place.
     *
     * @param pageAddr Page address.
     * @param freeDirectIdx Free slot.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If the last direct item already had corresponding indirect item.
     */
    private boolean moveLastItem(long pageAddr, int freeDirectIdx, int directCnt, int indirectCnt) {
        int lastIndirectId = findIndirectIndexForLastDirect(pageAddr, directCnt, indirectCnt);

        int lastItemId = directCnt - 1;

        assert lastItemId != freeDirectIdx;

        short indirectItem = indirectItem(lastItemId, freeDirectIdx);

        assert itemId(indirectItem) == lastItemId && directItemIndex(indirectItem) == freeDirectIdx;

        setItem(pageAddr, freeDirectIdx, getItem(pageAddr, lastItemId));
        setItem(pageAddr, lastItemId, indirectItem);

        assert getItem(pageAddr, lastItemId) == indirectItem;

        if (lastIndirectId != -1) { // Fix pointer to direct item.
            setItem(pageAddr, lastIndirectId, indirectItem(itemId(getItem(pageAddr, lastIndirectId)), freeDirectIdx));

            return true;
        }

        return false;
    }

    /**
     * Returns index of indirect item for the last direct item.
     *
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     */
    private int findIndirectIndexForLastDirect(long pageAddr, int directCnt, int indirectCnt) {
        int lastDirectId = directCnt - 1;

        for (int i = directCnt, end = directCnt + indirectCnt; i < end; i++) {
            short item = getItem(pageAddr, i);

            if (directItemIndex(item) == lastDirectId) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Removes a row.
     *
     * @param pageAddr Page address.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param pageSize Page size.
     * @return Next link for fragmented entries or {@code 0} if none.
     * @throws IgniteInternalCheckedException If failed.
     */
    public long removeRow(long pageAddr, int itemId, int pageSize) throws IgniteInternalCheckedException {
        assert checkIndex(itemId) : itemId;
        assertPageType(pageAddr);

        final int dataOff = getDataOffset(pageAddr, itemId, pageSize);
        final long nextLink = isFragmented(pageAddr, dataOff) ? getNextFragmentLink(pageAddr, dataOff) : 0;

        // Record original counts to calculate delta in free space in the end of remove.
        final int directCnt = getDirectCount(pageAddr);
        final int indirectCnt = getIndirectCount(pageAddr);

        int curIndirectCnt = indirectCnt;

        assert directCnt > 0 : directCnt; // Direct count always represents overall number of live items.

        // Remove the last item on the page.
        if (directCnt == 1) {
            assert (indirectCnt == 0 && itemId == 0)
                    || (indirectCnt == 1 && itemId == itemId(getItem(pageAddr, 1))) : itemId;

            setEmptyPage(pageAddr, pageSize);
        } else {
            // Get the entry size before the actual remove.
            int rmvEntrySize = getPageEntrySize(pageAddr, dataOff, SHOW_PAYLOAD_LEN | SHOW_LINK);

            int indirectId = 0;

            if (itemId >= directCnt) { // Need to remove indirect item.
                assert indirectCnt > 0;

                indirectId = findIndirectItemIndex(pageAddr, itemId, directCnt, indirectCnt);

                assert indirectId >= directCnt;

                itemId = directItemIndex(getItem(pageAddr, indirectId));

                assert itemId < directCnt;
            }

            boolean dropLast = true;

            if (itemId + 1 < directCnt) {
                // It is not the last direct item.
                dropLast = moveLastItem(pageAddr, itemId, directCnt, indirectCnt);
            }

            if (indirectId == 0) {
                // For the last direct item with no indirect item.
                if (dropLast) {
                    moveItems(pageAddr, directCnt, indirectCnt, -1, pageSize);
                } else {
                    curIndirectCnt++;
                }
            } else {
                if (dropLast) {
                    moveItems(pageAddr, directCnt, indirectId - directCnt, -1, pageSize);
                }

                moveItems(pageAddr, indirectId + 1, directCnt + indirectCnt - indirectId - 1, dropLast ? -2 : -1, pageSize);

                if (dropLast) {
                    curIndirectCnt--;
                }
            }

            setIndirectCount(pageAddr, curIndirectCnt);
            setDirectCount(pageAddr, directCnt - 1);

            assert getIndirectCount(pageAddr) <= getDirectCount(pageAddr);

            // Increase free space.
            setRealFreeSpace(
                    pageAddr,
                    getRealFreeSpace(pageAddr) + rmvEntrySize
                            + ITEM_SIZE * (directCnt - getDirectCount(pageAddr) + indirectCnt - getIndirectCount(pageAddr)),
                    pageSize
            );
        }

        return nextLink;
    }

    /**
     * Moves items.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     * @param cnt Count.
     * @param step Step.
     * @param pageSize Page size.
     */
    private void moveItems(long pageAddr, int idx, int cnt, int step, int pageSize) {
        assert cnt >= 0 : cnt;

        if (cnt != 0) {
            moveBytes(pageAddr, itemOffset(idx), cnt * ITEM_SIZE, step * ITEM_SIZE, pageSize);
        }
    }

    /**
     * Returns {@code true} If there is enough space for the entry.
     *
     * @param newEntryFullSize New entry full size (with item, length and link).
     * @param firstEntryOff First entry data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     */
    private boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
        return ITEMS_OFF + ITEM_SIZE * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
    }

    /**
     * Adds row to this data page and sets respective link to the given row object.
     *
     * @param pageAddr Page address.
     * @param row Data row.
     * @param rowSize Row size.
     * @param pageSize Page size.
     */
    public void addRow(
            final long pageId,
            final long pageAddr,
            Storable row,
            final int rowSize,
            final int pageSize
    ) {
        assert rowSize <= getFreeSpace(pageAddr) : "can't call addRow if not enough space for the whole row";
        assert rowSize <= 0xFFFF : "Row size is too big: " + rowSize;
        assertPageType(pageAddr);

        int fullEntrySize = getPageEntrySize(rowSize, SHOW_PAYLOAD_LEN | SHOW_ITEM);

        int directCnt = getDirectCount(pageAddr);
        int indirectCnt = getIndirectCount(pageAddr);

        int dataOff = getDataOffsetForWrite(pageAddr, fullEntrySize, directCnt, indirectCnt, pageSize);

        row.writeRowData(pageAddr, dataOff, rowSize, true);

        int itemId = addItem(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageSize);

        setLinkByPageId(row, pageId, itemId);
    }

    /**
     * Compacts a page if needed.
     *
     * @param pageAddr Page address.
     * @param entryFullSize New entry full size (with item, length and link).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff First entry offset.
     * @param pageSize Page size.
     * @return First entry offset after compaction.
     */
    private int compactIfNeed(
            final long pageAddr,
            final int entryFullSize,
            final int directCnt,
            final int indirectCnt,
            int dataOff,
            int pageSize
    ) {
        assertPageType(pageAddr);

        if (!isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt)) {
            dataOff = compactDataEntries(pageAddr, directCnt, pageSize);

            assert isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt);
        }

        return dataOff;
    }

    /**
     * Put item reference on entry.
     *
     * @param pageAddr Page address.
     * @param fullEntrySize Full entry size (with link, payload size and item).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff Data offset.
     * @param pageSize Page size.
     * @return Item ID.
     */
    private int addItem(final long pageAddr,
            final int fullEntrySize,
            final int directCnt,
            final int indirectCnt,
            final int dataOff,
            final int pageSize) {
        setFirstEntryOffset(pageAddr, dataOff, pageSize);

        int itemId = insertItem(pageAddr, dataOff, directCnt, indirectCnt, pageSize);

        assert checkIndex(itemId) : itemId;
        assert getIndirectCount(pageAddr) <= getDirectCount(pageAddr);

        // Update free space. If number of indirect items changed, then we were able to reuse an item slot.
        setRealFreeSpace(pageAddr,
                getRealFreeSpace(pageAddr) - fullEntrySize + (getIndirectCount(pageAddr) != indirectCnt ? ITEM_SIZE : 0),
                pageSize);

        return itemId;
    }

    /**
     * Returns offset in the buffer where the entry must be written.
     *
     * @param pageAddr Page address.
     * @param fullEntrySize Full entry size.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageSize Page size.
     */
    private int getDataOffsetForWrite(long pageAddr, int fullEntrySize, int directCnt, int indirectCnt, int pageSize) {
        int dataOff = getFirstEntryOffset(pageAddr);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageSize);

        // We will write data right before the first entry.
        dataOff -= fullEntrySize - ITEM_SIZE;

        return dataOff;
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param pageMem Page memory.
     * @param pageId Page ID to use to construct a link.
     * @param pageAddr Page address.
     * @param row Data row.
     * @param written Number of bytes of row size that was already written.
     * @param rowSize Row size.
     * @param pageSize Page size.
     * @return Written payload size.
     * @throws IgniteInternalCheckedException If failed.
     */
    public int addRowFragment(
            PageMemory pageMem,
            long pageId,
            long pageAddr,
            Storable row,
            int written,
            int rowSize,
            int pageSize
    ) throws IgniteInternalCheckedException {
        assertPageType(pageAddr);

        assert row != null;

        long lastLink = row.link();

        int directCnt = getDirectCount(pageAddr);
        int indirectCnt = getIndirectCount(pageAddr);

        int payloadSize = Math.min(rowSize - written, getFreeSpace(pageAddr));

        int remaining = rowSize - written - payloadSize;
        int headerSize = row.headerSize();

        // We need row header to be located entirely on the very first page in chain.
        // So we force moving it to the next page if it could not fit entirely on this page.
        if (remaining > 0 && remaining < headerSize) {
            payloadSize -= headerSize - remaining;
        }

        int fullEntrySize = getPageEntrySize(payloadSize, SHOW_PAYLOAD_LEN | SHOW_LINK | SHOW_ITEM);
        int dataOff = getDataOffsetForWrite(pageAddr, fullEntrySize, directCnt, indirectCnt, pageSize);

        ByteBuffer buf = pageMem.pageBuffer(pageAddr);

        buf.position(dataOff);

        short fragmentSize = (short) (payloadSize | FRAGMENTED_FLAG);

        buf.putShort(fragmentSize);
        buf.putLong(lastLink);

        int rowOff = rowSize - written - payloadSize;

        row.writeFragmentData(buf, rowOff, payloadSize);

        int itemId = addItem(pageAddr, fullEntrySize, directCnt, indirectCnt, dataOff, pageSize);

        if (row != null) {
            setLinkByPageId(row, pageId, itemId);
        }

        return payloadSize;
    }

    /**
     * Sets link to row.
     *
     * @param row Row to set link to.
     * @param pageId Page ID.
     * @param itemId Item ID.
     */
    private void setLinkByPageId(Storable row, long pageId, int itemId) {
        row.link(PageIdUtils.link(pageId, itemId));
    }

    /**
     * Inserts an item.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param pageSize Page size.
     * @return Item ID (insertion index).
     */
    private int insertItem(long pageAddr, int dataOff, int directCnt, int indirectCnt, int pageSize) {
        if (indirectCnt > 0) {
            // If the first indirect item is on correct place to become the last direct item, do the transition
            // and insert the new item into the free slot which was referenced by this first indirect item.
            short item = getItem(pageAddr, directCnt);

            if (itemId(item) == directCnt) {
                int directItemIdx = directItemIndex(item);

                setItem(pageAddr, directCnt, getItem(pageAddr, directItemIdx));
                setItem(pageAddr, directItemIdx, directItemFromOffset(dataOff));

                setDirectCount(pageAddr, directCnt + 1);
                setIndirectCount(pageAddr, indirectCnt - 1);

                return directItemIdx;
            }
        }

        // Move all the indirect items forward to make a free slot and insert new item at the end of direct items.
        moveItems(pageAddr, directCnt, indirectCnt, +1, pageSize);

        setItem(pageAddr, directCnt, directItemFromOffset(dataOff));

        setDirectCount(pageAddr, directCnt + 1);
        assert getDirectCount(pageAddr) == directCnt + 1;

        return directCnt; // Previous directCnt will be our itemId.
    }

    /**
     * Compacts data entries.
     *
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     * @param pageSize Page size.
     * @return New first entry offset.
     */
    private int compactDataEntries(long pageAddr, int directCnt, int pageSize) {
        assert checkCount(directCnt) : directCnt;

        int[] offs = new int[directCnt];

        for (int i = 0; i < directCnt; i++) {
            int off = directItemToOffset(getItem(pageAddr, i));

            offs[i] = (off << 8) | i; // This way we'll be able to sort by offset using Arrays.sort(...).
        }

        Arrays.sort(offs);

        // Move right all of the entries if possible to make the page as compact as possible to its tail.
        int prevOff = pageSize;

        final int start = directCnt - 1;
        int curOff = offs[start] >>> 8;
        int curEntrySize = getPageEntrySize(pageAddr, curOff, SHOW_PAYLOAD_LEN | SHOW_LINK);

        for (int i = start; i >= 0; i--) {
            assert curOff < prevOff : curOff;

            int delta = prevOff - (curOff + curEntrySize);

            int off = curOff;
            int entrySize = curEntrySize;

            if (delta != 0) { // Move right.
                assert delta > 0 : delta;

                int itemId = offs[i] & 0xFF;

                setItem(pageAddr, itemId, directItemFromOffset(curOff + delta));

                for (int j = i - 1; j >= 0; j--) {
                    int offNext = offs[j] >>> 8;
                    int nextSize = getPageEntrySize(pageAddr, offNext, SHOW_PAYLOAD_LEN | SHOW_LINK);

                    if (offNext + nextSize == off) {
                        i--;

                        off = offNext;
                        entrySize += nextSize;

                        itemId = offs[j] & 0xFF;
                        setItem(pageAddr, itemId, directItemFromOffset(offNext + delta));
                    } else {
                        curOff = offNext;
                        curEntrySize = nextSize;

                        break;
                    }
                }

                moveBytes(pageAddr, off, entrySize, delta, pageSize);

                off += delta;
            } else if (i > 0) {
                curOff = offs[i - 1] >>> 8;
                curEntrySize = getPageEntrySize(pageAddr, curOff, SHOW_PAYLOAD_LEN | SHOW_LINK);
            }

            prevOff = off;
        }

        return prevOff;
    }

    /**
     * Full-scan free space calculation procedure.
     *
     * @param pageAddr Page to scan.
     * @param pageSize Page size.
     * @return Actual free space in the buffer.
     */
    private int actualFreeSpace(long pageAddr, int pageSize) {
        int directCnt = getDirectCount(pageAddr);

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            int off = directItemToOffset(getItem(pageAddr, i));

            int entrySize = getPageEntrySize(pageAddr, off, SHOW_PAYLOAD_LEN | SHOW_LINK);

            entriesSize += entrySize;
        }

        return pageSize - entriesSize - getHeaderSizeWithItems(pageAddr, directCnt);
    }

    /**
     * Returns size of the page header including all items.
     *
     * @param pageAddr Page address.
     * @param directCnt Direct items count.
     */
    private int getHeaderSizeWithItems(long pageAddr, int directCnt) {
        return ITEMS_OFF + (directCnt + getIndirectCount(pageAddr)) * ITEM_SIZE;
    }

    /**
     * Moves bytes.
     *
     * @param addr Address.
     * @param off Offset.
     * @param cnt Count.
     * @param step Step.
     * @param pageSize Page size.
     */
    private void moveBytes(long addr, int off, int cnt, int step, int pageSize) {
        assert cnt >= 0 : cnt;
        assert step != 0 : step;
        assert off + step >= 0;
        assert off + step + cnt <= pageSize : "[off=" + off + ", step=" + step + ", cnt=" + cnt
                + ", cap=" + pageSize + ']';

        PageUtils.copyMemory(addr, off, addr, off + step, cnt);
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("DataPageIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
