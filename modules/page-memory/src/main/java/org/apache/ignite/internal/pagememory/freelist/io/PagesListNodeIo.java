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

package org.apache.ignite.internal.pagememory.freelist.io;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.maskPartitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.copyMemory;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;

/**
 * Pages list node IO.
 *
 * <p>TODO: https://issues.apache.org/jira/browse/IGNITE-16350 optimize: now we have slow {@link #removePage(long, long)}.
 */
public class PagesListNodeIo extends PageIo {
    /** Page IO type. */
    public static final int T_PAGE_LIST_NODE = 2;

    /** I/O versions. */
    public static final IoVersions<PagesListNodeIo> VERSIONS = new IoVersions<>(new PagesListNodeIo(1));

    private static final int PREV_PAGE_ID_OFF = COMMON_HEADER_END;

    private static final int NEXT_PAGE_ID_OFF = PREV_PAGE_ID_OFF + 8;

    private static final int CNT_OFF = NEXT_PAGE_ID_OFF + 8;

    private static final int PAGE_IDS_OFF = CNT_OFF + 2;

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected PagesListNodeIo(int ver) {
        super(T_PAGE_LIST_NODE, ver, FLAG_AUX);
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setEmpty(pageAddr);

        setPreviousId(pageAddr, 0L);
        setNextId(pageAddr, 0L);
    }

    /**
     * Resets the content of the page.
     *
     * @param pageAddr Page address.
     */
    private void setEmpty(long pageAddr) {
        setCount(pageAddr, 0);
    }

    /**
     * Returns next page ID.
     *
     * @param pageAddr Page address.
     */
    public long getNextId(long pageAddr) {
        return getLong(pageAddr, NEXT_PAGE_ID_OFF);
    }

    /**
     * Writes next page ID.
     *
     * @param pageAddr Page address.
     * @param nextId Next page ID.
     */
    public void setNextId(long pageAddr, long nextId) {
        assertPageType(pageAddr);

        putLong(pageAddr, NEXT_PAGE_ID_OFF, nextId);
    }

    /**
     * Returns previous page ID.
     *
     * @param pageAddr Page address.
     */
    public long getPreviousId(long pageAddr) {
        return getLong(pageAddr, PREV_PAGE_ID_OFF);
    }

    /**
     * Writes previous page ID.
     *
     * @param pageAddr Page address.
     * @param prevId Previous page ID.
     */
    public void setPreviousId(long pageAddr, long prevId) {
        assertPageType(pageAddr);

        putLong(pageAddr, PREV_PAGE_ID_OFF, prevId);
    }

    /**
     * Gets total count of entries in this page. Does not change the buffer state.
     *
     * @param pageAddr Page address to get count from.
     * @return Total number of entries.
     */
    public int getCount(long pageAddr) {
        return getShort(pageAddr, CNT_OFF);
    }

    /**
     * Sets total count of entries in this page. Does not change the buffer state.
     *
     * @param pageAddr Page address to write to.
     * @param cnt Count.
     */
    private void setCount(long pageAddr, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;
        assertPageType(pageAddr);

        putShort(pageAddr, CNT_OFF, (short) cnt);
    }

    /**
     * Returns capacity of this page in items.
     *
     * @param pageSize Page size.
     */
    private int getCapacity(int pageSize) {
        return (pageSize - PAGE_IDS_OFF) >>> 3; // /8
    }

    /**
     * Returns item offset.
     *
     * @param idx Item index.
     */
    private int offset(int idx) {
        return PAGE_IDS_OFF + 8 * idx;
    }

    /**
     * Returns item at the given index.
     *
     * @param pageAddr Page address.
     * @param idx Item index.
     */
    public long getAt(long pageAddr, int idx) {
        return getLong(pageAddr, offset(idx));
    }

    /**
     * Writes item at the given index.
     *
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param pageId Item value to write.
     */
    private void setAt(long pageAddr, int idx, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, offset(idx), pageId);
    }

    /**
     * Adds page to the end of pages list.
     *
     * @param pageAddr Page address.
     * @param pageId Page ID.
     * @param pageSize Page size.
     * @return Total number of items in this page, or {@code -1} if page is full.
     */
    public int addPage(long pageAddr, long pageId, int pageSize) {
        assertPageType(pageAddr);

        int cnt = getCount(pageAddr);

        if (cnt == getCapacity(pageSize)) {
            return -1;
        }

        setAt(pageAddr, cnt, pageId);
        setCount(pageAddr, cnt + 1);

        return cnt;
    }

    /**
     * Removes any page from the pages list.
     *
     * @param pageAddr Page address.
     * @return Removed page ID.
     */
    public long takeAnyPage(long pageAddr) {
        assertPageType(pageAddr);

        int cnt = getCount(pageAddr);

        if (cnt == 0) {
            return 0L;
        }

        setCount(pageAddr, --cnt);

        return getAt(pageAddr, cnt);
    }

    /**
     * Removes the given page ID from the pages list.
     *
     * @param pageAddr Page address.
     * @param dataPageId Page ID to remove.
     * @return {@code true} if page was in the list and was removed, {@code false} otherwise.
     */
    public boolean removePage(long pageAddr, long dataPageId) {
        assert dataPageId != 0;
        assertPageType(pageAddr);

        int cnt = getCount(pageAddr);

        for (int i = 0; i < cnt; i++) {
            if (maskPartitionId(getAt(pageAddr, i)) == maskPartitionId(dataPageId)) {
                if (i != cnt - 1) {
                    copyMemory(pageAddr, offset(i + 1), pageAddr, offset(i), 8 * (cnt - i - 1));
                }

                setCount(pageAddr, cnt - 1);

                return true;
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if there are no items in this page.
     *
     * @param pageAddr Page address.
     */
    public boolean isEmpty(long pageAddr) {
        return getCount(pageAddr) == 0;
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("PagesListNode [\n\tpreviousPageId=").appendHex(getPreviousId(addr))
                .app(",\n\tnextPageId=").appendHex(getNextId(addr))
                .app(",\n\tcount=").app(getCount(addr))
                .app(",\n\tpages={");

        for (int i = 0; i < getCount(addr); i++) {
            sb.app("\n\t\t").app(getAt(addr, i));
        }

        sb.app("\n\t}\n]");
    }
}
