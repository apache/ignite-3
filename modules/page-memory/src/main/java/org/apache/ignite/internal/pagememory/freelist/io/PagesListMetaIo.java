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
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.freelist.PagesList;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;

/**
 * Pages list meta IO.
 */
public class PagesListMetaIo extends PageIo {
    /** Page IO type. */
    public static final int T_PAGE_LIST_META = 1;

    /** I/O versions. */
    public static final IoVersions<PagesListMetaIo> VERSIONS = new IoVersions<>(new PagesListMetaIo(1));

    private static final int CNT_OFF = COMMON_HEADER_END;

    private static final int NEXT_META_PAGE_OFF = CNT_OFF + 2;

    private static final int ITEMS_OFF = NEXT_META_PAGE_OFF + 8;

    private static final int ITEM_SIZE = 10;

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private PagesListMetaIo(int ver) {
        super(T_PAGE_LIST_META, ver, FLAG_AUX);
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setCount(pageAddr, 0);
        setNextMetaPageId(pageAddr, 0L);
    }

    /**
     * Returns stored items count.
     *
     * @param pageAddr Page address.
     */
    private int getCount(long pageAddr) {
        return getShort(pageAddr, CNT_OFF);
    }

    /**
     * Writes stored items count.
     *
     * @param pageAddr Page address.
     * @param cnt Stored items count.
     */
    private void setCount(long pageAddr, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;
        assertPageType(pageAddr);

        putShort(pageAddr, CNT_OFF, (short) cnt);
    }

    /**
     * Returns next meta page ID.
     *
     * @param pageAddr Page address.
     */
    public long getNextMetaPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_META_PAGE_OFF);
    }

    /**
     * Writes next meta page ID.
     *
     * @param pageAddr Page address.
     * @param metaPageId Next meta page ID.
     */
    public void setNextMetaPageId(long pageAddr, long metaPageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, NEXT_META_PAGE_OFF, metaPageId);
    }

    /**
     * Resets stored items count.
     *
     * @param pageAddr Page address.
     */
    public void resetCount(long pageAddr) {
        setCount(pageAddr, 0);
    }

    /**
     * Writes tails.
     *
     * @param pageSize Page size.
     * @param pageAddr Page address.
     * @param bucket Bucket number.
     * @param tails Tails.
     * @param tailsOff Tails offset.
     * @return Number of items written.
     */
    public int addTails(int pageSize, long pageAddr, int bucket, PagesList.Stripe[] tails, int tailsOff) {
        assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;
        assertPageType(pageAddr);

        int cnt = getCount(pageAddr);
        int cap = getCapacity(pageSize);

        if (cnt == cap) {
            return 0;
        }

        int off = offset(cnt);

        int write = Math.min(cap - cnt, tails.length - tailsOff);

        for (int i = 0; i < write; i++) {
            putShort(pageAddr, off, (short) bucket);
            putLong(pageAddr, off + 2, tails[tailsOff].tailId);

            tailsOff++;

            off += ITEM_SIZE;
        }

        setCount(pageAddr, cnt + write);

        return write;
    }

    /**
     * Reads tails.
     *
     * @param pageAddr Page address.
     * @param res Results map.
     */
    public void getBucketsData(long pageAddr, Map<Integer, LongArrayList> res) {
        int cnt = getCount(pageAddr);

        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        if (cnt == 0) {
            return;
        }

        int off = offset(0);

        for (int i = 0; i < cnt; i++) {
            int bucket = getShort(pageAddr, off);
            assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;

            long tailId = PageUtils.getLong(pageAddr, off + 2);
            assert tailId != 0;

            LongArrayList list = res.get(bucket);

            if (list == null) {
                res.put(bucket, list = new LongArrayList());
            }

            list.add(tailId);

            off += ITEM_SIZE;
        }
    }

    /**
     * Returns maximum number of items which can be stored in buffer.
     */
    private int getCapacity(int pageSize) {
        return (pageSize - ITEMS_OFF) / ITEM_SIZE;
    }

    /**
     * Returns item offset.
     *
     * @param idx Item index.
     */
    private int offset(int idx) {
        return ITEMS_OFF + ITEM_SIZE * idx;
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        int cnt = getCount(addr);

        sb.app("PagesListMeta [\n\tnextMetaPageId=").appendHex(getNextMetaPageId(addr))
                .app(",\n\tcount=").app(cnt)
                .app(",\n\tbucketData={");

        Map<Integer, LongArrayList> bucketsData = new HashMap<>(cnt);

        getBucketsData(addr, bucketsData);

        for (Map.Entry<Integer, LongArrayList> e : bucketsData.entrySet()) {
            sb.app("\n\t\tbucket=").app(e.getKey()).app(", list=").app(e.getValue());
        }

        sb.app("\n\t}\n]");
    }
}
