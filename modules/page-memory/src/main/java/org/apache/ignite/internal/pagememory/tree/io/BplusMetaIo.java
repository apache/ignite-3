/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.pagememory.util.PageUtils.getByte;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * IO routines for B+Tree meta pages.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-16534
public class BplusMetaIo extends PageIo {
    /** Page IO type. */
    public static final int T_BPLUS_META = 2;

    /** I/O versions. */
    public static final IoVersions<BplusMetaIo> VERSIONS = new IoVersions<>(new BplusMetaIo(1));

    /** Offset where the number of levels is stored. */
    private static final int LVLS_OFFSET = COMMON_HEADER_END;

    /** Offset where each level's page ID starts to be stored. */
    private static final int REFS_OFFSET = LVLS_OFFSET + 1;

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private BplusMetaIo(int ver) {
        super(T_BPLUS_META, ver, FLAG_AUX);
    }

    /**
     * Initializes the root.
     *
     * @param pageAdrr Page address.
     * @param rootId Root page ID.
     * @param pageSize Page size.
     */
    public void initRoot(long pageAdrr, long rootId, int pageSize) {
        assertPageType(pageAdrr);

        setLevelsCount(pageAdrr, 1, pageSize);
        setFirstPageId(pageAdrr, 0, rootId);
    }

    /**
     * Returns number of levels in this tree.
     *
     * @param pageAddr Page address.
     */
    public int getLevelsCount(long pageAddr) {
        return Byte.toUnsignedInt(getByte(pageAddr, LVLS_OFFSET));
    }

    /**
     * Returns max levels possible for this page size.
     *
     * @param pageSize Page size.
     */
    private int getMaxLevels(int pageSize) {
        return (pageSize - REFS_OFFSET) / 8;
    }

    /**
     * Sets number of levels in this tree.
     *
     * @param pageAddr Page address.
     * @param lvls Number of levels in this tree.
     * @param pageSize Page size.
     */
    private void setLevelsCount(long pageAddr, int lvls, int pageSize) {
        assert lvls >= 0 && lvls <= getMaxLevels(pageSize) : lvls;

        PageUtils.putByte(pageAddr, LVLS_OFFSET, (byte) lvls);

        assert getLevelsCount(pageAddr) == lvls;
    }

    /**
     * Returns offset for page reference.
     *
     * @param lvl Level.
     */
    private int offset(int lvl) {
        return lvl * 8 + REFS_OFFSET;
    }

    /**
     * Returns the ID of the first page at the requested level.
     *
     * @param pageAddr Page address.
     * @param lvl Level.
     */
    public long getFirstPageId(long pageAddr, int lvl) {
        return PageUtils.getLong(pageAddr, offset(lvl));
    }

    /**
     * Sets the ID of the first page at the requested level.
     *
     * @param pageAddr Page address.
     * @param lvl Level.
     * @param pageId Page ID.
     */
    private void setFirstPageId(long pageAddr, int lvl, long pageId) {
        assert lvl >= 0 && lvl < getLevelsCount(pageAddr) : lvl;

        PageUtils.putLong(pageAddr, offset(lvl), pageId);

        assert getFirstPageId(pageAddr, lvl) == pageId;
    }

    /**
     * Return root level.
     *
     * @param pageAddr Page address.
     */
    public int getRootLevel(long pageAddr) {
        int lvls = getLevelsCount(pageAddr); // The highest level page is root.

        assert lvls > 0 : lvls;

        return lvls - 1;
    }

    /**
     * Adds root.
     *
     * @param pageAddr Page address.
     * @param rootPageId New root page ID.
     * @param pageSize Page size.
     */
    public void addRoot(long pageAddr, long rootPageId, int pageSize) {
        assertPageType(pageAddr);

        int lvl = getLevelsCount(pageAddr);

        setLevelsCount(pageAddr, lvl + 1, pageSize);
        setFirstPageId(pageAddr, lvl, rootPageId);
    }

    /**
     * Cuts (decrease tree height) root.
     *
     * @param pageAddr Page address.
     * @param pageSize Page size.
     */
    public void cutRoot(long pageAddr, int pageSize) {
        assertPageType(pageAddr);

        int lvl = getRootLevel(pageAddr);

        setLevelsCount(pageAddr, lvl, pageSize); // Decrease tree height.
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        //TODO https://issues.apache.org/jira/browse/IGNITE-16350
        sb.app("BPlusMeta [\n\tlevelsCnt=").app(getLevelsCount(addr))
                .app(",\n\trootLvl=").app(getRootLevel(addr))
                .app("\n]");
    }
}
