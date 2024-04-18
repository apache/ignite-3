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
import static org.apache.ignite.internal.pagememory.util.PageUtils.getUnsignedByte;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putUnsignedByte;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.PageIo;

/**
 * Abstract IO routines for B+Tree meta pages.
 */
public abstract class BplusMetaIo extends PageIo {
    /** Offset where the number of levels is stored. */
    private static final int LVLS_OFFSET = COMMON_HEADER_END;

    /** Offset where each level's page ID starts to be stored. */
    private static final int REFS_OFFSET = LVLS_OFFSET + Byte.BYTES;

    /** Size of the link in bytes. */
    private static final int LINK_SIZE = PARTITIONLESS_LINK_SIZE_BYTES;

    /** Maximum level of the tree. */
    private static final int MAX_LEVEL = 32;

    /** Size of the B+tree common meta in bytes. */
    protected static final int COMMON_META_END = REFS_OFFSET + (LINK_SIZE * MAX_LEVEL);

    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver Page format version.
     */
    protected BplusMetaIo(int type, int ver) {
        super(type, ver, FLAG_AUX);
    }

    /**
     * Initializes the root.
     *
     * @param pageAdrr Page address.
     * @param rootId Root page ID.
     */
    public void initRoot(long pageAdrr, long rootId) {
        assertPageType(pageAdrr);

        setLevelsCount(pageAdrr, 1);
        setFirstPageId(pageAdrr, 0, rootId);
    }

    /**
     * Returns number of levels in this tree.
     *
     * @param pageAddr Page address.
     */
    public int getLevelsCount(long pageAddr) {
        return getUnsignedByte(pageAddr, LVLS_OFFSET);
    }

    /**
     * Sets number of levels in this tree.
     *
     * @param pageAddr Page address.
     * @param lvls Number of levels in this tree.
     */
    private void setLevelsCount(long pageAddr, int lvls) {
        assert lvls >= 0 && lvls <= MAX_LEVEL : lvls;

        putUnsignedByte(pageAddr, LVLS_OFFSET, lvls);

        assert getLevelsCount(pageAddr) == lvls;
    }

    /**
     * Returns offset for page reference.
     *
     * @param lvl Level.
     */
    private int offset(int lvl) {
        return lvl * LINK_SIZE + REFS_OFFSET;
    }

    /**
     * Returns the ID of the first page at the requested level.
     *
     * @param pageAddr Page address.
     * @param lvl Level.
     * @param partId Partition ID.
     */
    public long getFirstPageId(long pageAddr, int lvl, int partId) {
        return readPartitionless(partId, pageAddr, offset(lvl));
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

        writePartitionless(pageAddr + offset(lvl), pageId);

        assert getFirstPageId(pageAddr, lvl, partitionId(pageId)) == pageId;
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
     */
    public void addRoot(long pageAddr, long rootPageId) {
        assertPageType(pageAddr);

        int lvl = getLevelsCount(pageAddr);

        setLevelsCount(pageAddr, lvl + 1);
        setFirstPageId(pageAddr, lvl, rootPageId);
    }

    /**
     * Cuts (decrease tree height) root.
     *
     * @param pageAddr Page address.
     */
    public void cutRoot(long pageAddr) {
        assertPageType(pageAddr);

        int lvl = getRootLevel(pageAddr);

        setLevelsCount(pageAddr, lvl); // Decrease tree height.
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-16350
        sb.app("BPlusMeta [\n\tlevelsCnt=").app(getLevelsCount(addr))
                .app(",\n\trootLvl=").app(getRootLevel(addr))
                .app("\n]");
    }
}
