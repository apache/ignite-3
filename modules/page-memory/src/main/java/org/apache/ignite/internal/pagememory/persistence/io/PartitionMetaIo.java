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

package org.apache.ignite.internal.pagememory.persistence.io;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.PageIo;

/**
 * base Io for partition metadata pages.
 */
public abstract class PartitionMetaIo extends PageIo {
    /** Page IO type. */
    public static final short T_TABLE_PARTITION_META_IO = 7;

    private final int getPageCountOff;

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected PartitionMetaIo(int ver, int getPageCountOff) {
        super(T_TABLE_PARTITION_META_IO, ver, FLAG_AUX);
        this.getPageCountOff = getPageCountOff;
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setPageCount(pageAddr, 0);
    }

    /**
     * Sets the count of pages.
     *
     * @param pageAddr Page address.
     * @param pageCount Count of pages.
     */
    public void setPageCount(long pageAddr, int pageCount) {
        assertPageType(pageAddr);

        putInt(pageAddr, getPageCountOff, pageCount);
    }

    /**
     * Returns the page count.
     *
     * @param pageAddr Page address.
     */
    public int getPageCount(long pageAddr) {
        return getInt(pageAddr, getPageCountOff);
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TablePartitionMeta [").nl()
                .app("pageCount=").app(getPageCount(addr)).nl()
                .app(']');
    }
}
