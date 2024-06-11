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

import org.apache.ignite.internal.pagememory.io.PageIo;

/**
 * Base Io for partition metadata pages.
 */
public abstract class PartitionMetaIo extends PageIo {
    private static final int PAGE_COUNT_OFF = COMMON_HEADER_END;

    protected static final int PARTITION_META_HEADER_END = PAGE_COUNT_OFF + Integer.BYTES;

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected PartitionMetaIo(int type, int ver) {
        super(type, ver, FLAG_AUX);
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

        putInt(pageAddr, PAGE_COUNT_OFF, pageCount);
    }

    /**
     * Returns the page count.
     *
     * @param pageAddr Page address.
     */
    public int getPageCount(long pageAddr) {
        return getInt(pageAddr, PAGE_COUNT_OFF);
    }
}
