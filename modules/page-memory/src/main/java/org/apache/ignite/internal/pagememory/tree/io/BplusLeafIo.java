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

/**
 * Abstract IO routines for B+Tree leaf pages.
 */
public abstract class BplusLeafIo<L> extends BplusIo<L> {
    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    protected BplusLeafIo(int type, int ver, int itemSize) {
        super(type, ver, true, true, itemSize);
    }

    @Override
    public int getMaxCount(long pageAddr, int pageSize) {
        return (pageSize - ITEMS_OFF) / getItemSize();
    }

    @Override
    public final void copyItems(
            long srcPageAddr,
            long dstPageAddr,
            int srcIdx,
            int dstIdx,
            int cnt,
            boolean cpLeft
    ) {
        assert srcIdx != dstIdx || srcPageAddr != dstPageAddr;

        assertPageType(dstPageAddr);

        copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt * (long) getItemSize());
    }

    @Override
    public final int offset(int idx) {
        assert idx >= 0 : idx;

        return ITEMS_OFF + idx * getItemSize();
    }
}
