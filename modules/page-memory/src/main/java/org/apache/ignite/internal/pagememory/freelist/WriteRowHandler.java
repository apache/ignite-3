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

package org.apache.ignite.internal.pagememory.freelist;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.util.PageHandler;

class WriteRowHandler implements PageHandler<Storable, Integer> {
    private final FreeListImpl freeList;

    WriteRowHandler(FreeListImpl freeList) {
        this.freeList = freeList;
    }

    @Override
    public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIo iox,
            Storable row,
            int written,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        written = addRow(pageId, pageAddr, iox, row, written);

        putPage(((DataPageIo) iox).getFreeSpace(pageAddr), pageId, pageAddr, statHolder);

        return written;
    }

    /**
     * Writes row to data page.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param iox IO.
     * @param row Row to write.
     * @param written Written size.
     * @return Number of bytes written, {@link FreeListImpl#COMPLETE} if the row was fully written.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected Integer addRow(
            long pageId,
            long pageAddr,
            PageIo iox,
            Storable row,
            int written
    ) throws IgniteInternalCheckedException {
        DataPageIo io = (DataPageIo) iox;

        int rowSize = row.size();
        int oldFreeSpace = io.getFreeSpace(pageAddr);

        assert oldFreeSpace > 0 : oldFreeSpace;

        // If the full row does not fit into this page write only a fragment.
        written = (written == 0 && oldFreeSpace >= rowSize) ? addRowFull(pageId, pageAddr, io, row, rowSize) :
                addRowFragment(pageId, pageAddr, io, row, written, rowSize);

        if (written == rowSize) {
            freeList.evictionTracker.touchPage(pageId);
        }

        // Avoid boxing with garbage generation for usual case.
        return written == rowSize ? FreeListImpl.COMPLETE : written;
    }

    /**
     * Adds row to this data page and sets respective link to the given row object.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param io IO.
     * @param row Row.
     * @param rowSize Row size.
     * @return Written size which is always equal to row size here.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected int addRowFull(
            long pageId,
            long pageAddr,
            DataPageIo io,
            Storable row,
            int rowSize
    ) throws IgniteInternalCheckedException {
        io.addRow(pageId, pageAddr, row, rowSize, freeList.pageSize());

        return rowSize;
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param io IO.
     * @param row Row.
     * @param written Written size.
     * @param rowSize Row size.
     * @return Updated written size.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected int addRowFragment(
            long pageId,
            long pageAddr,
            DataPageIo io,
            Storable row,
            int written,
            int rowSize
    ) throws IgniteInternalCheckedException {
        int payloadSize = io.addRowFragment(freeList.pageMemory(), pageId, pageAddr, row, written, rowSize, freeList.pageSize());

        assert payloadSize > 0 : payloadSize;

        return written + payloadSize;
    }

    /**
     * Put page into the free list if needed.
     *
     * @param freeSpace Page free space.
     * @param pageId Page ID.
     * @param pageAddr Page address.
     * @param statHolder Statistics holder to track IO operations.
     */
    protected void putPage(
            int freeSpace,
            long pageId,
            long pageAddr,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        if (freeSpace > FreeListImpl.MIN_PAGE_FREE_SPACE) {
            int bucket = freeList.bucket(freeSpace, false);

            freeList.put(null, pageId, pageAddr, bucket, statHolder);
        }
    }
}
