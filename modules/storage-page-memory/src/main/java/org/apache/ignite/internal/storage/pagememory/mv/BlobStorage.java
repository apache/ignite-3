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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobDataIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobFirstIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

public class BlobStorage {
    public static final long NO_PAGE_ID = 0;

    private final ReuseList reuseList;
    private final PageMemory pageMemory;

    private final int groupId;
    private final int partitionId;

    private final IoStatisticsHolder statisticsHolder;

    public BlobStorage(ReuseList reuseList, PageMemory pageMemory, int groupId, int partitionId, IoStatisticsHolder statisticsHolder) {
        this.reuseList = reuseList;
        this.pageMemory = pageMemory;
        this.groupId = groupId;
        this.partitionId = partitionId;
        this.statisticsHolder = statisticsHolder;
    }

    public byte[] readBlob(long firstPageId) throws IgniteInternalCheckedException {
        return doRead(firstPageId, null, 0);
    }

    private byte[] doRead(long pageId, byte @Nullable [] bytes, int bytesOffset) throws IgniteInternalCheckedException {
        final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

        try {
            long pageAddr = pageMemory.readLock(groupId, pageId, page);

            try {
                BlobIo io = pageMemory.ioRegistry().resolve(pageAddr);

                if (bytes == null) {
                    assert bytesOffset == 0;

                    bytes = new byte[io.getTotalLength(pageAddr)];
                }

                int fragmentLength = io.getFragmentLength(pageAddr);
                io.getFragmentBytes(pageAddr, bytes, bytesOffset, fragmentLength);

                int newBytesOffset = bytesOffset + fragmentLength;

                if (newBytesOffset < bytes.length) {
                    long nextPageId = io.getNextPageId(pageAddr);

                    assert nextPageId != NO_PAGE_ID;

                    return doRead(nextPageId, bytes, newBytesOffset);
                } else {
                    return bytes;
                }
            } finally {
                pageMemory.readUnlock(groupId, pageId, page);
            }
        } finally {
            pageMemory.releasePage(groupId, pageId, page);
        }
    }

    public long addBlob(byte[] bytes) throws IgniteInternalCheckedException {
        return doStore(NO_PAGE_ID, bytes, 0);
    }

    public void updateBlob(long firstPageId, byte[] bytes) throws IgniteInternalCheckedException {
        doStore(firstPageId, bytes, 0);
    }

    private long doStore(long maybePageId, byte[] bytes, int bytesOffset) throws IgniteInternalCheckedException {
        final boolean mustAllocatePage = maybePageId == NO_PAGE_ID;
        final boolean firstPage = bytesOffset == 0;

        final long pageId;
        if (mustAllocatePage) {
            pageId = allocatePage();

            PageHandler.initPage(pageMemory, groupId, pageId, latestBlobIo(firstPage), PageLockListenerNoOp.INSTANCE, statisticsHolder);
        } else {
            pageId = maybePageId;
        }

        final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

        try {
            boolean wroteSomething = false;

            long pageAddr = pageMemory.writeLock(groupId, pageId, page);

            try {
                BlobIo io;
                if (mustAllocatePage) {
                    io = latestBlobIo(firstPage);
                } else {
                    io = pageMemory.ioRegistry().resolve(pageAddr);
                }

                long nextPageId = mustAllocatePage ? NO_PAGE_ID : io.getNextPageId(pageAddr);
                int currentPageCapacity = pageMemory.realPageSize(groupId) - io.fullHeaderSize();
                int currentFragmentLength = Math.min(currentPageCapacity, bytes.length - bytesOffset);

                if (firstPage) {
                    io.setTotalLength(pageAddr, bytes.length);
                }
                io.setFragmentLength(pageAddr, currentFragmentLength);
                io.setFragmentBytes(pageAddr, bytes, bytesOffset, currentFragmentLength);

                wroteSomething = true;

                int newBytesOffset = bytesOffset + currentFragmentLength;
                if (newBytesOffset < bytes.length) {
                    long newNextPageId = doStore(nextPageId, bytes, newBytesOffset);

                    io.setNextPageId(pageAddr, newNextPageId);
                }
            } finally {
                pageMemory.writeUnlock(groupId, pageId, page, wroteSomething);
            }
        } finally {
            pageMemory.releasePage(groupId, pageId, page);
        }

        return pageId;
    }

    private static BlobIo latestBlobIo(boolean firstPage) {
        return firstPage ? BlobFirstIo.VERSIONS.latest() : BlobDataIo.VERSIONS.latest();
    }

    private long allocatePage() throws IgniteInternalCheckedException {
        long pageId = reuseList.takeRecycledPage();

        if (pageId != 0) {
            pageId = reuseList.initRecycledPage(pageId, PageIdAllocator.FLAG_AUX, null);
        }

        if (pageId == 0) {
            pageId = pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX);
        }

        return pageId;
    }
}
