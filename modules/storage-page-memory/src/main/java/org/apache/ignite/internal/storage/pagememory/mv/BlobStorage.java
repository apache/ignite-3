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
import org.apache.ignite.internal.pagememory.datastructure.DataStructure;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.LongListReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobDataIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobFirstIo;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

public class BlobStorage {
    static final long NO_PAGE_ID = 0;

    private final ReuseList reuseList;
    private final PageMemory pageMemory;

    private final int groupId;
    private final int partitionId;

    private final IoStatisticsHolder statisticsHolder;

    private final RecycleAndAddToReuseBag recycleAndAddToReuseBag = new RecycleAndAddToReuseBag();

    private final ReadBytes readBytes = new ReadBytes();

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
        return PageHandler.readPage(
                pageMemory,
                groupId,
                pageId,
                PageLockListenerNoOp.INSTANCE,
                readBytes,
                bytes,
                bytesOffset,
                null,
                IoStatisticsHolderNoOp.INSTANCE
        );
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

        long firstPageToFreeId = 0L;

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
                } else {
                    io.setNextPageId(pageAddr, NO_PAGE_ID);

                    firstPageToFreeId = nextPageId;
                }
            } finally {
                pageMemory.writeUnlock(groupId, pageId, page, wroteSomething);
            }
        } finally {
            pageMemory.releasePage(groupId, pageId, page);
        }

        freePagesStartingWith(firstPageToFreeId);

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

    private void freePagesStartingWith(long pageId) throws IgniteInternalCheckedException {
        if (pageId != NO_PAGE_ID) {
            reuseList.addForRecycle(recycleAndCollectPagesStartingWith(pageId));
        }
    }

    private ReuseBag recycleAndCollectPagesStartingWith(long startingPageId) throws IgniteInternalCheckedException {
        ReuseBag reuseBag = new LongListReuseBag();

        long pageId = startingPageId;

        while (pageId != NO_PAGE_ID) {
            Long nextPageId = PageHandler.writePage(pageMemory, groupId, pageId, PageLockListenerNoOp.INSTANCE,
                    recycleAndAddToReuseBag, null, reuseBag, 0, pageId, IoStatisticsHolderNoOp.INSTANCE);

            assert nextPageId != pageId : pageId;

            pageId = nextPageId;
        }

        return reuseBag;
    }

    private static class RecycleAndAddToReuseBag implements PageHandler<ReuseBag, Long> {
        @Override
        public Long run(int groupId, long pageId, long page, long pageAddr, PageIo io, ReuseBag reuseBag, int unused,
                IoStatisticsHolder statHolder) {
            BlobIo blobIo = (BlobIo) io;

            long nextPageId = blobIo.getNextPageId(pageAddr);

            long recycledPageId = DataStructure.recyclePage(pageId, pageAddr);

            reuseBag.addFreePage(recycledPageId);

            return nextPageId;
        }
    }

    private class ReadBytes implements PageHandler<byte[], byte[]> {
        @Override
        public byte[] run(int groupId, long pageId, long page, long pageAddr, PageIo io, byte[] bytes, int bytesOffset,
                IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
            BlobIo blobIo = (BlobIo) io;

            if (bytes == null) {
                assert bytesOffset == 0;

                bytes = new byte[blobIo.getTotalLength(pageAddr)];
            }

            int fragmentLength = blobIo.getFragmentLength(pageAddr);
            blobIo.getFragmentBytes(pageAddr, bytes, bytesOffset, fragmentLength);

            int newBytesOffset = bytesOffset + fragmentLength;

            if (newBytesOffset < bytes.length) {
                long nextPageId = blobIo.getNextPageId(pageAddr);

                assert nextPageId != NO_PAGE_ID;

                return doRead(nextPageId, bytes, newBytesOffset);
            } else {
                return bytes;
            }
        }
    }
}
