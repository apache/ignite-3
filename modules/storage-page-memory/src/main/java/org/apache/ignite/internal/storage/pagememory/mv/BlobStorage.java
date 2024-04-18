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

import java.util.Objects;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
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
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobFragmentIo;
import org.jetbrains.annotations.Nullable;

/**
 * Used to store a limited number of blobs (just byte arrays) per partition. Each blob is stored in a sequence
 * of pages forming a linked list (a previous page links to the next one).
 *
 * <p>If a lot of blobs (comparable with the number of rows) needs to be stored in a partition, another mechanism
 * (probably using a {@link org.apache.ignite.internal.pagememory.freelist.FreeList}) should be used.
 */
public class BlobStorage extends DataStructure {
    static final long NO_PAGE_ID = 0;

    private final IoStatisticsHolder statisticsHolder;

    private final RecycleAndAddToReuseBag recycleAndAddToReuseBag = new RecycleAndAddToReuseBag();

    private final ReadFragment readFragment = new ReadFragment();

    private final WriteFragment writeFragment = new WriteFragment();

    /**
     * Creates a new instance.
     */
    public BlobStorage(ReuseList reuseList, PageMemory pageMemory, int groupId, int partitionId, IoStatisticsHolder statisticsHolder) {
        super("BlobStorage", groupId, null, partitionId, pageMemory, PageLockListenerNoOp.INSTANCE, PageIdAllocator.FLAG_AUX);

        super.reuseList = reuseList;
        this.statisticsHolder = statisticsHolder;
    }

    /**
     * Reads a blob stored starting at a page with the given ID.
     *
     * @param firstPageId ID of first page.
     * @return Byte array for the blob.
     * @throws IgniteInternalCheckedException If something goes wrong.
     */
    public byte[] readBlob(long firstPageId) throws IgniteInternalCheckedException {
        ReadState readState = new ReadState();

        long pageId = firstPageId;

        while (pageId != NO_PAGE_ID) {
            Boolean ok = PageHandler.readPage(
                    pageMem,
                    grpId,
                    pageId,
                    PageLockListenerNoOp.INSTANCE,
                    readFragment,
                    readState,
                    0,
                    false,
                    IoStatisticsHolderNoOp.INSTANCE
            );

            assert ok : pageId;

            pageId = readState.nextPageId;
        }

        assert readState.bytes != null;

        return readState.bytes;
    }

    /**
     * Adds a new blob to the storage.
     *
     * @param bytes Blob bytes.
     * @return ID of the page starting the chain representing the blob.
     * @throws IgniteInternalCheckedException If something goes wrong.
     */
    public long addBlob(byte[] bytes) throws IgniteInternalCheckedException {
        return doStore(NO_PAGE_ID, bytes);
    }

    /**
     * Updates the blob content.
     *
     * @param firstPageId ID of the first page in the chain storing the blob.
     * @param bytes New blob content.
     * @throws IgniteInternalCheckedException If something goes wrong.
     */
    public void updateBlob(long firstPageId, byte[] bytes) throws IgniteInternalCheckedException {
        doStore(firstPageId, bytes);
    }

    private long doStore(long maybeFirstPageId, byte[] bytes) throws IgniteInternalCheckedException {
        Objects.requireNonNull(bytes, "bytes is null");

        long firstPageId = allocatePageIfNeeded(maybeFirstPageId);

        WriteState state = new WriteState(bytes);
        state.pageId = firstPageId;

        do {
            Boolean ok = PageHandler.writePage(
                    pageMem,
                    grpId,
                    state.pageId,
                    PageLockListenerNoOp.INSTANCE,
                    writeFragment,
                    null,
                    state,
                    0,
                    false,
                    statisticsHolder
            );

            assert ok : state.pageId;
        } while (!state.stop);

        freePagesStartingWith(state.firstPageToFreeId);

        return firstPageId;
    }

    private long allocatePageIfNeeded(long maybePageId) throws IgniteInternalCheckedException {
        long pageId;

        if (maybePageId == NO_PAGE_ID) {
            pageId = allocatePage(null);

            init(pageId, BlobFragmentIo.VERSIONS.latest());
        } else {
            pageId = maybePageId;
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
            Long nextPageId = PageHandler.writePage(pageMem, grpId, pageId, PageLockListenerNoOp.INSTANCE,
                    recycleAndAddToReuseBag, null, reuseBag, 0, pageId, IoStatisticsHolderNoOp.INSTANCE);

            assert nextPageId != pageId : pageId;

            pageId = nextPageId;
        }

        return reuseBag;
    }

    /**
     * State of a read operation.
     */
    private static class ReadState {
        private byte @Nullable [] bytes;

        private int bytesOffset;

        private long nextPageId = NO_PAGE_ID;

        private boolean isFirstPage() {
            return bytesOffset == 0;
        }
    }

    /**
     * Reads a fragment stored in a page.
     */
    private class ReadFragment implements PageHandler<ReadState, Boolean> {
        @Override
        public Boolean run(int groupId, long pageId, long page, long pageAddr, PageIo io, ReadState state, int unused,
                IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
            BlobFragmentIo blobIo = (BlobFragmentIo) io;

            if (state.bytes == null) {
                assert state.isFirstPage();

                state.bytes = new byte[blobIo.getTotalLength(pageAddr)];
            }

            int capacityForBytes = blobIo.getCapacityForFragmentBytes(pageSize(), state.isFirstPage());
            int fragmentLength = Math.min(capacityForBytes, state.bytes.length - state.bytesOffset);

            blobIo.getFragmentBytes(pageAddr, state.isFirstPage(), state.bytes, state.bytesOffset, fragmentLength);

            long nextPageId = blobIo.getNextPageId(pageAddr);

            int newBytesOffset = state.bytesOffset + fragmentLength;

            if (newBytesOffset < state.bytes.length) {
                assert nextPageId != NO_PAGE_ID;

                state.nextPageId = nextPageId;
            } else {
                assert nextPageId == NO_PAGE_ID;

                state.nextPageId = NO_PAGE_ID;
            }

            state.bytesOffset = newBytesOffset;

            return true;
        }
    }

    /**
     * State of a write operation.
     */
    private static class WriteState {
        private final byte[] bytes;
        private int bytesOffset;

        private long pageId;

        private boolean stop;
        private long firstPageToFreeId = NO_PAGE_ID;

        private WriteState(byte[] bytes) {
            this.bytes = bytes;
        }

        private boolean isFirstPage() {
            return bytesOffset == 0;
        }
    }

    /**
     * Writes a fragment to a page.
     */
    private class WriteFragment implements PageHandler<WriteState, Boolean> {
        @Override
        public Boolean run(int groupId, long pageId, long page, long pageAddr, PageIo io, WriteState state, int unused,
                IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
            BlobFragmentIo blobIo = (BlobFragmentIo) io;

            int capacityForBytes = blobIo.getCapacityForFragmentBytes(pageSize(), state.isFirstPage());

            int fragmentLength = Math.min(capacityForBytes, state.bytes.length - state.bytesOffset);

            if (state.isFirstPage()) {
                blobIo.setTotalLength(pageAddr, state.bytes.length);
            }

            blobIo.setFragmentBytes(pageAddr, state.isFirstPage(), state.bytes, state.bytesOffset, fragmentLength);

            int newBytesOffset = state.bytesOffset + fragmentLength;

            long maybeNextPageId = blobIo.getNextPageId(pageAddr);

            if (newBytesOffset < state.bytes.length) {
                long nextPageId = allocatePageIfNeeded(maybeNextPageId);

                blobIo.setNextPageId(pageAddr, nextPageId);

                state.bytesOffset = newBytesOffset;
                state.pageId = nextPageId;
            } else {
                blobIo.setNextPageId(pageAddr, NO_PAGE_ID);

                state.firstPageToFreeId = maybeNextPageId;
                state.stop = true;
            }

            return true;
        }
    }

    /**
     * Recycles a page and adds it to a {@link ReuseBag}.
     */
    private class RecycleAndAddToReuseBag implements PageHandler<ReuseBag, Long> {
        @Override
        public Long run(int groupId, long pageId, long page, long pageAddr, PageIo io, ReuseBag reuseBag, int unused,
                IoStatisticsHolder statHolder) {
            BlobFragmentIo blobIo = (BlobFragmentIo) io;

            long nextPageId = blobIo.getNextPageId(pageAddr);

            long recycledPageId = recyclePage(pageId, pageAddr);

            reuseBag.addFreePage(recycledPageId);

            return nextPageId;
        }
    }
}
