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

package org.apache.ignite.internal.pagememory.datapage;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Contains logic for reading values from page memory data pages (this includes handling multy-page values).
 */
public abstract class DataPageReader<T> {
    private final PageMemory pageMemory;
    private final int groupId;
    private final IoStatisticsHolder statisticsHolder;

    /**
     * Constructs a new instance.
     *
     * @param pageMemory       page memory that will be used to lock and access memory
     * @param groupId          ID of the cache group with which the reader works (all pages must belong to this group)
     * @param statisticsHolder used to track statistics about operations
     */
    public DataPageReader(PageMemory pageMemory, int groupId, IoStatisticsHolder statisticsHolder) {
        this.pageMemory = pageMemory;
        this.groupId = groupId;
        this.statisticsHolder = statisticsHolder;
    }

    /**
     * Returns a row by link. To get the row bytes, more than one pages may be traversed (if the corresponding row
     * was fragmented when stored).
     *
     * @param link Row link
     * @return row object assembled from the row bytes
     * @throws IgniteInternalCheckedException If failed
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     */
    public T getRowByLink(final long link) throws IgniteInternalCheckedException {
        assert link != 0;

        int pageSize = pageMemory.realPageSize(groupId);

        ByteArrayOutputStream baos = null;

        long nextLink = link;

        do {
            final long pageId = pageId(nextLink);

            final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

            try {
                long pageAddr = pageMemory.readLock(groupId, pageId, page);

                assert pageAddr != 0L : nextLink;

                try {
                    AbstractDataPageIo<?> dataIo = pageMemory.ioRegistry().resolve(pageAddr);

                    int itemId = itemId(nextLink);

                    DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                    if (!data.hasMoreFragments() && nextLink == link) {
                        // Good luck: we can read the row without fragments.
                        return readRowFromAddress(link, pageAddr + data.offset());
                    }

                    ByteBuffer dataBuf = wrapPointer(pageAddr, pageSize);

                    dataBuf.position(data.offset());
                    dataBuf.limit(data.offset() + data.payloadSize());

                    if (baos == null) {
                        baos = new ByteArrayOutputStream();
                    }

                    byte[] bytes = new byte[data.payloadSize()];
                    dataBuf.get(bytes);
                    try {
                        baos.write(bytes);
                    } catch (IOException e) {
                        throw new IllegalStateException("Should not happen", e);
                    }

                    nextLink = data.nextLink();
                } finally {
                    pageMemory.readUnlock(groupId, pageId, page);
                }
            } finally {
                pageMemory.releasePage(groupId, pageId, page);
            }
        } while (nextLink != 0);

        byte[] wholePayloadBytes = baos.toByteArray();

        ByteBuffer wholePayloadBuf = ByteBuffer.wrap(wholePayloadBytes)
                .order(NATIVE_BYTE_ORDER);
        return readRowFromBuffer(link, wholePayloadBuf);
    }

    /**
     * Traverses page memory starting at the given link. At each step, reads the current data row and feeds it to the given
     * {@link PageMemoryTraversal} object which updates itself (usually) and returns next link to continue traversal
     * (or {@link PageMemoryTraversal#STOP_TRAVERSAL} to stop.
     *
     * @param link Row link
     * @param traversal object consuming payloads and controlling the traversal
     * @throws IgniteInternalCheckedException If failed
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     * @see PageMemoryTraversal
     */
    public void traverse(final long link, PageMemoryTraversal traversal) throws IgniteInternalCheckedException {
        assert link != 0;

        int pageSize = pageMemory.realPageSize(groupId);

        long currentLink = link;

        do {
            final long pageId = pageId(currentLink);
            final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

            try {
                long pageAddr = pageMemory.readLock(groupId, pageId, page);
                assert pageAddr != 0L : currentLink;

                try {
                    AbstractDataPageIo<?> dataIo = pageMemory.ioRegistry().resolve(pageAddr);

                    int itemId = itemId(currentLink);

                    DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                    currentLink = traversal.consumePagePayload(currentLink, pageAddr, data);
                } finally {
                    pageMemory.readUnlock(groupId, pageId, page);
                }
            } finally {
                pageMemory.releasePage(groupId, pageId, page);
            }
        } while (currentLink != PageMemoryTraversal.STOP_TRAVERSAL);
    }

    /**
     * Reads row object from a contiguous region of memory represented with a buffer. The buffer contains exactly
     * the bytes representing the row, not more, not less.
     *
     * @param link            row link
     * @param wholePayloadBuf buffer containing all row bytes
     * @return row object
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     */
    protected abstract T readRowFromBuffer(long link, ByteBuffer wholePayloadBuf);

    /**
     * Reads row object from a contiguous region of memory represented by its starting address. The memory region contains exactly
     * the bytes representing the row.
     *
     * @param link            row link
     * @param pageAddr        address of memory containing all row bytes
     * @return row object
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     */
    protected abstract T readRowFromAddress(long link, long pageAddr);
}
