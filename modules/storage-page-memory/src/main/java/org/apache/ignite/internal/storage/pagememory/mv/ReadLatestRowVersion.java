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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Traversal for reading the latest row version. If the version is uncommitted, returns its value; otherwise, does NOT return it.
 */
class ReadLatestRowVersion implements PageMemoryTraversal {
    private final Predicate<Timestamp> loadValue;

    private RowVersion result;

    private boolean readingFirstSlot = true;

    private long firstFragmentLink;
    @Nullable
    private Timestamp timestamp;
    private long nextLink;

    /**
     * Used to collect all the bytes of the target version value.
     */
    private byte @Nullable [] allValueBytes;
    /**
     * Number of bytes written to {@link #allValueBytes}.
     */
    private int writtenBytes = 0;

    ReadLatestRowVersion(Predicate<Timestamp> loadValue) {
        this.loadValue = loadValue;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload) {
        if (readingFirstSlot) {
            return readFullOrInitiateReadFragmented(link, pageAddr, payload);
        } else {
            return readNextFragment(pageAddr, payload);
        }
    }

    private long readFullOrInitiateReadFragmented(long link, long pageAddr, DataPagePayload payload) {
        firstFragmentLink = link;

        timestamp = Timestamps.readTimestamp(pageAddr, payload.offset());
        nextLink = PartitionlessLinks.readFromMemory(pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);

        if (!loadValue.test(timestamp)) {
            result = new RowVersion(partitionIdFromLink(link), firstFragmentLink, timestamp, nextLink, null);
            return STOP_TRAVERSAL;
        }

        int valueSizeInCurrentSlot = readValueSize(pageAddr, payload);

        if (!payload.hasMoreFragments()) {
            ByteBuffer value = ByteBuffer.wrap(PageUtils.getBytes(pageAddr, payload.offset() + RowVersion.VALUE_OFFSET, valueSizeInCurrentSlot))
                    .order(ByteBufferRow.ORDER);
            result = new RowVersion(partitionIdFromLink(link), firstFragmentLink, timestamp, nextLink, value);
            return STOP_TRAVERSAL;
        } else {
            readingFirstSlot = false;

            allValueBytes = new byte[valueSizeInCurrentSlot];
            writtenBytes = 0;

            readValueFragmentToArray(pageAddr, payload, valueSizeInCurrentSlot);

            return payload.nextLink();
        }
    }

    private int readValueSize(long pageAddr, DataPagePayload payload) {
        return PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);
    }

    private void readValueFragmentToArray(long pageAddr, DataPagePayload payload, int valueSizeInCurrentSlot) {
        PageUtils.getBytes(pageAddr, payload.offset() + RowVersion.VALUE_OFFSET, allValueBytes, writtenBytes, valueSizeInCurrentSlot);
        writtenBytes += valueSizeInCurrentSlot;
    }

    private long readNextFragment(long pageAddr, DataPagePayload payload) {
        assert allValueBytes != null;

        int valueSizeInCurrentSlot = readValueSize(pageAddr, payload);

        readValueFragmentToArray(pageAddr, payload, valueSizeInCurrentSlot);

        if (payload.hasMoreFragments()) {
            return payload.nextLink();
        } else {
            if (allValueBytes.length == 0) {
                result = null;
            } else {
                result = new RowVersion(partitionIdFromLink(firstFragmentLink), firstFragmentLink, timestamp, nextLink, ByteBuffer.wrap(allValueBytes));
            }

            return STOP_TRAVERSAL;
        }
    }

    private int partitionIdFromLink(long link) {
        return PageIdUtils.partitionId(PageIdUtils.pageId(link));
    }

    RowVersion result() {
        return result;
    }
}
