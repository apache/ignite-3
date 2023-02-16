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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionIdFromLink;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Search for a row version in version chains.
 */
class FindRowVersion implements PageMemoryTraversal<RowVersionFilter> {
    private final int partitionId;

    private final boolean loadValueBytes;

    private boolean rowVersionFounded;

    private final ReadRowVersionValue readRowVersionValue = new ReadRowVersionValue();

    private long rowLink = NULL_LINK;

    private @Nullable HybridTimestamp rowTimestamp;

    private long rowNextLink = NULL_LINK;

    private int rowValueSize;

    private @Nullable RowVersion result;

    FindRowVersion(int partitionId, boolean loadValueBytes) {
        this.partitionId = partitionId;
        this.loadValueBytes = loadValueBytes;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, RowVersionFilter arg) {
        if (!rowVersionFounded) {
            long nextLink = readPartitionless(partitionId, pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);

            if (arg.apply(link, pageAddr + payload.offset())) {
                rowVersionFounded = true;

                rowLink = link;
                rowTimestamp = HybridTimestamps.readTimestamp(pageAddr, payload.offset() + RowVersion.TIMESTAMP_OFFSET);
                rowNextLink = nextLink;

                if (!loadValueBytes) {
                    rowValueSize = PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);

                    return STOP_TRAVERSAL;
                } else {
                    return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
                }
            } else {
                return nextLink;
            }
        } else {
            return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
        }
    }

    @Override
    public void finish() {
        if (!rowVersionFounded) {
            return;
        }

        if (loadValueBytes) {
            readRowVersionValue.finish();

            byte[] valueBytes = readRowVersionValue.result();

            ByteBuffer value = ByteBuffer.wrap(valueBytes).order(ByteBufferRow.ORDER);

            result = new RowVersion(partitionId, rowLink, rowTimestamp, rowNextLink, value);
        } else {
            result = new RowVersion(partitionId, rowLink, rowTimestamp, rowNextLink, rowValueSize);
        }
    }

    /**
     * Returns the found version in the version chain, {@code null} if not found.
     */
    @Nullable RowVersion getResult() {
        return result;
    }

    /**
     * Row version filter in the version chain.
     */
    interface RowVersionFilter {
        /**
         * Returns {@code true} if the version matches.
         *
         * @param rowVersionLink Row version link;
         * @param rowVersionAdder Address to row version (including page address + offset within it).
         */
        boolean apply(long rowVersionLink, long rowVersionAdder);

        static RowVersionFilter equalsByTimestamp(HybridTimestamp timestamp) {
            return (rowVersionLink, rowVersionAdder) -> {
                HybridTimestamp readTimestamp = HybridTimestamps.readTimestamp(rowVersionAdder, RowVersion.TIMESTAMP_OFFSET);

                return readTimestamp != null && readTimestamp.compareTo(timestamp) == 0;
            };
        }

        static RowVersionFilter equalsByNextLink(long nextLink) {
            return (rowVersionLink, rowVersionAdder) -> {
                long readNextLink = readPartitionless(partitionIdFromLink(rowVersionLink), rowVersionAdder, RowVersion.NEXT_LINK_OFFSET);

                return readNextLink == nextLink;
            };
        }
    }
}
