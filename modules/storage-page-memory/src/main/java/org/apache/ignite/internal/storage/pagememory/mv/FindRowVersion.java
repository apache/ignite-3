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
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Search for a row version in version chains.
 */
class FindRowVersion implements PageMemoryTraversal<RowVersionFilter> {
    private final int partitionId;

    private final boolean loadValueBytes;

    private boolean rowVersionFound;

    private final ReadRowVersionValue readRowVersionValue = new ReadRowVersionValue();

    private long rowLink = NULL_LINK;

    private @Nullable HybridTimestamp rowTimestamp;

    private long rowNextLink = NULL_LINK;

    private int rowValueSize;

    private int schemaVersion;

    private @Nullable RowVersion result;

    FindRowVersion(int partitionId, boolean loadValueBytes) {
        this.partitionId = partitionId;
        this.loadValueBytes = loadValueBytes;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, RowVersionFilter filter) {
        if (rowVersionFound) {
            return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
        }

        long nextLink = readPartitionless(partitionId, pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);

        if (!filter.apply(link, pageAddr + payload.offset())) {
            return nextLink;
        }

        rowVersionFound = true;

        rowLink = link;
        rowTimestamp = HybridTimestamps.readTimestamp(pageAddr, payload.offset() + RowVersion.TIMESTAMP_OFFSET);
        rowNextLink = nextLink;
        schemaVersion = Short.toUnsignedInt(PageUtils.getShort(pageAddr, payload.offset() + RowVersion.SCHEMA_VERSION_OFFSET));

        if (loadValueBytes) {
            return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
        }

        rowValueSize = PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);

        return STOP_TRAVERSAL;
    }

    @Override
    public void finish() {
        if (!rowVersionFound) {
            return;
        }

        if (loadValueBytes) {
            readRowVersionValue.finish();

            byte[] valueBytes = readRowVersionValue.result();

            BinaryRow row = valueBytes.length == 0
                    ? null
                    : new BinaryRowImpl(schemaVersion, ByteBuffer.wrap(valueBytes).order(BinaryTuple.ORDER));

            result = new RowVersion(partitionId, rowLink, rowTimestamp, rowNextLink, row);
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
    @FunctionalInterface
    interface RowVersionFilter {
        /**
         * Returns {@code true} if the version matches.
         *
         * @param rowVersionLink Row version link;
         * @param rowVersionAddr Address of row version (including page address + offset within it).
         */
        boolean apply(long rowVersionLink, long rowVersionAddr);

        static RowVersionFilter equalsByTimestamp(@Nullable HybridTimestamp timestamp) {
            return (rowVersionLink, rowVersionAddr) -> {
                HybridTimestamp readTimestamp = HybridTimestamps.readTimestamp(rowVersionAddr, RowVersion.TIMESTAMP_OFFSET);

                return Objects.equals(timestamp, readTimestamp);
            };
        }

        static RowVersionFilter equalsByNextLink(long nextLink) {
            return (rowVersionLink, rowVersionAddr) -> {
                long readNextLink = readPartitionless(partitionIdFromLink(rowVersionLink), rowVersionAddr, RowVersion.NEXT_LINK_OFFSET);

                return readNextLink == nextLink;
            };
        }
    }
}
