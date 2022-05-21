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

import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Traversal that scans Version Chain until first version visible at the given timestamp is found; then the version
 * is converted to {@link ByteBufferRow} and finally made available via {@link #result()}.
 *
 * NB: this traversal first traverses starting data slots of the Version Chain one after another; when it finds the
 * version it needs, it switches to traversing the slots comprising the version (because it might be fragmented).
 */
class ScanVersionChainByTimestamp implements PageMemoryTraversal {
    private final Timestamp timestamp;
    private final int partitionId;

    /**
     * Contains the result when the traversal ends.
     */
    @Nullable
    private ByteBufferRow result;

    /**
     * First it's {@code true} (this means that we traverse first slots of versions of the Version Chain using NextLink);
     * then it's {@code false} (when we found the version we need and we read its value).
     */
    private boolean lookingForRow = true;

    /**
     * Used to collect all the bytes of the target version value.
     */
    private byte @Nullable [] allValueBytes;
    /**
     * Number of bytes written to {@link #allValueBytes}.
     */
    private int writtenBytes = 0;

    ScanVersionChainByTimestamp(Timestamp timestamp, int partitionId) {
        this.timestamp = timestamp;
        this.partitionId = partitionId;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload) {
        if (lookingForRow) {
            Timestamp rowVersionTs = Timestamps.readTimestamp(pageAddr, payload.offset());

            if (rowTimestampMatches(rowVersionTs)) {
                return readFullyOrStartReadingFragmented(pageAddr, payload);
            } else {
                return advanceToNextVersion(pageAddr, payload);
            }
        } else {
            // we are continuing reading a fragmented row
            return readNextFragment(pageAddr, payload);
        }
    }

    private boolean rowTimestampMatches(Timestamp rowVersionTs) {
        return rowVersionTs != null && rowVersionTs.beforeOrEquals(timestamp);
    }

    private long readFullyOrStartReadingFragmented(long pageAddr, DataPagePayload payload) {
        lookingForRow = false;

        int valueSizeInCurrentPage = readValueSize(pageAddr, payload);

        if (!payload.hasMoreFragments()) {
            return readFully(pageAddr, payload, valueSizeInCurrentPage);
        } else {
            allValueBytes = new byte[valueSizeInCurrentPage];
            writtenBytes = 0;

            readValueFragmentToArray(pageAddr, payload, valueSizeInCurrentPage);

            return payload.nextLink();
        }
    }

    private int readValueSize(long pageAddr, DataPagePayload payload) {
        return PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);
    }

    private long readFully(long pageAddr, DataPagePayload payload, int valueSizeInCurrentPage) {
        if (RowVersion.isTombstone(valueSizeInCurrentPage)) {
            result = null;
        } else {
            result = new ByteBufferRow(PageUtils.getBytes(pageAddr, payload.offset() + RowVersion.VALUE_OFFSET, valueSizeInCurrentPage));
        }
        return STOP_TRAVERSAL;
    }

    private void readValueFragmentToArray(long pageAddr, DataPagePayload payload, int valueSizeInCurrentPage) {
        PageUtils.getBytes(pageAddr, payload.offset() + RowVersion.VALUE_OFFSET, allValueBytes, writtenBytes, valueSizeInCurrentPage);
        writtenBytes += valueSizeInCurrentPage;
    }

    private long advanceToNextVersion(long pageAddr, DataPagePayload payload) {
        long partitionlessNextLink = PartitionlessLinks.readFromMemory(pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);
        if (partitionlessNextLink == RowVersion.NULL_LINK) {
            return STOP_TRAVERSAL;
        }
        return PartitionlessLinks.addPartitionIdToPartititionlessLink(partitionlessNextLink, partitionId);
    }

    private long readNextFragment(long pageAddr, DataPagePayload payload) {
        assert allValueBytes != null;

        int valueSizeInCurrentPage = readValueSize(pageAddr, payload);

        readValueFragmentToArray(pageAddr, payload, valueSizeInCurrentPage);

        if (payload.hasMoreFragments()) {
            return payload.nextLink();
        } else {
            if (allValueBytes.length == 0) {
                result = null;
            } else {
                result = new ByteBufferRow(allValueBytes);
            }

            return STOP_TRAVERSAL;
        }
    }

    @Nullable
    ByteBufferRow result() {
        return result;
    }
}
