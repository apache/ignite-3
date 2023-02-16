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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.jetbrains.annotations.Nullable;

/**
 * Looks for a row version in the version chain by timestamp.
 */
class FindRowVersion implements PageMemoryTraversal<HybridTimestamp> {
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
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, HybridTimestamp arg) {
        if (!rowVersionFounded) {
            HybridTimestamp readTimestamp = HybridTimestamps.readTimestamp(pageAddr, payload.offset() + RowVersion.TIMESTAMP_OFFSET);

            long nextLink = PartitionlessLinks.readPartitionless(partitionId, pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);

            if (readTimestamp != null && readTimestamp.compareTo(arg) == 0) {
                rowVersionFounded = true;

                rowLink = link;
                rowTimestamp = readTimestamp;
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
     * Returns the row version in the version chain found by the timestamp, {@code null} if not found.
     */
    @Nullable RowVersion getResult() {
        return result;
    }
}
