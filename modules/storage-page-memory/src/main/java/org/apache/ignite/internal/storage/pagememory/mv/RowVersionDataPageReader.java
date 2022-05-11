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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * {@link DataPageReader} for {@link RowVersion} instances.
 */
class RowVersionDataPageReader extends DataPageReader<RowVersion> {
    /**
     * Constructs a new instance.
     *
     * @param pageMemory       page memory that will be used to lock and access memory
     * @param groupId          ID of the cache group with which the reader works (all pages must belong to this group)
     * @param statisticsHolder used to track statistics about operations
     */
    public RowVersionDataPageReader(PageMemory pageMemory, int groupId, IoStatisticsHolder statisticsHolder) {
        super(pageMemory, groupId, statisticsHolder);
    }

    @Override
    protected RowVersion readRowFromBuffer(long link, ByteBuffer wholePayloadBuf) {
        long nodeId = wholePayloadBuf.getLong();
        long localTimestamp = wholePayloadBuf.getLong();
        Timestamp timestamp = new Timestamp(localTimestamp, nodeId);
        if (timestamp.equals(RowVersion.NULL_TIMESTAMP)) {
            timestamp = null;
        }

        long nextLink = PartitionlessLinks.readFromBuffer(wholePayloadBuf);

        int valueSize = wholePayloadBuf.getInt();
        ByteBuffer value = GridUnsafe.wrapPointer(GridUnsafe.bufferAddress(wholePayloadBuf) + wholePayloadBuf.position(), valueSize);

        assert wholePayloadBuf.position() == wholePayloadBuf.limit();

        return new RowVersion(partitionOfLink(link), link, timestamp, nextLink, value);
    }

    @Override
    protected RowVersion readRowFromAddress(long link, long pageAddr) {
        int offset = 0;

        long nodeId = getLong(pageAddr, offset);
        offset += Long.BYTES;

        long localTimestamp = getLong(pageAddr, offset);
        offset += Long.BYTES;

        Timestamp timestamp = new Timestamp(localTimestamp, nodeId);
        if (timestamp.equals(RowVersion.NULL_TIMESTAMP)) {
            timestamp = null;
        }

        long nextLink = PartitionlessLinks.readFromMemory(pageAddr, offset);
        offset += PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;

        int valueSize = getInt(pageAddr, offset);
        offset += Integer.BYTES;
        ByteBuffer value = GridUnsafe.wrapPointer(pageAddr + offset, valueSize);

        return new RowVersion(partitionOfLink(link), link, timestamp, nextLink, value);
    }

    private int partitionOfLink(long link) {
        return partitionId(pageId(link));
    }
}
