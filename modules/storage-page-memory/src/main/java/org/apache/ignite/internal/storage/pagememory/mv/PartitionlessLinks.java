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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.link;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.tag;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/**
 * Handling of <em>partitionless links</em>, that is, page memory links from which partition ID is removed. They are used to spare storage
 * space in cases when we know the partition ID from the context.
 *
 * @see PageIdUtils#link(long, int)
 */
public class PartitionlessLinks {
    /**
     * Number of bytes a partitionless link takes in storage.
     */
    public static final int PARTITIONLESS_LINK_SIZE_BYTES = 6;

    /**
     * Reads a partitionless link from the memory.
     *
     * @param pageAddr Page address.
     * @param offset Data offset.
     * @return Partitionless link.
     */
    public static long readPartitionlessLink(int partitionId, long pageAddr, int offset) {
        int tag = getShort(pageAddr, offset) & 0xFFFF;
        int pageIdx = getInt(pageAddr, offset + Short.BYTES);

        // Links to metapages are impossible. For the sake of simplicity, NULL_LINK is returned in this case.
        if (pageIdx == 0) {
            assert tag == 0 : tag;

            return RowVersion.NULL_LINK;
        }

        byte flags = (byte) tag;
        int itemId = tag >>> 8;

        long pageId = pageId(partitionId, flags, pageIdx);

        return link(pageId, itemId);
    }

    /**
     * Writes a partitionless link to memory: first high 2 bytes, then low 4 bytes.
     *
     * @param addr Address in memory where to start.
     * @param link The link to write.
     * @return Number of bytes written (equal to {@link #PARTITIONLESS_LINK_SIZE_BYTES}).
     */
    public static long writePartitionlessLink(long addr, long link) {
        putShort(addr, 0, (short) tag(link));

        putInt(addr + Short.BYTES, 0, pageIndex(link));

        return PARTITIONLESS_LINK_SIZE_BYTES;
    }

    /**
     * Writes a partitionless link to a buffer: first high 2 bytes, then low 4 bytes.
     *
     * @param buffer Buffer to write into.
     * @param link The link to write.
     */
    public static void writeToBuffer(ByteBuffer buffer, long link) {
        buffer.putShort((short) tag(link));

        buffer.putInt(pageIndex(link));
    }
}
