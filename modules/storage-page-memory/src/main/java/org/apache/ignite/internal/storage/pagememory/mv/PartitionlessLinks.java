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

import static org.apache.ignite.internal.pagememory.util.PageUtils.getInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getShort;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/**
 * Handling of <em>partitionless links</em>, that is, page memory links from which partition ID is removed.
 * They are used to spare storage space in cases when we know the partition ID from the context.
 *
 * @see PageIdUtils#link(long, int)
 */
public class PartitionlessLinks {
    /**
     * Number of bytes a partitionless link takes in storage.
     */
    public static final int PARTITIONLESS_LINK_SIZE_BYTES = 6;

    /**
     * Converts a full link to partitionless link by removing the partition ID from it.
     *
     * @param link full link
     * @return partitionless link
     * @see PageIdUtils#link(long, int)
     */
    public static long removePartitionIdFromLink(long link) {
        return ((link >> PageIdUtils.PART_ID_SIZE) & 0x0000FFFF00000000L)
                | (link & 0xFFFFFFFFL);
    }

    /**
     * Converts a partitionless link to a full link by inserting partition ID at the corresponding position.
     *
     * @param partitionlessLink link without partition
     * @param partitionId       partition ID to insert
     * @return full link
     * @see PageIdUtils#link(long, int)
     */
    public static long addPartitionIdToPartititionlessLink(long partitionlessLink, int partitionId) {
        return (partitionlessLink << PageIdUtils.PART_ID_SIZE) & 0xFFFF000000000000L
                | ((((long) partitionId) << PageIdUtils.PAGE_IDX_SIZE) & 0xFFFF00000000L)
                | (partitionlessLink & PageIdUtils.PAGE_IDX_MASK);
    }

    /**
     * Returns high 16 bits of the 6 bytes representing a partitionless link.
     *
     * @param partitionlessLink link without partition info
     * @return high 16 bits
     * @see #assemble(short, int)
     */
    public static short high16Bits(long partitionlessLink) {
        return (short) (partitionlessLink >> Integer.SIZE);
    }

    /**
     * Returns low 32 bits of the 6 bytes representing a partitionless link.
     *
     * @param partitionlessLink link without partition info
     * @return low 32 bits
     * @see #assemble(short, int)
     */
    public static int low32Bits(long partitionlessLink) {
        return (int) (partitionlessLink & PageIdUtils.PAGE_IDX_MASK);
    }

    /**
     * Assembles a partitionless link from high 16 bits and 32 low bits of its representation.
     *
     * @param high16    high 16 bits
     * @param low32     low 32 bits
     * @return reconstructed partitionless link
     * @see #high16Bits(long)
     * @see #low32Bits(long)
     */
    public static long assemble(short high16, int low32) {
        return ((((long) high16) & 0xFFFF) << 32)
                | (((long) low32) & 0xFFFFFFFFL);
    }

    static long readFromMemory(long pageAddr, int offset) {
        short nextLinkHigh16 = getShort(pageAddr, offset);
        int nextLinkLow32 = getInt(pageAddr, offset + Short.BYTES);

        return assemble(nextLinkHigh16, nextLinkLow32);
    }

    static long readFromBuffer(ByteBuffer buffer) {
        short nextLinkHigh16 = buffer.getShort();
        int nextLinkLow32 = buffer.getInt();

        return assemble(nextLinkHigh16, nextLinkLow32);
    }

    /**
     * Writes a partitionless link to memory: first high 2 bytes, then low 4 bytes.
     *
     * @param addr              address in memory where to start
     * @param partitionlessLink the link to write
     * @return number of bytes written (equal to {@link #PARTITIONLESS_LINK_SIZE_BYTES})
     */
    public static long writeToMemory(long addr, long partitionlessLink) {
        putShort(addr, 0, PartitionlessLinks.high16Bits(partitionlessLink));
        addr += Short.BYTES;
        putInt(addr, 0, PartitionlessLinks.low32Bits(partitionlessLink));

        return PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
    }

    private PartitionlessLinks() {
        // prevent instantiation
    }
}
