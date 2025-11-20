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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.INVALID_REL_PTR;
import static org.apache.ignite.internal.util.GridUnsafe.decrementAndGetInt;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getIntVolatile;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.incrementAndGetInt;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.GridUnsafe.putLongVolatile;

import org.apache.ignite.internal.pagememory.FullPageId;

/**
 * Helper class for working with the page header that is stored in memory for {@link PersistentPageMemory}.
 *
 * <p>Page header has the following structure:</p>
 * <pre>
 * +-----------------+---------------------+--------+--------+---------+----------+----------+----------------------+
 * |     8 bytes     |       4 bytes       |4 bytes |8 bytes |4 bytes  |4 bytes   |8 bytes   |       8 bytes        |
 * +-----------------+---------------------+--------+--------+---------+----------+----------+----------------------+
 * |Marker/Timestamp |Partition generation |Flags   |Page ID |Group ID |Pin count |Lock data |Checkpoint tmp buffer |
 * +-----------------+---------------------+--------+--------+---------+----------+----------+----------------------+
 * </pre>
 *
 * <p>Additional information:</p>
 * <ul>
 *     <li>Size of the page header in {@link #PAGE_OVERHEAD}.</li>
 *     <li>Flags currently store only one value, whether the page is dirty or not. Only one byte is used for now, the rest can be reused
 *     later, we do not remove them only for alignment.</li>
 * </ul>
 */
public class PageHeader {
    /** Page marker. */
    static final long PAGE_MARKER = 0x0000000000000001L;

    /** Dirty flag. */
    private static final int DIRTY_FLAG = 0x01000000;

    /** Page header validity flag. */
    private static final int HEADER_IS_VALID_FLAG = 0x02000000;

    /** Unknown partition generation. */
    static final int UNKNOWN_PARTITION_GENERATION = -1;

    /** 8b Marker/timestamp, 4b Partition generation, 4b flags, 8b Page ID, 4b Group ID, 4b Pin count, 8b Lock, 8b Temporary buffer. */
    public static final int PAGE_OVERHEAD = 48;

    /** Marker or timestamp offset. */
    private static final int MARKER_OR_TIMESTAMP_OFFSET = 0;

    /** Partition generation offset. */
    private static final int PARTITION_GENERATION_OFFSET = 8;

    /** Flags offset. */
    private static final int FLAGS_OFFSET = 12;

    /** Page ID offset. */
    private static final int PAGE_ID_OFFSET = 16;

    /** Page group ID offset. */
    private static final int PAGE_GROUP_ID_OFFSET = 24;

    /** Page pin counter offset. */
    private static final int PAGE_PIN_CNT_OFFSET = 28;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** Page temp copy buffer relative pointer offset. */
    private static final int PAGE_TMP_BUF_OFFSET = 40;

    /**
     * Initializes the header of the page.
     *
     * @param absPtr Absolute pointer to initialize.
     */
    public static void initNew(long absPtr) {
        partitionGeneration(absPtr, UNKNOWN_PARTITION_GENERATION);

        tempBufferPointer(absPtr, INVALID_REL_PTR);

        putLong(absPtr + MARKER_OR_TIMESTAMP_OFFSET, PAGE_MARKER);
        putInt(absPtr + PAGE_PIN_CNT_OFFSET, 0);
    }

    /**
     * Returns value of dirty flag.
     *
     * @param absPtr Absolute pointer.
     */
    public static boolean dirty(long absPtr) {
        return flag(absPtr, DIRTY_FLAG);
    }

    /**
     * Updates value of dirty flag.
     *
     * @param absPtr Page absolute pointer.
     * @param dirty Dirty flag.
     * @return Previous value of dirty flag.
     */
    public static boolean dirty(long absPtr, boolean dirty) {
        return flag(absPtr, DIRTY_FLAG, dirty);
    }

    /**
     * Reads the value of a header validity flag.
     */
    public static boolean headerValidity(long absPtr) {
        return flag(absPtr, HEADER_IS_VALID_FLAG);
    }

    /**
     * Updates the value of a header validity flag.
     */
    public static void headerValidity(long absPtr, boolean valid) {
        flag(absPtr, HEADER_IS_VALID_FLAG, valid);
    }

    /**
     * Returns flag value.
     *
     * @param absPtr Absolute pointer.
     * @param flagMask Flag mask.
     */
    private static boolean flag(long absPtr, int flagMask) {
        assert (flagMask & 0xFFFFFF) == 0 : Integer.toHexString(flagMask);
        assert Long.bitCount(flagMask) == 1 : Integer.toHexString(flagMask);

        int flags = getInt(absPtr + FLAGS_OFFSET);

        return (flags & flagMask) != 0;
    }

    /**
     * Sets flag value.
     *
     * @param absPtr Absolute pointer.
     * @param flagMask Flag mask.
     * @param set New flag value.
     * @return Previous flag value.
     */
    private static boolean flag(long absPtr, int flagMask, boolean set) {
        assert (flagMask & 0xFFFFFF) == 0 : Integer.toHexString(flagMask);
        assert Long.bitCount(flagMask) == 1 : Integer.toHexString(flagMask);

        int flags = getInt(absPtr + FLAGS_OFFSET);

        boolean was = (flags & flagMask) != 0;

        if (set) {
            flags |= flagMask;
        } else {
            flags &= ~flagMask;
        }

        putInt(absPtr + FLAGS_OFFSET, flags);

        return was;
    }

    /**
     * Checks if page is pinned.
     *
     * @param absPtr Page pointer.
     */
    public static boolean isAcquired(long absPtr) {
        return getInt(absPtr + PAGE_PIN_CNT_OFFSET) > 0;
    }

    /**
     * Acquires a page.
     *
     * @param absPtr Absolute pointer.
     * @return Number of acquires for the page.
     */
    public static int acquirePage(long absPtr) {
        return incrementAndGetInt(absPtr + PAGE_PIN_CNT_OFFSET);
    }

    /**
     * Releases the page.
     *
     * @param absPtr Absolute pointer.
     * @return Number of acquires for the page.
     */
    public static int releasePage(long absPtr) {
        return decrementAndGetInt(absPtr + PAGE_PIN_CNT_OFFSET);
    }

    /**
     * Returns number of acquires for the page.
     *
     * @param absPtr Absolute pointer.
     */
    static int pinCount(long absPtr) {
        return getIntVolatile(null, absPtr + PAGE_PIN_CNT_OFFSET);
    }

    /**
     * Volatile write for current timestamp to page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @param timestamp Timestamp.
     */
    public static void timestamp(long absPtr, long timestamp) {
        timestamp &= 0xFFFFFFFFFFFFFF00L;

        putLongVolatile(null, absPtr + MARKER_OR_TIMESTAMP_OFFSET, timestamp | 0x01);
    }

    /**
     * Read for timestamp from page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @return Timestamp.
     */
    public static long timestamp(long absPtr) {
        long markerAndTs = getLong(absPtr + MARKER_OR_TIMESTAMP_OFFSET);

        // Clear last byte as it is occupied by page marker.
        return markerAndTs & ~0xFF;
    }

    /**
     * Sets pointer to checkpoint buffer.
     *
     * @param absPtr Page absolute pointer.
     * @param tmpRelPtr Temp buffer relative pointer or {@link PersistentPageMemory#INVALID_REL_PTR} if page is not copied to checkpoint
     *      buffer.
     */
    static void tempBufferPointer(long absPtr, long tmpRelPtr) {
        putLong(absPtr + PAGE_TMP_BUF_OFFSET, tmpRelPtr);
    }

    /**
     * Gets pointer to checkpoint buffer or {@link PersistentPageMemory#INVALID_REL_PTR} if page is not copied to checkpoint buffer.
     *
     * @param absPtr Page absolute pointer.
     * @return Temp buffer relative pointer.
     */
    static long tempBufferPointer(long absPtr) {
        return getLong(absPtr + PAGE_TMP_BUF_OFFSET);
    }

    /**
     * Reads page ID from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Page ID written to the page.
     */
    static long pageId(long absPtr) {
        return getLong(absPtr + PAGE_ID_OFFSET);
    }

    /**
     * Writes page ID to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param pageId Page ID to write.
     */
    private static void pageId(long absPtr, long pageId) {
        putLong(absPtr + PAGE_ID_OFFSET, pageId);
    }

    /**
     * Reads group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Group ID written to the page.
     */
    private static int pageGroupId(long absPtr) {
        return getInt(absPtr + PAGE_GROUP_ID_OFFSET);
    }

    /**
     * Writes group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param grpId Group ID to write.
     */
    private static void pageGroupId(long absPtr, int grpId) {
        putInt(absPtr + PAGE_GROUP_ID_OFFSET, grpId);
    }

    /**
     * Reads page ID and group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Full page ID written to the page.
     */
    public static FullPageId fullPageId(long absPtr) {
        return new FullPageId(pageId(absPtr), pageGroupId(absPtr));
    }

    /**
     * Writes page ID and group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param fullPageId Full page ID to write.
     */
    public static void fullPageId(long absPtr, FullPageId fullPageId) {
        pageId(absPtr, fullPageId.pageId());

        pageGroupId(absPtr, fullPageId.groupId());
    }

    /**
     * Reads partition generation from page header, {@link #UNKNOWN_PARTITION_GENERATION} if the partition generation was not set.
     *
     * @param absPtr Absolute memory pointer to the page header.
     */
    public static int partitionGeneration(long absPtr) {
        return getInt(absPtr + PARTITION_GENERATION_OFFSET);
    }

    /**
     * Writes partition generation to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param partitionGeneration Partition generation, strictly positive or {@link #UNKNOWN_PARTITION_GENERATION} if reset is required.
     */
    static void partitionGeneration(long absPtr, int partitionGeneration) {
        assert partitionGeneration > 0 || partitionGeneration == UNKNOWN_PARTITION_GENERATION : partitionGeneration;

        putInt(absPtr + PARTITION_GENERATION_OFFSET, partitionGeneration);
    }

    /**
     * Reads dirty full page ID from page header.
     *
     * @param absPtr Absolute memory pointer to the page header.
     */
    public static DirtyFullPageId dirtyFullPageId(long absPtr) {
        return new DirtyFullPageId(pageId(absPtr), pageGroupId(absPtr), partitionGeneration(absPtr));
    }

    /**
     * Writes dirty full page ID to page header.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param dirtyFullPageId Dirty full page ID to write.
     */
    public static void dirtyFullPageId(long absPtr, DirtyFullPageId dirtyFullPageId) {
        pageId(absPtr, dirtyFullPageId.pageId());
        pageGroupId(absPtr, dirtyFullPageId.groupId());
        partitionGeneration(absPtr, dirtyFullPageId.partitionGeneration());
    }
}
