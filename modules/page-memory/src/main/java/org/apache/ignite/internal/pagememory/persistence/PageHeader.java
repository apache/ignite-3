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
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import org.apache.ignite.internal.pagememory.FullPageId;

/**
 * Helper class for working with the page header that is stored in memory for {@link PersistentPageMemory}.
 *
 * <p>Page header has the following structure:</p>
 * <pre>
 * +-----------------+---------------------+--------+--------+---------+--------------+----------+----------------------+
 * |     8 bytes     |       4 bytes       |4 bytes |8 bytes |4 bytes  |4 bytes       |8 bytes   |       8 bytes        |
 * +-----------------+---------------------+--------+--------+---------+--------------+----------+----------------------+
 * |Marker/Timestamp |Partition generation |Flags   |Page ID |Group ID |Acquire count |Lock data |Checkpoint tmp buffer |
 * +-----------------+---------------------+--------+--------+---------+--------------+----------+----------------------+
 * </pre>
 *
 * <p>Additional information:</p>
 * <ul>
 *     <li>Size of the page header in {@link #PAGE_OVERHEAD}.</li>
 *     <li>Flags currently store only one value, whether the page is dirty or not. Only one byte is used for now, the rest can be reused
 *     later, we do not remove them only for alignment.</li>
 * </ul>
 */
// TODO: IGNITE-26216 заиспользовать уже
public class PageHeader {
    /** Page marker. */
    private static final long PAGE_MARKER = 0x0000000000000001L;

    /** Dirty flag mask. */
    private static final int DIRTY_FLAG_MASK = 0x00000001;

    /** Unknown partition generation. */
    static final int UNKNOWN_PARTITION_GENERATION = -1;

    /**
     * Page overhead in bytes.
     * <ol>
     *     <li>8 bytes - Marker/Timestamp.</li>
     *     <li>4 bytes - Partition generation.</li>
     *     <li>4 bytes - Flags.</li>
     *     <li>8 bytes - Page ID.</li>
     *     <li>4 bytes - Page group ID.</li>
     *     <li>4 bytes - Count of page acquires.</li>
     *     <li>8 bytes - Page lack data.</li>
     *     <li>8 bytes - Checkpoint temporal copy buffer relative pointer.</li>
     * </ol>
     */
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
    private static final int GROUP_ID_OFFSET = 24;

    /** Page acquire counter offset. */
    private static final int ACQUIRE_COUNT_OFFSET = 28;

    /** Page lock data offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** Page temporal copy buffer relative pointer offset. */
    private static final int CHECKPOINT_TMP_BUFFER_OFFSET = 40;

    /**
     * Initializes the header of page.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    public static void initNew(long absPtr) {
        writePageMarker(absPtr);

        writePartitionGeneration(absPtr, UNKNOWN_PARTITION_GENERATION);

        putInt(absPtr + ACQUIRE_COUNT_OFFSET, 0);

        writeCheckpointTempBufferRelativePointer(absPtr, INVALID_REL_PTR);
    }

    /**
     * Reads value of dirty flag from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    public static boolean readDirtyFlag(long absPtr) {
        return readFlag(absPtr, DIRTY_FLAG_MASK);
    }

    /**
     * Write value of dirty flag to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param dirty Value dirty flag.
     * @return Previous value of dirty flag.
     */
    public static boolean writeDirtyFlag(long absPtr, boolean dirty) {
        return writeFlag(absPtr, DIRTY_FLAG_MASK, dirty);
    }

    /**
     * Reads flag value from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param flagMask Flag mask.
     */
    private static boolean readFlag(long absPtr, int flagMask) {
        assert Integer.bitCount(flagMask) == 1 : hexLong(flagMask);

        int flags = getInt(absPtr + FLAGS_OFFSET);

        return (flags & flagMask) != 0;
    }

    /**
     * Writes flag value to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param flagMask Flag mask.
     * @param set New flag value.
     * @return Previous flag value.
     */
    private static boolean writeFlag(long absPtr, int flagMask, boolean set) {
        assert Integer.bitCount(flagMask) == 1 : hexLong(flagMask);

        int flags = getInt(absPtr + FLAGS_OFFSET);

        boolean was = (flags & flagMask) != 0;

        if (set) {
            flags |= flagMask;
        } else {
            flags &= ~flagMask;
        }

        putLong(absPtr + FLAGS_OFFSET, flags);

        return was;
    }

    /**
     * Checks if page is pinned from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    public static boolean isAcquired(long absPtr) {
        return getInt(absPtr + ACQUIRE_COUNT_OFFSET) > 0;
    }

    /**
     * Atomically acquires a page in page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @return Number of acquires for page.
     */
    public static int acquirePage(long absPtr) {
        return incrementAndGetInt(absPtr + ACQUIRE_COUNT_OFFSET);
    }

    /**
     * Atomically releases the page in page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @return Number of acquires for page.
     */
    public static int releasePage(long absPtr) {
        return decrementAndGetInt(absPtr + ACQUIRE_COUNT_OFFSET);
    }

    /**
     * Volatile reads count of acquires for the page from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    static int readAcquiresCount(long absPtr) {
        return getIntVolatile(null, absPtr + ACQUIRE_COUNT_OFFSET);
    }

    /**
     * Volatile writes timestamp to page header.
     *
     * <p>It is written without the last byte, to avoid ABA problems see {@link PagePool#borrowOrAllocateFreePage} in
     * {@code borrowFreePage}.</p>
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param timestamp Timestamp.
     */
    static void writeTimestamp(long absPtr, long timestamp) {
        timestamp &= 0xFFFFFFFFFFFFFF00L;

        putLongVolatile(null, absPtr + MARKER_OR_TIMESTAMP_OFFSET, timestamp | 0x01);
    }

    /**
     * Reads timestamp from page header.
     *
     * <p>It is read without the last byte, to avoid ABA problems see {@link PagePool#borrowOrAllocateFreePage} in
     * {@code borrowFreePage}.</p>
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    public static long readTimestamp(long absPtr) {
        long markerAndTs = getLong(absPtr + MARKER_OR_TIMESTAMP_OFFSET);

        // Clear last byte as it is occupied by page marker.
        return markerAndTs & ~0xFF;
    }

    /**
     * Writes relative pointer to checkpoint temporal copy buffer to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param tmpRelPtr Temporal copy buffer relative pointer or {@link PersistentPageMemory#INVALID_REL_PTR} if page is not copied
     *         to checkpoint buffer.
     */
    static void writeCheckpointTempBufferRelativePointer(long absPtr, long tmpRelPtr) {
        putLong(absPtr + CHECKPOINT_TMP_BUFFER_OFFSET, tmpRelPtr);
    }

    /**
     * Reads relative pointer to checkpoint temporal copy buffer or {@link PersistentPageMemory#INVALID_REL_PTR} if page is not copied to
     * checkpoint buffer from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    static long readCheckpointTempBufferRelativePointer(long absPtr) {
        return getLong(absPtr + CHECKPOINT_TMP_BUFFER_OFFSET);
    }

    /**
     * Reads page ID from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    static long readPageId(long absPtr) {
        return getLong(absPtr + PAGE_ID_OFFSET);
    }

    /**
     * Writes page ID to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param pageId Page ID.
     */
    private static void writePageId(long absPtr, long pageId) {
        putLong(absPtr + PAGE_ID_OFFSET, pageId);
    }

    /**
     * Reads page group ID from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @return Page group ID.
     */
    private static int readPageGroupId(long absPtr) {
        return getInt(absPtr + GROUP_ID_OFFSET);
    }

    /**
     * Writes page group ID to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param groupId Page group ID.
     */
    private static void writePageGroupId(long absPtr, int groupId) {
        putInt(absPtr + GROUP_ID_OFFSET, groupId);
    }

    /**
     * Reads full page ID from page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    public static FullPageId readFullPageId(long absPtr) {
        return new FullPageId(readPageId(absPtr), readPageGroupId(absPtr));
    }

    /**
     * Writes full page ID to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param fullPageId Full page ID.
     */
    static void writeFullPageId(long absPtr, FullPageId fullPageId) {
        writePageId(absPtr, fullPageId.pageId());

        writePageGroupId(absPtr, fullPageId.groupId());
    }

    /**
     * Volatile writes a page marker to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    static void writePageMarker(long absPtr) {
        putLongVolatile(null, absPtr + MARKER_OR_TIMESTAMP_OFFSET, PAGE_MARKER);
    }

    /**
     * Reads partition generation from page header, {@link #UNKNOWN_PARTITION_GENERATION} if the partition generation was not set.
     *
     * @param absPtr Absolute memory pointer to page header.
     */
    static int readPartitionGeneration(long absPtr) {
        return getInt(absPtr + PARTITION_GENERATION_OFFSET);
    }

    /**
     * Writes partition generation to page header.
     *
     * @param absPtr Absolute memory pointer to page header.
     * @param partitionGeneration Partition generation, strictly positive or {@link #UNKNOWN_PARTITION_GENERATION} if reset is required.
     */
    static void writePartitionGeneration(long absPtr, int partitionGeneration) {
        assert partitionGeneration > 0 || partitionGeneration == UNKNOWN_PARTITION_GENERATION : partitionGeneration;

        putInt(absPtr + PARTITION_GENERATION_OFFSET, partitionGeneration);
    }
}
