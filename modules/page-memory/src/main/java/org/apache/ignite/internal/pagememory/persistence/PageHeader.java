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
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.RELATIVE_PTR_MASK;
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
 * Page header.
 */
public class PageHeader {
    /** Page marker. */
    public static final long PAGE_MARKER = 0x0000000000000001L;

    /** Dirty flag. */
    private static final long DIRTY_FLAG = 0x0100000000000000L;

    /** Page relative pointer. Does not change once a page is allocated. */
    private static final int RELATIVE_PTR_OFFSET = 8;

    /** Page ID offset. */
    private static final int PAGE_ID_OFFSET = 16;

    /** Page group ID offset. */
    private static final int PAGE_GROUP_ID_OFFSET = 24;

    /** Page pin counter offset. */
    private static final int PAGE_PIN_CNT_OFFSET = 28;

    /** Page temp copy buffer relative pointer offset. */
    private static final int PAGE_TMP_BUF_OFFSET = 40;

    /**
     * Initializes the header of the page.
     *
     * @param absPtr Absolute pointer to initialize.
     * @param relative Relative pointer to write.
     */
    public static void initNew(long absPtr, long relative) {
        relative(absPtr, relative);

        tempBufferPointer(absPtr, INVALID_REL_PTR);

        putLong(absPtr, PAGE_MARKER);
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
     * Returns flag value.
     *
     * @param absPtr Absolute pointer.
     * @param flag Flag mask.
     */
    private static boolean flag(long absPtr, long flag) {
        assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
        assert Long.bitCount(flag) == 1;

        long relPtrWithFlags = getLong(absPtr + RELATIVE_PTR_OFFSET);

        return (relPtrWithFlags & flag) != 0;
    }

    /**
     * Sets flag value.
     *
     * @param absPtr Absolute pointer.
     * @param flag Flag mask.
     * @param set New flag value.
     * @return Previous flag value.
     */
    private static boolean flag(long absPtr, long flag, boolean set) {
        assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
        assert Long.bitCount(flag) == 1;

        long relPtrWithFlags = getLong(absPtr + RELATIVE_PTR_OFFSET);

        boolean was = (relPtrWithFlags & flag) != 0;

        if (set) {
            relPtrWithFlags |= flag;
        } else {
            relPtrWithFlags &= ~flag;
        }

        putLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

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
    public static int pinCount(long absPtr) {
        return getIntVolatile(null, absPtr);
    }

    /**
     * Reads relative pointer from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     */
    public static long readRelative(long absPtr) {
        return getLong(absPtr + RELATIVE_PTR_OFFSET) & RELATIVE_PTR_MASK;
    }

    /**
     * Writes relative pointer to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param relPtr Relative pointer to write.
     */
    public static void relative(long absPtr, long relPtr) {
        putLong(absPtr + RELATIVE_PTR_OFFSET, relPtr & RELATIVE_PTR_MASK);
    }

    /**
     * Volatile write for current timestamp to page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @param tstamp Timestamp.
     */
    public static void writeTimestamp(final long absPtr, long tstamp) {
        tstamp &= 0xFFFFFFFFFFFFFF00L;

        putLongVolatile(null, absPtr, tstamp | 0x01);
    }

    /**
     * Read for timestamp from page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @return Timestamp.
     */
    public static long readTimestamp(final long absPtr) {
        long markerAndTs = getLong(absPtr);

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
    public static void tempBufferPointer(long absPtr, long tmpRelPtr) {
        putLong(absPtr + PAGE_TMP_BUF_OFFSET, tmpRelPtr);
    }

    /**
     * Gets pointer to checkpoint buffer or {@link PersistentPageMemory#INVALID_REL_PTR} if page is not copied to checkpoint buffer.
     *
     * @param absPtr Page absolute pointer.
     * @return Temp buffer relative pointer.
     */
    public static long tempBufferPointer(long absPtr) {
        return getLong(absPtr + PAGE_TMP_BUF_OFFSET);
    }

    /**
     * Reads page ID from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Page ID written to the page.
     */
    public static long readPageId(long absPtr) {
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
    private static int readPageGroupId(final long absPtr) {
        return getInt(absPtr + PAGE_GROUP_ID_OFFSET);
    }

    /**
     * Writes group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param grpId Group ID to write.
     */
    private static void pageGroupId(final long absPtr, final int grpId) {
        putInt(absPtr + PAGE_GROUP_ID_OFFSET, grpId);
    }

    /**
     * Reads page ID and group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Full page ID written to the page.
     */
    public static FullPageId fullPageId(final long absPtr) {
        return new FullPageId(readPageId(absPtr), readPageGroupId(absPtr));
    }

    /**
     * Writes page ID and group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param fullPageId Full page ID to write.
     */
    public static void fullPageId(final long absPtr, final FullPageId fullPageId) {
        pageId(absPtr, fullPageId.pageId());

        pageGroupId(absPtr, fullPageId.groupId());
    }
}
