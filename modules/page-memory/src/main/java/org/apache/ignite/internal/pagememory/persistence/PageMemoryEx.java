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

package org.apache.ignite.internal.pagememory.persistence;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Page memory with some persistence related additions.
 */
//TODO IGNITE-16350 Improve javadoc in this class.
//TODO IGNITE-16350 Consider removing this interface.
public interface PageMemoryEx extends PageMemory {
    /**
     * Acquires a read lock associated with the given page.
     *
     * @param absPtr Absolute pointer to read lock.
     * @param pageId Page ID.
     * @param force Force flag.
     * @param touch Update page timestamp.
     * @return Pointer to the page read buffer.
     */
    long readLock(long absPtr, long pageId, boolean force, boolean touch);

    /**
     * Acquired a write lock on the page.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param restore Determines if the page is locked for restore memory (crash recovery).
     * @return Pointer to the page read buffer.
     */
    long writeLock(int grpId, long pageId, long page, boolean restore);

    /**
     * Releases locked page.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param dirtyFlag Determines whether the page was modified since the last checkpoint.
     * @param restore Determines if the page is locked for restore.
     */
    void writeUnlock(int grpId, long pageId, long page, boolean dirtyFlag, boolean restore);

    /**
     * Gets partition metadata page ID for specified grpId and partId.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Meta page for grpId and partId.
     */
    long partitionMetaPageId(int grpId, int partId);

    /**
     * Returns an absolute pointer to a page, associated with the given page ID.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param pageAllocated Flag is set if new page was allocated in offheap memory.
     * @return Page.
     * @throws IgniteInternalCheckedException If failed.
     * @see #acquirePage(int, long) Sets additional flag indicating that page was not found in memory and had to be allocated.
     */
    long acquirePage(int grpId, long pageId, AtomicBoolean pageAllocated) throws IgniteInternalCheckedException;

    /**
     * Returns an absolute pointer to a page, associated with the given page ID.
     *
     * @param grpId Group ID.
     * @param pageId Page id.
     * @param restore Get page for restore
     * @return Page.
     * @throws IgniteInternalCheckedException If failed.
     * @throws StorageException If page reading failed from storage.
     * @see #acquirePage(int, long) Will read page from file if it is not present in memory.
     */
    long acquirePage(int grpId, long pageId, IoStatisticsHolder statHldr, boolean restore) throws IgniteInternalCheckedException;

    /**
     * Marks partition as invalid / outdated.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return New partition generation (growing 1-based partition file version).
     */
    int invalidate(int grpId, int partId);

    /**
     * Clears internal metadata of destroyed group.
     *
     * @param grpId Group ID.
     */
    void onCacheGroupDestroyed(int grpId);

    /**
     * Total pages can be placed to memory.
     */
    long totalPages();
}
