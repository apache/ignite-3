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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointPages;
import org.jetbrains.annotations.Nullable;

/**
 * Stateful class for page replacement of one page with write() delay. This allows to write page content without holding
 * segment write lock, which allows for a long time not to block the segment read lock due to IO writes when reading pages or writing dirty
 * pages at a checkpoint.
 *
 * <p>Usage:</p>
 * <ul>
 *     <li>On page replacement, invoke {@link #copyPageToTemporaryBuffer}.</li>
 *     <li>After releasing the segment write lock, invoke {@link #flushCopiedPageIfExists}.</li>
 * </ul>
 *
 * <p>Not thread safe.</p>
 */
public class DelayedDirtyPageWrite {
    /** Real flush dirty page implementation. */
    private final WriteDirtyPage flushDirtyPage;

    /** Page size. */
    private final int pageSize;

    /** Thread local with byte buffers. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc;

    /** Replacing pages tracker, used to register & unregister pages being written. */
    private final DelayedPageReplacementTracker tracker;

    private final PartitionDestructionLockManager partitionDestructionLockManager;

    /** Full page id to be written on {@link #flushCopiedPageIfExists}, {@code null} if nothing to write. */
    private @Nullable DirtyFullPageId fullPageId;

    /** Page memory to be used in {@link #flushCopiedPageIfExists}, {@code null} if nothing to write. */
    private @Nullable PersistentPageMemory pageMemory;

    /**
     * Dirty pages of the segment that need to be written at the current checkpoint or page replacement, {@code null} if nothing to write.
     */
    private @Nullable CheckpointPages checkpointPages;

    /**
     * Constructor.
     *
     * @param flushDirtyPage Real writer to IO write page to store.
     * @param byteBufThreadLoc Thread local buffers to use for pages copying.
     * @param pageSize Page size in bytes.
     * @param tracker Tracker to lock/unlock page reads.
     * @param partitionDestructionLockManager Partition Destruction Lock Manager.
     */
    DelayedDirtyPageWrite(
            WriteDirtyPage flushDirtyPage,
            ThreadLocal<ByteBuffer> byteBufThreadLoc,
            // TODO: IGNITE-17017 Move to common config
            int pageSize,
            DelayedPageReplacementTracker tracker,
            PartitionDestructionLockManager partitionDestructionLockManager
    ) {
        this.flushDirtyPage = flushDirtyPage;
        this.pageSize = pageSize;
        this.byteBufThreadLoc = byteBufThreadLoc;
        this.tracker = tracker;
        this.partitionDestructionLockManager = partitionDestructionLockManager;
    }

    /**
     * Copies a page to a temporary buffer on page replacement.
     *
     * @param pageMemory Persistent page memory for subsequent page IO writes.
     * @param pageId ID of the copied page.
     * @param originPageBuf Buffer with the full contents of the page being copied (from which we will copy).
     * @param checkpointPages Dirty pages of the segment that need to be written at the current checkpoint or page replacement.
     * @see #flushCopiedPageIfExists()
     */
    public void copyPageToTemporaryBuffer(
            PersistentPageMemory pageMemory,
            DirtyFullPageId pageId,
            ByteBuffer originPageBuf,
            CheckpointPages checkpointPages
    ) {
        tracker.lock(pageId.toFullPageId());

        ByteBuffer threadLocalBuf = byteBufThreadLoc.get();

        threadLocalBuf.rewind();

        long dstBufAddr = bufferAddress(threadLocalBuf);
        long srcBufAddr = bufferAddress(originPageBuf);

        copyMemory(srcBufAddr, dstBufAddr, pageSize);

        this.fullPageId = pageId;
        this.pageMemory = pageMemory;
        this.checkpointPages = checkpointPages;
    }

    /**
     * Flushes a previously copied page to disk if it was copied.
     *
     * @throws IgniteInternalCheckedException If write failed.
     * @see #copyPageToTemporaryBuffer
     */
    public void flushCopiedPageIfExists() throws IgniteInternalCheckedException {
        if (fullPageId == null) {
            return;
        }

        assert pageMemory != null : fullPageId;
        assert checkpointPages != null : fullPageId;

        Throwable errorOnWrite = null;

        Lock partitionDestructionLock = partitionDestructionLockManager.destructionLock(GroupPartitionId.convert(fullPageId)).readLock();

        partitionDestructionLock.lock();

        try {
            // Return value not needed for page replacement writes
            flushDirtyPage.write(pageMemory, fullPageId, byteBufThreadLoc.get());
        } catch (Throwable t) {
            errorOnWrite = t;

            throw t;
        } finally {
            partitionDestructionLock.unlock();

            checkpointPages.unblockFsyncOnPageReplacement(fullPageId, errorOnWrite);

            tracker.unlock(fullPageId.toFullPageId());

            fullPageId = null;
            pageMemory = null;
            checkpointPages = null;
        }
    }
}
