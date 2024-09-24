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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains dirty pages of the segment that will need to be written at a checkpoint or page replacement. It also contains helper
 * methods before writing pages.
 *
 * <p>For correct parallel operation of the checkpoint writer and page replacement, external synchronization must be used, for example,
 * segment read and write locks.</p>
 */
public class CheckpointPages {
    private final Set<FullPageId> pageIds;

    private final CheckpointProgressImpl checkpointProgress;

    /**
     * Constructor.
     *
     * @param pageIds Dirty page IDs in the segment that should be written at a checkpoint or page replacement.
     * @param checkpointProgress Progress of the current checkpoint at which the object was created.
     */
    public CheckpointPages(Set<FullPageId> pageIds, CheckpointProgress checkpointProgress) {
        this.pageIds = pageIds;
        this.checkpointProgress = (CheckpointProgressImpl) checkpointProgress;
    }

    /**
     * Checks if the page is available for replacement, without removing the page.
     *
     * <p>Page is available for replacement if the following conditions are met:</p>
     * <ul>
     *     <li>If the dirty page sorting phase is complete, otherwise we wait for it. This is necessary so that we can safely create
     *     partition delta files in which the dirty page order must be preserved.</li>
     *     <li>If the checkpoint dirty page writer has not started writing the page or has already written it.</li>
     *     <li>If the delta file fsync phase is not ready to start or is not in progress. This is necessary so that the data remains
     *     consistent after the fsync phase is complete. If the phase has not yet begun, we will block it until we complete the
     *     replacement.</li>
     * </ul>
     *
     * <p>It is expected that if the method returns {@code true}, it will not be invoked again for the same page ID, then
     * {@link #finishReplace} will be invoked later.</p>
     *
     * @param pageId Page ID of the replacement candidate.
     * @return {@code True} if the page is available for replacement, {@code false} if not.
     * @throws IgniteInternalCheckedException If any error occurred while waiting for the dirty page sorting phase to complete at a
     *      checkpoint.
     * @see #finishReplace(FullPageId, Throwable)
     */
    public boolean allowToReplace(FullPageId pageId) throws IgniteInternalCheckedException {
        try {
            // Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
            getUninterruptibly(checkpointProgress.futureFor(PAGES_SORTED));
        } catch (ExecutionException e) {
            throw new IgniteInternalCheckedException(e.getCause());
        } catch (CancellationException e) {
            throw new IgniteInternalCheckedException(e);
        }

        return pageIds.contains(pageId) && checkpointProgress.tryBlockFsyncOnPageReplacement(pageId);
    }

    /**
     * Finishes the replacement of a page previously allowed by {@link #allowToReplace}.
     *
     * <p>Unblocks the fsync delta file phase at the checkpoint. But it will only start when all unlocks that began before the phase are
     * completed.</p>
     *
     * <p>It is expected that if the {@link #allowToReplace} returns {@code true}, then current method will be invoked later.</p>
     *
     * @param pageId ID of the replaced page.
     * @param error Error on IO write of the replaced page, {@code null} if missing.
     * @see #allowToReplace(FullPageId)
     */
    public void finishReplace(FullPageId pageId, @Nullable Throwable error) {
        checkpointProgress.unblockFsyncOnPageReplacement(pageId, error);
    }

    /**
     * Returns {@code true} if the page has not yet been written by a checkpoint or page replacement.
     *
     * @param pageId Page ID for checking.
     */
    public boolean contains(FullPageId pageId) {
        return pageIds.contains(pageId);
    }

    /**
     * Removes a page ID that would be written at a checkpoint or page replacement. Must be invoked before writing a page to disk.
     *
     * @param pageId Page ID to remove.
     * @return {@code True} if the page was removed by the current method invoke, {@code false} if the page was already removed by another
     *      invoke or did not exist.
     */
    public boolean remove(FullPageId pageId) {
        return pageIds.remove(pageId);
    }

    /**
     * Adds a page ID that would be written at a checkpoint or page replacement. The code that invokes this method must ensure that the
     * added page is written at a checkpoint or page replacement. For example, at a checkpoint when an attempt to take a write lock for the
     * page fails.
     *
     * @param pageId Page ID to add.
     * @return {@code True} if the page was added by the current method invoke, {@code false} if the page was already exists.
     */
    public boolean add(FullPageId pageId) {
        return pageIds.add(pageId);
    }

    /** Returns the current size of all pages that will be written at a checkpoint or page replacement. */
    public int size() {
        return pageIds.size();
    }

    /**
     * Blocks physical destruction of partition.
     *
     * <p>When the intention to destroy partition appears, {@link FilePageStore#isMarkedToDestroy()} is set to {@code == true} and
     * {@link PersistentPageMemory#invalidate(int, int)} invoked at the beginning. And if there is a block, it waits for unblocking.
     * Then it destroys the partition, {@link FilePageStoreManager#getStore(GroupPartitionId)} will return {@code null}.</p>
     *
     * <p>It is recommended to use where physical destruction of the partition may have an impact, for example when writing dirty pages and
     * executing a fsync.</p>
     *
     * <p>To make sure that we can physically do something with the partition during a block, we will need to use approximately the
     * following code:</p>
     * <pre><code>
     *     checkpointProgress.blockPartitionDestruction(partitionId);
     *
     *     try {
     *         FilePageStore pageStore = FilePageStoreManager#getStore(partitionId);
     *
     *         if (pageStore == null || pageStore.isMarkedToDestroy()) {
     *             return;
     *         }
     *
     *         someAction(pageStore);
     *     } finally {
     *         checkpointProgress.unblockPartitionDestruction(partitionId);
     *     }
     * </code></pre>
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @see #unblockPartitionDestruction(GroupPartitionId)
     */
    public void blockPartitionDestruction(GroupPartitionId groupPartitionId) {
        checkpointProgress.blockPartitionDestruction(groupPartitionId);
    }

    /**
     * Unblocks physical destruction of partition.
     *
     * <p>As soon as the last thread makes an unlock, the physical destruction of the partition can immediately begin.</p>
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @see #blockPartitionDestruction(GroupPartitionId)
     */
    public void unblockPartitionDestruction(GroupPartitionId groupPartitionId) {
        checkpointProgress.unblockPartitionDestruction(groupPartitionId);
    }
}
