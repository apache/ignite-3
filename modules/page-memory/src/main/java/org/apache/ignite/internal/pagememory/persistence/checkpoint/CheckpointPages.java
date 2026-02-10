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
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.jetbrains.annotations.Nullable;

/**
 * Class contains dirty pages of the segment that will need to be written at a checkpoint or page replacement. It also contains helper
 * methods before writing pages.
 *
 * <p>For correct parallel operation of the checkpoint writer and page replacement, external synchronization must be used.</p>
 *
 * @see PersistentPageMemory#checkpointWritePage
 * @see PersistentPageMemory.Segment#tryToRemovePage
 */
public class CheckpointPages {
    private final Set<DirtyFullPageId> pageIds;

    private final CheckpointProgressImpl checkpointProgress;

    /**
     * Constructor.
     *
     * @param pageIds Dirty page IDs in the segment that should be written at a checkpoint or page replacement.
     * @param checkpointProgress Progress of the current checkpoint at which the object was created.
     */
    public CheckpointPages(Set<DirtyFullPageId> pageIds, CheckpointProgress checkpointProgress) {
        this.pageIds = pageIds;
        this.checkpointProgress = (CheckpointProgressImpl) checkpointProgress;
    }

    /**
     * Removes a page ID that would be written at page replacement. Must be invoked before writing a page to disk.
     *
     * <p>Page will be removed only if the dirty page sorting phase at the checkpoint has completed or will synchronously wait for it to
     * complete.</p>
     *
     * <p>To keep the data consistent, we need to use {@link #blockFsyncOnPageReplacement} and {@link #unblockFsyncOnPageReplacement} after
     * calling the current method to prevent the fsync checkpoint phase from starting.</p>
     *
     * @param pageId Page ID to remove.
     * @return {@code True} if the page was removed by the current method invoke, {@code false} if the page was already removed by another
     *      removes or did not exist.
     * @throws IgniteInternalCheckedException If any error occurred while waiting for the dirty page sorting phase to complete at a
     *         checkpoint.
     * @see #removeOnCheckpoint
     * @see #blockFsyncOnPageReplacement
     * @see #unblockFsyncOnPageReplacement
     */
    public boolean removeOnPageReplacement(DirtyFullPageId pageId) throws IgniteInternalCheckedException {
        try {
            // Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
            getUninterruptibly(checkpointProgress.futureFor(PAGES_SORTED));
        } catch (ExecutionException e) {
            throw new IgniteInternalCheckedException(e.getCause());
        } catch (CancellationException e) {
            throw new IgniteInternalCheckedException(e);
        }

        return pageIds.remove(pageId);
    }

    /**
     * Removes a page ID that would be written at checkpoint. Must be invoked before writing a page to disk.
     *
     * <p>We don't need to block the fsync phase of the checkpoint, since it won't start until all dirty pages have been written at the
     * checkpoint, except those for which page replacement has occurred.</p>
     *
     * @param pageId Page ID to remove.
     * @return {@code True} if the page was removed by the current method invoke, {@code false} if the page was already removed by another
     *      removes or did not exist.
     * @see #removeOnPageReplacement
     */
    public boolean removeOnCheckpoint(DirtyFullPageId pageId) {
        return pageIds.remove(pageId);
    }

    /**
     * Returns {@code true} if the page has not yet been written by a checkpoint or page replacement.
     *
     * @param pageId Page ID for checking.
     */
    public boolean contains(DirtyFullPageId pageId) {
        return pageIds.contains(pageId);
    }

    /** Returns the current size of all pages that will be written at a checkpoint or page replacement. */
    public int size() {
        return pageIds.size();
    }

    /**
     * Block the start of the fsync phase at a checkpoint before replacing the page.
     *
     * <p>It is expected that the method will be invoked once and after that the {@link #unblockFsyncOnPageReplacement} will be invoked on
     * the same page.</p>
     *
     * @param pageId Page ID for which page replacement will begin.
     * @see #unblockFsyncOnPageReplacement
     * @see #removeOnPageReplacement
     */
    public void blockFsyncOnPageReplacement(DirtyFullPageId pageId) {
        checkpointProgress.blockFsyncOnPageReplacement(pageId);
    }

    /**
     * Unblocks the start of the fsync phase at a checkpoint after the page replacement is completed.
     *
     * <p>It is expected that the method will be invoked once and after the {@link #blockFsyncOnPageReplacement} for same page ID.</p>
     *
     * <p>The fsync phase will only be started after page replacement has been completed for all pages for which
     * {@link #blockFsyncOnPageReplacement} was invoked or no page replacement occurred at all.</p>
     *
     * <p>The method must be invoked even if any error occurred, so as not to hang a checkpoint.</p>
     *
     * @param pageId Page ID for which the page replacement has ended.
     * @param error Error on page replacement, {@code null} if missing.
     * @see #blockFsyncOnPageReplacement
     * @see #removeOnPageReplacement
     */
    public void unblockFsyncOnPageReplacement(DirtyFullPageId pageId, @Nullable Throwable error) {
        checkpointProgress.unblockFsyncOnPageReplacement(pageId, error);
    }
}
