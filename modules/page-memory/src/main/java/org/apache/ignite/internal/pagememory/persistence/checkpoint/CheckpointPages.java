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
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.storage.StorageException;

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
     * Checks if the page is available for replacement.
     *
     * <p>Page is available for replacement if the following conditions are met:</p>
     * <ul>
     *     <li>If the dirty page sorting phase is complete, otherwise we wait for it. This is necessary so that we can safely create
     *     partition delta files in which the dirty page order must be preserved.</li>
     *     <li>If the checkpoint dirty page writer has not started writing the page or has already written it.</li>
     *     <li>If the delta file fsync phase is not ready to start or is not in progress. This is necessary so that the data remains
     *     consistent after the fsync phase is complete.</li>
     * </ul>
     *
     * <p>It is expected that if the method returns true, it will not be invoked again for the same page ID.</p>
     *
     * @param pageId Page ID of the replacement candidate.
     * @return {@code True} if the page is available for replacement, {@code false} if not.
     * @throws StorageException If any error occurred while waiting for the dirty page sorting phase to complete at a checkpoint.
     */
    public boolean allowToReplace(FullPageId pageId) throws StorageException {
        try {
            // Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
            getUninterruptibly(checkpointProgress.futureFor(PAGES_SORTED));
        } catch (ExecutionException e) {
            throw new StorageException(e.getCause());
        } catch (CancellationException e) {
            throw new StorageException(e);
        }

        return pageIds.contains(pageId) && checkpointProgress.tryBlockFsyncOnPageReplacement(pageId);
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
}
