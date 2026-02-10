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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for synchronizing page replacement and the beginning of the fsync phase at a checkpoint.
 *
 * <p>For data consistency, it is important for us that the fsync delta file phase begins strictly after all page replacements are
 * completed.</p>
 *
 * <p>Usage:</p>
 * <ul>
 *     <li>{@link #block} - before you need to perform a page replacement.</li>
 *     <li>{@link #unblock} - after the page replacement has finished and written to disk. The method must be invoked even if any error
 *     occurred, so as not to hang a checkpoint.</li>
 *     <li>{@link #stopBlocking} - must be invoked before the start of the fsync phase on the checkpoint and wait for the future to
 *     complete in order to safely perform the phase.</li>
 * </ul>
 *
 * <p>Thread safe.</p>
 */
class CheckpointPageReplacement {
    /** IDs of pages for which page replacement is in progress. */
    private final Set<DirtyFullPageId> pageIds = ConcurrentHashMap.newKeySet();

    private final CompletableFuture<Void> stopBlockingFuture = new CompletableFuture<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Block the start of the fsync phase at a checkpoint before replacing the page.
     *
     * <p>It is expected that the method will be invoked once and after that the {@link #unblock} will be invoked on the same page.</p>
     *
     * <p>It is expected that the method will not be invoked after {@link #stopBlocking}, since by the start of the fsync phase, write
     * dirty pages at the checkpoint should be complete and no new page replacements should be started.</p>
     *
     * @param pageId Page ID for which page replacement will begin.
     * @see #unblock
     * @see #stopBlocking()
     */
    void block(DirtyFullPageId pageId) {
        boolean enterBusy = busyLock.enterBusy();

        assert enterBusy : "Method should not be invoked after the fsync phase has started for any page: " + pageId;

        try {
            boolean added = pageIds.add(pageId);

            assert added : "Page is already in the process of being replaced: " + pageId;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Unblocks the start of the fsync phase at a checkpoint after the page replacement is completed.
     *
     * <p>It is expected that the method will be invoked once and after the {@link #block} for same page ID.</p>
     *
     * <p>The fsync phase will only be started after page replacement has been completed for all pages for which
     * {@link #block} was invoked before {@link #stopBlocking} was invoked, or no page replacement occurred at all.</p>
     *
     * <p>If an error occurs on any page replacement during one checkpoint, the future from {@link #stopBlocking} will complete with the
     * first error.</p>
     *
     * <p>The method must be invoked even if any error occurred, so as not to hang a checkpoint.</p>
     *
     * @param pageId Page ID for which the page replacement has ended.
     * @param error Error on page replacement, {@code null} if missing.
     * @see #block
     * @see #stopBlocking()
     */
    void unblock(DirtyFullPageId pageId, @Nullable Throwable error) {
        boolean removed = pageIds.remove(pageId);

        assert removed : "Replacement for the page either did not start or ended: " + pageId;

        if (error != null) {
            stopBlockingFuture.completeExceptionally(error);

            return;
        }

        if (!busyLock.enterBusy()) {
            if (pageIds.isEmpty()) {
                stopBlockingFuture.complete(null);
            }
        } else {
            busyLock.leaveBusy();
        }
    }

    /**
     * Stops new blocks before the fsync phase starts at a checkpoint.
     *
     * @return Future that will be completed successfully if all {@link #block} are completed before the current method is invoked, either
     *      if there were none, or with an error from the first {@link #unblock}.
     * @see #block
     * @see #unblock
     */
    CompletableFuture<Void> stopBlocking() {
        if (stopGuard.compareAndSet(false, true)) {
            busyLock.block();
        }

        if (pageIds.isEmpty()) {
            stopBlockingFuture.complete(null);
        }

        return stopBlockingFuture;
    }
}
