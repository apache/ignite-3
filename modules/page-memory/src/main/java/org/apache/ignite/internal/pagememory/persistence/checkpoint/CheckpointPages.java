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

import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;

/**
 * View of pages which should be stored during current checkpoint.
 */
public class CheckpointPages {
    private final Set<FullPageId> segmentPages;

    private final CompletableFuture<?> allowToReplace;

    /**
     * Constructor.
     *
     * @param pages Pages which would be stored to disk in current checkpoint, does not copy the set.
     * @param replaceFuture The sign which allows replacing pages from a checkpoint by page replacer.
     */
    public CheckpointPages(Set<FullPageId> pages, CompletableFuture<?> replaceFuture) {
        segmentPages = pages;
        allowToReplace = replaceFuture;
    }

    /**
     * Returns {@code true} If fullPageId is allowable to store to disk.
     *
     * @param fullPageId Page id for checking.
     * @throws IgniteInternalCheckedException If the waiting sign which allows replacing pages from a checkpoint by page replacer fails.
     */
    public boolean allowToSave(FullPageId fullPageId) throws IgniteInternalCheckedException {
        try {
            // Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
            getUninterruptibly(allowToReplace);
        } catch (ExecutionException e) {
            throw new IgniteInternalCheckedException(e.getCause());
        } catch (CancellationException e) {
            throw new IgniteInternalCheckedException(e);
        }

        return segmentPages.contains(fullPageId);
    }

    /**
     * Returns {@code true} If fullPageId is candidate to stored to disk by current checkpoint.
     *
     * @param fullPageId Page id for checking.
     */
    public boolean contains(FullPageId fullPageId) {
        return segmentPages.contains(fullPageId);
    }

    /**
     * Returns {@code true} if it is marking was successful.
     *
     * @param fullPageId Page id which should be marked as saved to disk.
     */
    public boolean markAsSaved(FullPageId fullPageId) {
        return segmentPages.remove(fullPageId);
    }

    /**
     * Returns size of all pages in current checkpoint.
     */
    public int size() {
        return segmentPages.size();
    }
}
