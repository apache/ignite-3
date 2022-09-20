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

/**
 * Data class of checkpoint information.
 */
class Checkpoint {
    /** Sorted dirty pages from data regions that should be checkpointed. */
    final CheckpointDirtyPages dirtyPages;

    /** Checkpoint progress status. */
    final CheckpointProgressImpl progress;

    /** Number of dirty pages. */
    final int dirtyPagesSize;

    /**
     * Constructor.
     *
     * @param dirtyPages Sorted dirty pages from data regions that should be checkpointed.
     * @param progress Checkpoint progress status.
     */
    Checkpoint(
            CheckpointDirtyPages dirtyPages,
            CheckpointProgressImpl progress
    ) {
        this.dirtyPages = dirtyPages;
        this.progress = progress;

        dirtyPagesSize = dirtyPages.dirtyPagesCount();
    }

    /**
     * Returns {@code true} if this checkpoint contains at least one dirty page.
     */
    public boolean hasDelta() {
        return dirtyPagesSize != 0;
    }
}
