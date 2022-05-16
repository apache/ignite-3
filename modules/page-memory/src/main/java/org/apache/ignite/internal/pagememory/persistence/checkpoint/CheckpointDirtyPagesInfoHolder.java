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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.util.Collection;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Holder of information about dirty pages by {@link PageMemoryImpl} for checkpoint.
 */
class CheckpointDirtyPagesInfoHolder {
    /** Total number of dirty pages. */
    final int dirtyPageCount;

    /** Collection of dirty pages per {@link PageMemoryImpl} distribution. */
    final Collection<IgniteBiTuple<PageMemoryImpl, Collection<FullPageId>>> dirtyPages;

    /**
     * Constructor.
     *
     * @param dirtyPages Collection of dirty pages per {@link PageMemoryImpl} distribution.
     * @param dirtyPageCount Total number of dirty pages.
     */
    public CheckpointDirtyPagesInfoHolder(
            Collection<IgniteBiTuple<PageMemoryImpl, Collection<FullPageId>>> dirtyPages,
            int dirtyPageCount
    ) {
        this.dirtyPages = dirtyPages;
        this.dirtyPageCount = dirtyPageCount;
    }
}
