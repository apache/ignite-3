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

import java.util.Collection;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/**
 * Holder of information about dirty pages by {@link PersistentPageMemory} for checkpoint.
 *
 * <p>Due to page replacements, the total number of pages may change when they are written to disk.
 *
 * <p>Total number of dirty pages can only decrease due to page replacement, but should not increase.
 */
class DataRegionsDirtyPages {
    /** Total number of dirty pages for all data regions at the time of data collection. */
    final int dirtyPageCount;

    /** Collection of dirty pages per {@link PersistentPageMemory} distribution. */
    final Collection<DataRegionDirtyPages<Collection<FullPageId>>> dirtyPages;

    /**
     * Constructor.
     *
     * @param dirtyPages Collection of dirty pages per {@link PersistentPageMemory} distribution.
     */
    public DataRegionsDirtyPages(Collection<DataRegionDirtyPages<Collection<FullPageId>>> dirtyPages) {
        this.dirtyPages = dirtyPages;
        this.dirtyPageCount = dirtyPages.stream().mapToInt(dataRegionPages -> dataRegionPages.dirtyPages.size()).sum();
    }
}
