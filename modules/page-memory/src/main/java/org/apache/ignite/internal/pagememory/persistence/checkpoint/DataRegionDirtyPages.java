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

import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/**
 * Container of dirty pages of the data region.
 *
 * @param <T> Type of dirty pages container.
 */
class DataRegionDirtyPages<T> {
    final PersistentPageMemory pageMemory;

    final T dirtyPages;

    /**
     * Constructor.
     *
     * @param pageMemory Page memory.
     * @param dirtyPages Container of dirty pages.
     */
    DataRegionDirtyPages(PersistentPageMemory pageMemory, T dirtyPages) {
        this.pageMemory = pageMemory;
        this.dirtyPages = dirtyPages;
    }
}
