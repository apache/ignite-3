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

package org.apache.ignite.internal.pagememory.reuse;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.io.PageIO;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Reuse list.
 */
public interface ReuseList {
    /**
     * @param bag Reuse bag.
     * @throws IgniteInternalCheckedException If failed.
     */
    void addForRecycle(ReuseBag bag) throws IgniteInternalCheckedException;

    /**
     * @return Page ID or {@code 0} if none available.
     * @throws IgniteInternalCheckedException If failed.
     */
    long takeRecycledPage() throws IgniteInternalCheckedException;

    /**
     * @return Number of recycled pages it contains.
     * @throws IgniteInternalCheckedException If failed.
     */
    long recycledPagesCount() throws IgniteInternalCheckedException;

    /**
     * Converts recycled page id back to a usable id. Might modify page content as well if flag is changing.
     *
     * @param pageId Id of the recycled page.
     * @param flag   Flag value for the page. One of {@link PageIdAllocator#FLAG_DATA} or {@link PageIdAllocator#FLAG_AUX}.
     * @param initIO Page IO to reinit reused page.
     * @return Updated page id.
     * @throws IgniteInternalCheckedException If failed.
     *
     * @see FullPageId
     */
    long initRecycledPage(long pageId, byte flag, @Nullable PageIO initIO) throws IgniteInternalCheckedException;
}
