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

package org.apache.ignite.internal.pagememory.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;

/**
 * Page store manager.
 */
public interface PageReadWriteManager {
    /**
     * Reads a page for the given group ID.
     *
     * @param grpId Group ID, may be {@code 0} if the page is a meta page.
     * @param pageId PageID to read.
     * @param pageBuf Page buffer to write to.
     * @param keepCrc Keep CRC flag.
     * @throws IgniteInternalCheckedException If failed to read the page.
     */
    void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException;

    /**
     * Writes the page for the given group ID.
     *
     * @param grpId Group ID, may be {@code 0} if the page is a meta page.
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write from.
     * @param calculateCrc If {@code false} crc calculation will be forcibly skipped.
     * @return Page store where the page was written to.
     * @throws IgniteInternalCheckedException If failed to write page.
     */
    PageStore write(int grpId, long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException;

    /**
     * Allocates a page for the given page space.
     *
     * @param grpId Group ID.
     * @param partId Partition ID. Used only if {@code flags} is equal to {@link PageIdAllocator#FLAG_DATA}.
     * @param flags Page allocation flags.
     * @return Allocated page ID.
     * @throws IgniteInternalCheckedException If IO exception occurred while allocating a page ID.
     */
    long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException;
}
