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

package org.apache.ignite.internal.pagememory;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.lang.IgniteInternalException;

/** */
public interface PageMemory extends PageIdAllocator, PageSupport {
    /**
     * Start page memory.
     */
    public void start() throws IgniteInternalException;

    /**
     * Stop page memory.
     *
     * @param deallocate {@code True} to deallocate memory, {@code false} to allow memory reuse on subsequent {@link #start()}
     * @throws IgniteException
     */
    public void stop(boolean deallocate) throws IgniteInternalException;

    /**
     * @return Page size in bytes.
     */
    public int pageSize();

    /**
     * @param groupId Group id.
     * @return Page size without encryption overhead.
     */
    public int realPageSize(int groupId);

    /**
     * @return Page size with system overhead, in bytes.
     */
    public int systemPageSize();

    /**
     * @param pageAddr Page address.
     * @return Page byte buffer.
     */
    public ByteBuffer pageBuffer(long pageAddr);

    /**
     * @return Total number of loaded pages in memory.
     */
    public long loadedPages();

    /**
     * Number of pages used in checkpoint buffer.
     */
    public int checkpointBufferPagesCount();

    /**
     * Registry to retrieve {@link PageIo} instances for pages.
     */
    public PageIoRegistry ioRegistry();
}
