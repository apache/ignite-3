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

package org.apache.ignite.internal.pagememory.persistence.store;

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Persistent store of pages.
 */
public interface PageStore extends Closeable {
    /** Type for affinity partitions. */
    byte TYPE_DATA = 1;

    /** Type for index partition. */
    byte TYPE_IDX = 2;

    /**
     * Adds page write listener.
     *
     * @param listener Page write listener.
     */
    void addWriteListener(PageWriteListener listener);

    /**
     * Removes page write listener.
     *
     * @param listener Page write listener.
     */
    void removeWriteListener(PageWriteListener listener);

    /**
     * Returns {@code true} if the page store exists.
     */
    boolean exists();

    /**
     * Allocates next page index.
     *
     * @return Next page index.
     * @throws IgniteInternalCheckedException If failed to allocate.
     */
    long allocatePage() throws IgniteInternalCheckedException;

    /**
     * Returns number of allocated pages.
     */
    long pages();

    /**
     * Reads a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc By default, reading zeroes CRC which was on page store, but you can keep it in {@code pageBuf} if set {@code
     * keepCrc}.
     * @return {@code True} if page has been read successfully, {@code false} if page hasn't been written yet.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException;

    /**
     * Reads a page store header.
     *
     * @param buf Buffer to write to.
     * @throws IgniteInternalCheckedException If failed.
     */
    void readHeader(ByteBuffer buf) throws IgniteInternalCheckedException;

    /**
     * Writes a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write from.
     * @param tag Partition page store version, 1-based incrementing counter. For outdated pages {@code tag} has lower value,
     *      and write does nothing.
     * @param calculateCrc If {@code false} crc calculation will be forcibly skipped.
     * @throws IgniteInternalCheckedException If page writing failed (IO error occurred).
     */
    void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteInternalCheckedException;

    /**
     * Sync method used to ensure that the given pages are guaranteed to be written to the page store.
     *
     * @throws IgniteInternalCheckedException If sync failed (IO error occurred).
     */
    void sync() throws IgniteInternalCheckedException;

    /**
     * Initializes the page store if it hasn't already.
     *
     * @throws IgniteInternalCheckedException If initialization failed (IO error occurred).
     */
    void ensure() throws IgniteInternalCheckedException;

    /**
     * Returns page store version.
     */
    int version();

    /**
     * Stops the page store.
     *
     * @param clean {@code True} to clean page store.
     * @throws IgniteInternalCheckedException If failed.
     */
    void stop(boolean clean) throws IgniteInternalCheckedException;

    /**
     * Truncates and deletes page store.
     *
     * @param tag New partition tag.
     * @throws IgniteInternalCheckedException If failed.
     */
    void truncate(int tag) throws IgniteInternalCheckedException;

    /**
     * Returns page size in bytes.
     */
    int pageSize();

    /**
     * Returns size of the page store in bytes.
     *
     * <p>May differ from {@link #pages} * {@link #pageSize} due to delayed writes or due to other implementation specific details.
     */
    long size();
}
