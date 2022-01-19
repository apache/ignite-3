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

import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Supports operations on pages.
 */
public interface PageSupport {
    /**
     * Returns the page absolute pointer associated with the given page ID. Each page obtained with this method must be released by calling
     * {@link #releasePage(int, long, long)}. This method will allocate page with given ID if it doesn't exist.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @return Page pointer.
     * @throws IgniteInternalCheckedException If failed.
     */
    public long acquirePage(int groupId, long pageId) throws IgniteInternalCheckedException;

    /**
     * RReturns the page absolute pointer associated with the given page ID. Each page obtained with this method must be released by calling
     * {@link #releasePage(int, long, long)}. This method will allocate page with given ID if it doesn't exist.
     *
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page pointer.
     * @throws IgniteInternalCheckedException If failed.
     */
    public long acquirePage(int groupId, long pageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Releases page acquired by one of {@code acquirePage} methods.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID to release.
     * @param page    Page pointer returned by the corresponding {@code acquirePage} call.
     */
    public void releasePage(int groupId, long pageId, long page);

    /**
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Pointer for reading the page.
     */
    public long readLock(int groupId, long pageId, long page);

    /**
     * Obtains read lock without checking page tag.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Pointer for reading the page.
     */
    public long readLockForce(int groupId, long pageId, long page);

    /**
     * Releases locked page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     */
    public void readUnlock(int groupId, long pageId, long page);

    /**
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Address of a buffer with contents of the given page or {@code 0L} if attempt to take the write lock failed.
     */
    public long writeLock(int groupId, long pageId, long page);

    /**
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Address of a buffer with contents of the given page or {@code 0L} if attempt to take the write lock failed.
     */
    public long tryWriteLock(int groupId, long pageId, long page);

    /**
     * Releases locked page.
     *
     * @param groupId   Group ID.
     * @param pageId    Page ID.
     * @param page      Page pointer.
     * @param dirtyFlag Determines whether the page was modified since the last checkpoint.
     */
    public void writeUnlock(int groupId, long pageId, long page, boolean dirtyFlag);

    /**
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return {@code True} if the page is dirty.
     */
    public boolean isDirty(int groupId, long pageId, long page);
}
