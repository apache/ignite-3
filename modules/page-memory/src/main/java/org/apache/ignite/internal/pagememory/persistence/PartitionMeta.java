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

package org.apache.ignite.internal.pagememory.persistence;

import org.apache.ignite.internal.tostring.S;

/**
 * Partition meta information.
 */
public class PartitionMeta {
    private volatile long treeRootPageId;

    private volatile long reuseListRootPageId;

    private volatile int pageCount;

    private final boolean created;

    /**
     * Constructor.
     *
     * @param treeRootPageId Tree root page ID.
     * @param reuseListRootPageId Reuse list root page ID.
     * @param pageCount Count of pages in the partition.
     * @param created {@code True} if the partition meta was created, {@code false} if it was read from a partition file.
     */
    public PartitionMeta(long treeRootPageId, long reuseListRootPageId, int pageCount, boolean created) {
        this.treeRootPageId = treeRootPageId;
        this.reuseListRootPageId = reuseListRootPageId;
        this.pageCount = pageCount;
        this.created = created;
    }

    /**
     * Returns tree root page ID.
     */
    public long treeRootPageId() {
        return treeRootPageId;
    }

    /**
     * Sets tree root page ID.
     *
     * @param treeRootPageId Tree root page ID.
     */
    public void treeRootPageId(long treeRootPageId) {
        this.treeRootPageId = treeRootPageId;
    }

    /**
     * Returns reuse list root page ID.
     */
    public long reuseListRootPageId() {
        return reuseListRootPageId;
    }

    /**
     * Sets reuse list root page ID.
     *
     * @param reuseListRootPageId Reuse list root page ID.
     */
    public void reuseListRootPageId(long reuseListRootPageId) {
        this.reuseListRootPageId = reuseListRootPageId;
    }

    /**
     * Returns count of pages in the partition.
     */
    public int pageCount() {
        return pageCount;
    }

    /**
     * Sets count of pages in the partition.
     *
     * @param pageCount Count of pages in the partition.
     */
    public void pageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    /**
     * Returns {@code true} if the partition meta was created, {@code false} if it was read from a partition file.
     */
    public boolean isCreated() {
        return created;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionMeta.class, this);
    }
}
