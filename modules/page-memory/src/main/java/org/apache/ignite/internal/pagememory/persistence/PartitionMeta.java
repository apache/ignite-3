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

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information.
 */
public class PartitionMeta {
    private static final AtomicIntegerFieldUpdater<PartitionMeta> PAGE_COUNT_UPDATER = newUpdater(PartitionMeta.class, "pageCount");

    private volatile long treeRootPageId;

    private volatile long reuseListRootPageId;

    private volatile int pageCount;

    private volatile @Nullable PartitionMeta metaSnapshot;

    /**
     * Constructor.
     *
     * @param treeRootPageId Tree root page ID.
     * @param reuseListRootPageId Reuse list root page ID.
     * @param pageCount Count of pages in the partition.
     */
    public PartitionMeta(long treeRootPageId, long reuseListRootPageId, int pageCount) {
        this.treeRootPageId = treeRootPageId;
        this.reuseListRootPageId = reuseListRootPageId;
        this.pageCount = pageCount;
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
     * Increases the number of pages in a partition.
     */
    public void incrementPageCount() {
        PAGE_COUNT_UPDATER.incrementAndGet(this);
    }

    /**
     * TODO: IGNITE-17295 не забудь избавиться или типо того.
     */
    public void makeMetaSnapshot() {
        metaSnapshot = copy();
    }

    /**
     * Returns the latest snapshot of the meta.
     */
    public @Nullable PartitionMeta metaSnapshot() {
        return metaSnapshot;
    }

    private PartitionMeta copy() {
        return new PartitionMeta(treeRootPageId, reuseListRootPageId, pageCount);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionMeta.class, this);
    }
}
