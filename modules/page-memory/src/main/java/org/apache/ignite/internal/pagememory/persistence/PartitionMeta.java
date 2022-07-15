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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information.
 */
public class PartitionMeta {
    private static final AtomicIntegerFieldUpdater<PartitionMeta> PAGE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
            PartitionMeta.class,
            "pageCount"
    );

    private static final VarHandle META_SNAPSHOT_VH;

    static {
        try {
            META_SNAPSHOT_VH = MethodHandles.lookup().findVarHandle(PartitionMeta.class, "metaSnapshot", PartitionMetaSnapshot.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile long treeRootPageId;

    private volatile long reuseListRootPageId;

    private volatile int pageCount;

    private volatile PartitionMetaSnapshot metaSnapshot;

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param treeRootPageId Tree root page ID.
     * @param reuseListRootPageId Reuse list root page ID.
     * @param pageCount Count of pages in the partition.
     */
    public PartitionMeta(@Nullable UUID checkpointId, long treeRootPageId, long reuseListRootPageId, int pageCount) {
        this.treeRootPageId = treeRootPageId;
        this.reuseListRootPageId = reuseListRootPageId;
        this.pageCount = pageCount;

        metaSnapshot = new PartitionMetaSnapshot(checkpointId, this);
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
     * @param checkpointId Checkpoint ID.
     * @param treeRootPageId Tree root page ID.
     */
    public void treeRootPageId(@Nullable UUID checkpointId, long treeRootPageId) {
        updateSnapshot(checkpointId);

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
     * @param checkpointId Checkpoint ID.
     * @param reuseListRootPageId Reuse list root page ID.
     */
    public void reuseListRootPageId(@Nullable UUID checkpointId, long reuseListRootPageId) {
        updateSnapshot(checkpointId);

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
    public void incrementPageCount(@Nullable UUID checkpointId) {
        updateSnapshot(checkpointId);

        PAGE_COUNT_UPDATER.incrementAndGet(this);
    }

    /**
     * Returns the latest snapshot of the partition meta.
     *
     * @param checkpointId Checkpoint ID.
     */
    public PartitionMetaSnapshot metaSnapshot(@Nullable UUID checkpointId) {
        updateSnapshot(checkpointId);

        return metaSnapshot;
    }

    private void updateSnapshot(@Nullable UUID checkpointId) {
        PartitionMetaSnapshot current = this.metaSnapshot;

        if (current.checkpointId != checkpointId) {
            META_SNAPSHOT_VH.compareAndSet(this, current, new PartitionMetaSnapshot(checkpointId, this));
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartitionMeta.class, this);
    }

    /**
     * An immutable snapshot of the partition's meta information.
     */
    public static class PartitionMetaSnapshot {
        private final @Nullable UUID checkpointId;

        private final long treeRootPageId;

        private final long reuseListRootPageId;

        private final int pageCount;

        /**
         * Private constructor.
         *
         * @param checkpointId Checkpoint ID.
         * @param partitionMeta Partition meta.
         */
        private PartitionMetaSnapshot(@Nullable UUID checkpointId, PartitionMeta partitionMeta) {
            this.checkpointId = checkpointId;
            this.treeRootPageId = partitionMeta.treeRootPageId;
            this.reuseListRootPageId = partitionMeta.reuseListRootPageId;
            this.pageCount = partitionMeta.pageCount;
        }

        /**
         * Returns tree root page ID.
         */
        public long treeRootPageId() {
            return treeRootPageId;
        }

        /**
         * Returns reuse list root page ID.
         */
        public long reuseListRootPageId() {
            return reuseListRootPageId;
        }

        /**
         * Returns count of pages in the partition.
         */
        public int pageCount() {
            return pageCount;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(PartitionMetaSnapshot.class, this);
        }
    }
}
