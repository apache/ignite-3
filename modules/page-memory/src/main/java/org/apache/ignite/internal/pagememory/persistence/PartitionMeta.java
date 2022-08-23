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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information.
 */
public class PartitionMeta {
    private static final VarHandle PAGE_COUNT;

    private static final VarHandle META_SNAPSHOT;

    static {
        try {
            PAGE_COUNT = MethodHandles.lookup().findVarHandle(PartitionMeta.class, "pageCount", int.class);

            META_SNAPSHOT = MethodHandles.lookup().findVarHandle(PartitionMeta.class, "metaSnapshot", PartitionMetaSnapshot.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile long lastAppliedIndex;

    private volatile long versionChainTreeRootPageId;

    private volatile long rowVersionFreeListRootPageId;

    private volatile int pageCount;

    private volatile PartitionMetaSnapshot metaSnapshot;

    /**
     * Default constructor.
     */
    public PartitionMeta() {
        metaSnapshot = new PartitionMetaSnapshot(null, this);
    }

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param lastAppliedIndex Last applied index value.
     * @param versionChainTreeRootPageId Version chain tree root page ID.
     * @param rowVersionFreeListRootPageId Row version free list root page ID.
     * @param pageCount Count of pages in the partition.
     */
    public PartitionMeta(
            @Nullable UUID checkpointId,
            long lastAppliedIndex,
            long versionChainTreeRootPageId,
            long rowVersionFreeListRootPageId,
            int pageCount
    ) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.versionChainTreeRootPageId = versionChainTreeRootPageId;
        this.rowVersionFreeListRootPageId = rowVersionFreeListRootPageId;
        this.pageCount = pageCount;

        metaSnapshot = new PartitionMetaSnapshot(checkpointId, this);
    }

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param metaIo Partition meta IO.
     * @param pageAddr Address of the page with the partition meta.
     */
    PartitionMeta(@Nullable UUID checkpointId, PartitionMetaIo metaIo, long pageAddr) {
        this(
                checkpointId,
                metaIo.getLastAppliedIndex(pageAddr),
                metaIo.getVersionChainTreeRootPageId(pageAddr),
                metaIo.getRowVersionFreeListRootPageId(pageAddr),
                metaIo.getPageCount(pageAddr)
        );
    }

    /**
     * Returns a last applied index value.
     */
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /**
     * Sets a last applied index value.
     *
     * @param checkpointId Checkpoint ID.
     * @param lastAppliedIndex Last applied index value.
     */
    public void lastAppliedIndex(@Nullable UUID checkpointId, long lastAppliedIndex) {
        updateSnapshot(checkpointId);

        this.lastAppliedIndex = lastAppliedIndex;
    }

    /**
     * Returns version chain tree root page ID.
     */
    public long versionChainTreeRootPageId() {
        return versionChainTreeRootPageId;
    }

    /**
     * Sets version chain root page ID.
     *
     * @param checkpointId Checkpoint ID.
     * @param versionChainTreeRootPageId Version chain root page ID.
     */
    public void versionChainTreeRootPageId(@Nullable UUID checkpointId, long versionChainTreeRootPageId) {
        updateSnapshot(checkpointId);

        this.versionChainTreeRootPageId = versionChainTreeRootPageId;
    }

    /**
     * Returns row version free list root page ID.
     */
    public long rowVersionFreeListRootPageId() {
        return rowVersionFreeListRootPageId;
    }

    /**
     * Sets row version free list root page ID.
     *
     * @param checkpointId Checkpoint ID.
     * @param rowVersionFreeListRootPageId Row version free list root page ID.
     */
    public void rowVersionFreeListRootPageId(@Nullable UUID checkpointId, long rowVersionFreeListRootPageId) {
        updateSnapshot(checkpointId);

        this.rowVersionFreeListRootPageId = rowVersionFreeListRootPageId;
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

        PAGE_COUNT.getAndAdd(this, 1);
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

    /**
     * Takes a snapshot of the partition meta if the {@code checkpointId} is different from the {@link #metaSnapshot last snapshot} {@link
     * PartitionMetaSnapshot#checkpointId}.
     *
     * @param checkpointId Checkpoint ID.
     */
    private void updateSnapshot(@Nullable UUID checkpointId) {
        PartitionMetaSnapshot current = metaSnapshot;

        if (current.checkpointId != checkpointId) {
            META_SNAPSHOT.compareAndSet(this, current, new PartitionMetaSnapshot(checkpointId, this));
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

        private final long lastAppliedIndex;

        private final long versionChainTreeRootPageId;

        private final long rowVersionFreeListRootPageId;

        private final int pageCount;

        /**
         * Private constructor.
         *
         * @param checkpointId Checkpoint ID.
         * @param partitionMeta Partition meta.
         */
        private PartitionMetaSnapshot(@Nullable UUID checkpointId, PartitionMeta partitionMeta) {
            this.checkpointId = checkpointId;
            this.lastAppliedIndex = partitionMeta.lastAppliedIndex;
            this.versionChainTreeRootPageId = partitionMeta.versionChainTreeRootPageId;
            this.rowVersionFreeListRootPageId = partitionMeta.rowVersionFreeListRootPageId;
            this.pageCount = partitionMeta.pageCount;
        }

        /**
         * Returns a last applied index value.
         */
        public long lastAppliedIndex() {
            return lastAppliedIndex;
        }

        /**
         * Returns version chain tree root page ID.
         */
        public long versionChainTreeRootPageId() {
            return versionChainTreeRootPageId;
        }

        /**
         * Returns row version free list root page ID.
         */
        public long rowVersionFreeListRootPageId() {
            return rowVersionFreeListRootPageId;
        }

        /**
         * Returns count of pages in the partition.
         */
        public int pageCount() {
            return pageCount;
        }

        /**
         * Writes the contents of the snapshot to a page of type {@link PartitionMetaIo}.
         *
         * @param metaIo Partition meta IO.
         * @param pageAddr Address of the page with the partition meta.
         */
        void writeTo(PartitionMetaIo metaIo, long pageAddr) {
            metaIo.setLastAppliedIndex(pageAddr, lastAppliedIndex);
            metaIo.setVersionChainTreeRootPageId(pageAddr, versionChainTreeRootPageId);
            metaIo.setRowVersionFreeListRootPageId(pageAddr, rowVersionFreeListRootPageId);
            metaIo.setPageCount(pageAddr, pageCount);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(PartitionMetaSnapshot.class, this);
        }
    }

    /**
     * Gets partition metadata page ID for specified partId.
     *
     * @param partId Partition ID.
     * @return Meta page for partId.
     */
    public static long partitionMetaPageId(int partId) {
        return pageId(partId, FLAG_AUX, 0);
    }
}
