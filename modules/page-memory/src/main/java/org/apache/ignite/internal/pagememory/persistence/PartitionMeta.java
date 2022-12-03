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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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

    private volatile long lastAppliedTerm;

    private volatile long lastGroupConfigLink;

    private volatile long rowVersionFreeListRootPageId;

    private volatile long indexColumnsFreeListRootPageId;

    private volatile long versionChainTreeRootPageId;

    private volatile long indexTreeMetaPageId;

    private volatile long blobFreeListRootPageId;

    private volatile int pageCount;

    private volatile PartitionMetaSnapshot metaSnapshot;

    /**
     * Default constructor.
     */
    @TestOnly
    public PartitionMeta() {
        metaSnapshot = new PartitionMetaSnapshot(null, this);
    }

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param lastAppliedIndex Last applied index value.
     * @param rowVersionFreeListRootPageId Row version free list root page ID.
     * @param versionChainTreeRootPageId Version chain tree root page ID.
     * @param pageCount Count of pages in the partition.
     */
    public PartitionMeta(
            @Nullable UUID checkpointId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            long lastGroupConfigLink,
            long rowVersionFreeListRootPageId,
            long indexColumnsFreeListRootPageId,
            long versionChainTreeRootPageId,
            long indexTreeMetaPageId,
            int pageCount
    ) {
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.lastGroupConfigLink = lastGroupConfigLink;
        this.rowVersionFreeListRootPageId = rowVersionFreeListRootPageId;
        this.indexColumnsFreeListRootPageId = indexColumnsFreeListRootPageId;
        this.versionChainTreeRootPageId = versionChainTreeRootPageId;
        this.indexTreeMetaPageId = indexTreeMetaPageId;
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
                metaIo.getLastAppliedTerm(pageAddr),
                metaIo.getLastGroupConfigLink(pageAddr),
                metaIo.getRowVersionFreeListRootPageId(pageAddr),
                metaIo.getIndexColumnsFreeListRootPageId(pageAddr),
                metaIo.getVersionChainTreeRootPageId(pageAddr),
                metaIo.getIndexTreeMetaPageId(pageAddr),
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
     * Returns a last applied term value.
     */
    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    /**
     * Sets last applied index and term.
     *
     * @param checkpointId Checkpoint ID.
     * @param lastAppliedIndex Last applied index value.
     * @param lastAppliedTerm Last applied term value.
     */
    public void lastApplied(@Nullable UUID checkpointId, long lastAppliedIndex, long lastAppliedTerm) {
        updateSnapshot(checkpointId);

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    /**
     * Returns link to blob representing last group config.
     */
    public long lastGroupConfigLink() {
        return lastGroupConfigLink;
    }

    /**
     * Sets link to last group config blob.
     *
     * @param checkpointId Checkpoint ID.
     * @param groupConfigLink Link to blob representing a group config.
     */
    public void lastGroupConfigLink(@Nullable UUID checkpointId, long groupConfigLink) {
        updateSnapshot(checkpointId);

        this.lastGroupConfigLink = groupConfigLink;
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
     * Returns index columns free list root page id.
     */
    public long indexColumnsFreeListRootPageId() {
        return indexColumnsFreeListRootPageId;
    }

    /**
     * Sets an index columns free list root page id.
     *
     * @param checkpointId Checkpoint id.
     * @param indexColumnsFreeListRootPageId Index columns free list root page id.
     */
    public void indexColumnsFreeListRootPageId(@Nullable UUID checkpointId, long indexColumnsFreeListRootPageId) {
        updateSnapshot(checkpointId);

        this.indexColumnsFreeListRootPageId = indexColumnsFreeListRootPageId;
    }

    /**
     * Returns index meta tree meta page id.
     */
    public long indexTreeMetaPageId() {
        return indexTreeMetaPageId;
    }

    /**
     * Sets an index meta tree meta page id.
     *
     * @param checkpointId Checkpoint id.
     * @param indexTreeMetaPageId Index meta tree meta page id.
     */
    public void indexTreeMetaPageId(@Nullable UUID checkpointId, long indexTreeMetaPageId) {
        updateSnapshot(checkpointId);

        this.indexTreeMetaPageId = indexTreeMetaPageId;
    }

    /**
     * Returns blob free list root page ID.
     */
    public long blobFreeListRootPageId() {
        return blobFreeListRootPageId;
    }

    /**
     * Sets blob free list root page ID.
     *
     * @param checkpointId Checkpoint ID.
     * @param blobFreeListRootPageId Blob free list root page ID.
     */
    public void blobFreeListRootPageId(@Nullable UUID checkpointId, long blobFreeListRootPageId) {
        updateSnapshot(checkpointId);

        this.blobFreeListRootPageId = blobFreeListRootPageId;
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

        private final long lastAppliedTerm;

        private final long lastGroupConfigLink;

        private final long versionChainTreeRootPageId;

        private final long rowVersionFreeListRootPageId;

        private final long indexColumnsFreeListRootPageId;

        private final long indexTreeMetaPageId;

        private final int pageCount;

        /**
         * Private constructor.
         *
         * @param checkpointId Checkpoint ID.
         * @param partitionMeta Partition meta.
         */
        private PartitionMetaSnapshot(@Nullable UUID checkpointId, PartitionMeta partitionMeta) {
            this.checkpointId = checkpointId;
            lastAppliedIndex = partitionMeta.lastAppliedIndex;
            lastAppliedTerm = partitionMeta.lastAppliedTerm;
            lastGroupConfigLink = partitionMeta.lastGroupConfigLink;
            versionChainTreeRootPageId = partitionMeta.versionChainTreeRootPageId;
            rowVersionFreeListRootPageId = partitionMeta.rowVersionFreeListRootPageId;
            indexColumnsFreeListRootPageId = partitionMeta.indexColumnsFreeListRootPageId;
            indexTreeMetaPageId = partitionMeta.indexTreeMetaPageId;
            pageCount = partitionMeta.pageCount;
        }

        /**
         * Returns a last applied index value.
         */
        public long lastAppliedIndex() {
            return lastAppliedIndex;
        }

        /**
         * Returns a last applied term value.
         */
        public long lastAppliedTerm() {
            return lastAppliedTerm;
        }

        /**
         * Returns link to blob representing last group config.
         */
        public long lastGroupConfigLink() {
            return lastGroupConfigLink;
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
         * Returns index columns free list root page ID.
         */
        public long indexColumnsFreeListRootPageId() {
            return indexColumnsFreeListRootPageId;
        }

        /**
         * Returns index meta tree meta page ID.
         */
        public long indexTreeMetaPageId() {
            return indexTreeMetaPageId;
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
            metaIo.setLastAppliedTerm(pageAddr, lastAppliedTerm);
            metaIo.setLastGroupConfig(pageAddr, lastGroupConfigLink);
            metaIo.setVersionChainTreeRootPageId(pageAddr, versionChainTreeRootPageId);
            metaIo.setIndexColumnsFreeListRootPageId(pageAddr, indexColumnsFreeListRootPageId);
            metaIo.setRowVersionFreeListRootPageId(pageAddr, rowVersionFreeListRootPageId);
            metaIo.setPageCount(pageAddr, pageCount);
            metaIo.setIndexTreeMetaPageId(pageAddr, indexTreeMetaPageId);
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
