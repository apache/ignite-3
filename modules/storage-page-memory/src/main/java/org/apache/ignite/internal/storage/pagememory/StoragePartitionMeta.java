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

package org.apache.ignite.internal.storage.pagememory;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaFactory;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage partition meta information.
 */
public class StoragePartitionMeta extends PartitionMeta {

    public static final PartitionMetaFactory FACTORY = new StoragePartitionMetaFactory();

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    private volatile long leaseStartTime;

    private volatile long lastReplicationProtocolGroupConfigFirstPageId;

    private volatile long freeListRootPageId;

    private volatile long versionChainTreeRootPageId;

    private volatile long indexTreeMetaPageId;

    private volatile long gcQueueMetaPageId;

    /**
     * Default constructor.
     */
    @TestOnly
    public StoragePartitionMeta() {
        super(null);
    }

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param lastAppliedIndex Last applied index value.
     * @param freeListRootPageId Free list root page ID.
     * @param versionChainTreeRootPageId Version chain tree root page ID.
     * @param pageCount Count of pages in the partition.
     */
    public StoragePartitionMeta(
            @Nullable UUID checkpointId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            long lastReplicationProtocolGroupConfigFirstPageId,
            long freeListRootPageId,
            long versionChainTreeRootPageId,
            long indexTreeMetaPageId,
            long gcQueueMetaPageId,
            int pageCount,
            long leaseStartTime
    ) {
        super(checkpointId, pageCount);
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
        this.lastReplicationProtocolGroupConfigFirstPageId = lastReplicationProtocolGroupConfigFirstPageId;
        this.freeListRootPageId = freeListRootPageId;
        this.versionChainTreeRootPageId = versionChainTreeRootPageId;
        this.indexTreeMetaPageId = indexTreeMetaPageId;
        this.gcQueueMetaPageId = gcQueueMetaPageId;
        this.leaseStartTime = leaseStartTime;
    }

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     * @param metaIo Partition meta IO.
     * @param pageAddr Address of the page with the partition meta.
     */
    StoragePartitionMeta(@Nullable UUID checkpointId, PartitionMetaIo metaIo, long pageAddr) {
        this(checkpointId, (StoragePartitionMetaIo) metaIo, pageAddr);
    }

    private StoragePartitionMeta(@Nullable UUID checkpointId, StoragePartitionMetaIo metaIo, long pageAddr) {
        this(
                checkpointId,
                metaIo.getLastAppliedIndex(pageAddr),
                metaIo.getLastAppliedTerm(pageAddr),
                metaIo.getLastReplicationProtocolGroupConfigFirstPageId(pageAddr),
                StoragePartitionMetaIo.getFreeListRootPageId(pageAddr),
                metaIo.getVersionChainTreeRootPageId(pageAddr),
                metaIo.getIndexTreeMetaPageId(pageAddr),
                metaIo.getGcQueueMetaPageId(pageAddr),
                metaIo.getPageCount(pageAddr),
                metaIo.getLeaseStartTime(pageAddr)
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
     * Returns ID of the first page in a chain storing a blob representing last replication protocol group config.
     */
    public long lastReplicationProtocolGroupConfigFirstPageId() {
        return lastReplicationProtocolGroupConfigFirstPageId;
    }

    /**
     * Sets ID of the first page in a chain storing a blob representing last replication protocol group config.
     *
     * @param checkpointId Checkpoint ID.
     * @param pageId PageId.
     */
    public void lastReplicationProtocolGroupConfigFirstPageId(@Nullable UUID checkpointId, long pageId) {
        updateSnapshot(checkpointId);

        this.lastReplicationProtocolGroupConfigFirstPageId = pageId;
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
     * Returns free list root page ID.
     */
    public long freeListRootPageId() {
        return freeListRootPageId;
    }

    /**
     * Sets free list root page ID.
     *
     * @param checkpointId Checkpoint ID.
     * @param freeListRootPageId Free list root page ID.
     */
    public void freeListRootPageId(@Nullable UUID checkpointId, long freeListRootPageId) {
        updateSnapshot(checkpointId);

        this.freeListRootPageId = freeListRootPageId;
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
     * Returns garbage collection queue meta page id.
     */
    public long gcQueueMetaPageId() {
        return gcQueueMetaPageId;
    }

    /**
     * Sets a garbage collection queue meta page id.
     *
     * @param checkpointId Checkpoint id.
     * @param gcQueueMetaPageId Garbage collection queue meta page id.
     */
    public void gcQueueMetaPageId(@Nullable UUID checkpointId, long gcQueueMetaPageId) {
        updateSnapshot(checkpointId);

        this.gcQueueMetaPageId = gcQueueMetaPageId;
    }

    @Override
    protected StoragePartitionMetaSnapshot buildSnapshot(@Nullable UUID checkpointId) {
        return new StoragePartitionMetaSnapshot(checkpointId, this);
    }

    @Override
    public StoragePartitionMetaSnapshot metaSnapshot(@Nullable UUID checkpointId) {
        return (StoragePartitionMetaSnapshot) super.metaSnapshot(checkpointId);
    }

    @Override
    public String toString() {
        return S.toString(StoragePartitionMeta.class, this, super.toString());
    }

    /**
     * Updates the current lease start time in the storage.
     *
     * @param checkpointId Checkpoint ID.
     * @param leaseStartTime Lease start time.
     */
    public void updateLease(@Nullable UUID checkpointId, long leaseStartTime) {
        updateSnapshot(checkpointId);

        if (leaseStartTime <= this.leaseStartTime) {
            return;
        }

        this.leaseStartTime = leaseStartTime;
    }

    /**
     * Return the start time of the known lease for this replication group.
     *
     * @return Lease start time.
     */
    public long leaseStartTime() {
        return leaseStartTime;
    }

    /**
     * An immutable snapshot of the partition's meta information.
     */
    public static class StoragePartitionMetaSnapshot implements PartitionMetaSnapshot {
        private final @Nullable UUID checkpointId;

        private final long lastAppliedIndex;

        private final long lastAppliedTerm;

        private final long lastReplicationProtocolGroupConfigFirstPageId;

        private final long versionChainTreeRootPageId;

        private final long freeListRootPageId;

        private final long indexTreeMetaPageId;

        private final long gcQueueMetaPageId;

        private final int pageCount;

        private final long leaseStartTime;

        /**
         * Private constructor.
         *
         * @param checkpointId Checkpoint ID.
         * @param partitionMeta Partition meta.
         */
        private StoragePartitionMetaSnapshot(@Nullable UUID checkpointId, StoragePartitionMeta partitionMeta) {
            this.checkpointId = checkpointId;
            lastAppliedIndex = partitionMeta.lastAppliedIndex;
            lastAppliedTerm = partitionMeta.lastAppliedTerm;
            lastReplicationProtocolGroupConfigFirstPageId = partitionMeta.lastReplicationProtocolGroupConfigFirstPageId;
            versionChainTreeRootPageId = partitionMeta.versionChainTreeRootPageId;
            freeListRootPageId = partitionMeta.freeListRootPageId;
            indexTreeMetaPageId = partitionMeta.indexTreeMetaPageId;
            gcQueueMetaPageId = partitionMeta.gcQueueMetaPageId;
            pageCount = partitionMeta.pageCount();
            leaseStartTime = partitionMeta.leaseStartTime;
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
         * Returns ID of the first page in a chain storing a blob representing last replication protocol group config.
         */
        public long lastReplicationProtocolGroupConfigFirstPageId() {
            return lastReplicationProtocolGroupConfigFirstPageId;
        }

        /**
         * Returns version chain tree root page ID.
         */
        public long versionChainTreeRootPageId() {
            return versionChainTreeRootPageId;
        }

        /**
         * Returns free list root page ID.
         */
        public long freeListRootPageId() {
            return freeListRootPageId;
        }

        /**
         * Returns index meta tree meta page ID.
         */
        public long indexTreeMetaPageId() {
            return indexTreeMetaPageId;
        }

        /**
         * Returns garbage collection queue meta page id.
         */
        public long gcQueueMetaPageId() {
            return gcQueueMetaPageId;
        }

        /**
         * Returns count of pages in the partition.
         */
        public int pageCount() {
            return pageCount;
        }

        /**
         * Returns the lease start time.
         *
         * @return Lease start time.
         */
        public long leaseStartTime() {
            return leaseStartTime;
        }

        /**
         * Writes the contents of the snapshot to a page of type {@link StoragePartitionMetaIo}.
         *
         * @param metaIo Partition meta IO (which should be of type {@link StoragePartitionMetaIo}).
         * @param pageAddr Address of the page with the partition meta.
         */
        @Override
        public void writeTo(PartitionMetaIo metaIo, long pageAddr) {
            StoragePartitionMetaIo storageMetaIo = (StoragePartitionMetaIo) metaIo;
            storageMetaIo.setLastAppliedIndex(pageAddr, lastAppliedIndex);
            storageMetaIo.setLastAppliedTerm(pageAddr, lastAppliedTerm);
            storageMetaIo.setLastReplicationProtocolGroupConfigFirstPageId(pageAddr, lastReplicationProtocolGroupConfigFirstPageId);
            storageMetaIo.setVersionChainTreeRootPageId(pageAddr, versionChainTreeRootPageId);
            storageMetaIo.setFreeListRootPageId(pageAddr, freeListRootPageId);
            storageMetaIo.setIndexTreeMetaPageId(pageAddr, indexTreeMetaPageId);
            storageMetaIo.setGcQueueMetaPageId(pageAddr, gcQueueMetaPageId);
            storageMetaIo.setPageCount(pageAddr, pageCount);
            storageMetaIo.setLeaseStartTime(pageAddr, leaseStartTime);
        }

        /**
         * Returns the checkpoint ID.
         *
         * @return Checkpoint ID.
         */
        @Override
        @Nullable
        public UUID checkpointId() {
            return checkpointId;
        }

        @Override
        public String toString() {
            return S.toString(StoragePartitionMetaSnapshot.class, this);
        }
    }
}
