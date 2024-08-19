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

import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;

/**
 * Storage Io for partition metadata pages.
 */
public class StoragePartitionMetaIo extends PartitionMetaIo {
    /** Page IO type. */
    public static final short T_TABLE_PARTITION_META_IO = 7;

    private static final int LAST_APPLIED_INDEX_OFF = PARTITION_META_HEADER_END;

    private static final int LAST_APPLIED_TERM_OFF = LAST_APPLIED_INDEX_OFF + Long.BYTES;

    private static final int LAST_REPLICATION_PROTOCOL_GROUP_CONFIG_FIRST_PAGE_ID_OFF = LAST_APPLIED_TERM_OFF + Long.BYTES;

    private static final int FREE_LIST_ROOT_PAGE_ID_OFF = LAST_REPLICATION_PROTOCOL_GROUP_CONFIG_FIRST_PAGE_ID_OFF + Long.BYTES;

    private static final int VERSION_CHAIN_TREE_ROOT_PAGE_ID_OFF = FREE_LIST_ROOT_PAGE_ID_OFF + Long.BYTES;

    private static final int INDEX_TREE_META_PAGE_ID_OFF = VERSION_CHAIN_TREE_ROOT_PAGE_ID_OFF + Long.BYTES;

    private static final int GC_QUEUE_META_PAGE_ID_OFF = INDEX_TREE_META_PAGE_ID_OFF + Long.BYTES;

    private static final int LEASE_START_TIME_OFF = GC_QUEUE_META_PAGE_ID_OFF + Long.BYTES;

    private static final int ESTIMATED_SIZE_OFF = LEASE_START_TIME_OFF + Long.BYTES;

    private static final int PRIMARY_REPLICA_NODE_ID_FIRST_PAGE_ID_OFF = ESTIMATED_SIZE_OFF + Long.BYTES;

    private static final int PRIMARY_REPLICA_NODE_NAME_FIRST_PAGE_ID_OFF = PRIMARY_REPLICA_NODE_ID_FIRST_PAGE_ID_OFF + Long.BYTES;


    /** I/O versions. */
    public static final IoVersions<StoragePartitionMetaIo> VERSIONS = new IoVersions<>(new StoragePartitionMetaIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected StoragePartitionMetaIo(int ver) {
        super(T_TABLE_PARTITION_META_IO, ver);
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setLastAppliedIndex(pageAddr, 0);
        setLastAppliedTerm(pageAddr, 0);
        setLastReplicationProtocolGroupConfigFirstPageId(pageAddr, 0);
        setFreeListRootPageId(pageAddr, 0);
        setVersionChainTreeRootPageId(pageAddr, 0);
        setIndexTreeMetaPageId(pageAddr, 0);
        setGcQueueMetaPageId(pageAddr, 0);
        setPageCount(pageAddr, 0);
        setLeaseStartTime(pageAddr, HybridTimestamp.MIN_VALUE.longValue());
        setPrimaryReplicaNodeIdFirstPageId(pageAddr, 0);
        setPrimaryReplicaNodeNameFirstPageId(pageAddr, 0);
        setEstimatedSize(pageAddr, 0);
    }

    /**
     * Sets a last applied index value.
     *
     * @param pageAddr Page address.
     * @param lastAppliedIndex Last applied index value.
     */
    public void setLastAppliedIndex(long pageAddr, long lastAppliedIndex) {
        assertPageType(pageAddr);

        putLong(pageAddr, LAST_APPLIED_INDEX_OFF, lastAppliedIndex);
    }

    /**
     * Sets a last applied term value.
     *
     * @param pageAddr Page address.
     * @param lastAppliedTerm Last applied term value.
     */
    public void setLastAppliedTerm(long pageAddr, long lastAppliedTerm) {
        assertPageType(pageAddr);

        putLong(pageAddr, LAST_APPLIED_TERM_OFF, lastAppliedTerm);
    }

    /**
     * Sets ID of the first page in a chain storing a blob representing last replication protocol group config.
     *
     * @param pageAddr Page address.
     * @param pageId Page ID.
     */
    public void setLastReplicationProtocolGroupConfigFirstPageId(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, LAST_REPLICATION_PROTOCOL_GROUP_CONFIG_FIRST_PAGE_ID_OFF, pageId);
    }

    /**
     * Returns a last applied index value.
     *
     * @param pageAddr Page address.
     */
    public long getLastAppliedIndex(long pageAddr) {
        return getLong(pageAddr, LAST_APPLIED_INDEX_OFF);
    }

    /**
     * Returns a last applied term value.
     *
     * @param pageAddr Page address.
     */
    public long getLastAppliedTerm(long pageAddr) {
        return getLong(pageAddr, LAST_APPLIED_TERM_OFF);
    }

    /**
     * Returns ID of the first page in a chain storing a blob representing last replication protocol group config.
     *
     * @param pageAddr Page address.
     */
    public long getLastReplicationProtocolGroupConfigFirstPageId(long pageAddr) {
        return getLong(pageAddr, LAST_REPLICATION_PROTOCOL_GROUP_CONFIG_FIRST_PAGE_ID_OFF);
    }

    /**
     * Sets free list root page ID.
     *
     * @param pageAddr Page address.
     * @param pageId Free list root page ID.
     */
    public void setFreeListRootPageId(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, FREE_LIST_ROOT_PAGE_ID_OFF, pageId);
    }

    /**
     * Returns free list root page ID.
     *
     * @param pageAddr Page address.
     */
    public static long getFreeListRootPageId(long pageAddr) {
        return getLong(pageAddr, FREE_LIST_ROOT_PAGE_ID_OFF);
    }

    /**
     * Sets version chain tree root page ID.
     *
     * @param pageAddr Page address.
     * @param pageId Version chain tree root page ID.
     */
    public void setVersionChainTreeRootPageId(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, VERSION_CHAIN_TREE_ROOT_PAGE_ID_OFF, pageId);
    }

    /**
     * Returns version chain tree root page ID.
     *
     * @param pageAddr Page address.
     */
    public long getVersionChainTreeRootPageId(long pageAddr) {
        return getLong(pageAddr, VERSION_CHAIN_TREE_ROOT_PAGE_ID_OFF);
    }

    /**
     * Sets an index meta tree meta page id.
     *
     * @param pageAddr Page address.
     * @param pageId Meta page id.
     */
    public void setIndexTreeMetaPageId(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, INDEX_TREE_META_PAGE_ID_OFF, pageId);
    }

    /**
     * Returns an index meta tree meta page id.
     *
     * @param pageAddr Page address.
     */
    public long getIndexTreeMetaPageId(long pageAddr) {
        return getLong(pageAddr, INDEX_TREE_META_PAGE_ID_OFF);
    }

    /**
     * Sets a garbage collection queue meta page id.
     *
     * @param pageAddr Page address.
     * @param pageId Meta page id.
     */
    public void setGcQueueMetaPageId(long pageAddr, long pageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, GC_QUEUE_META_PAGE_ID_OFF, pageId);
    }

    /**
     * Returns an garbage collection queue meta page id.
     *
     * @param pageAddr Page address.
     */
    public long getGcQueueMetaPageId(long pageAddr) {
        return getLong(pageAddr, GC_QUEUE_META_PAGE_ID_OFF);
    }

    /**
     * Sets the lease start time.
     *
     * @param pageAddr Page address.
     * @param leaseStartTime Lease start time.
     */
    public void setLeaseStartTime(long pageAddr, long leaseStartTime) {
        assertPageType(pageAddr);

        putLong(pageAddr, LEASE_START_TIME_OFF, leaseStartTime);
    }

    /**
     * Returns the lease start time.
     *
     * @param pageAddr Page address.
     * @return Lease start time.
     */
    public long getLeaseStartTime(long pageAddr) {
        return getLong(pageAddr, LEASE_START_TIME_OFF);
    }

    /**
     * Sets the primary replica node id first page id.
     *
     * @param pageAddr Page address.
     * @param primaryReplicaNodeIdFirstPageId Primary replica node id first page id.
     */
    public void setPrimaryReplicaNodeIdFirstPageId(long pageAddr, long primaryReplicaNodeIdFirstPageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, PRIMARY_REPLICA_NODE_ID_FIRST_PAGE_ID_OFF, primaryReplicaNodeIdFirstPageId);
    }

    /**
     * Returns the primary replica node id first page id.
     *
     * @param pageAddr Page address.
     * @return Primary replica node id first page id.
     */
    public long getPrimaryReplicaNodeIdFirstPageId(long pageAddr) {
        return getLong(pageAddr, PRIMARY_REPLICA_NODE_ID_FIRST_PAGE_ID_OFF);
    }


    /**
     * Sets the primary replica node name first page id.
     *
     * @param pageAddr Page address.
     * @param primaryReplicaNodeNameFirstPageId Primary replica node name first page id.
     */
    public void setPrimaryReplicaNodeNameFirstPageId(long pageAddr, long primaryReplicaNodeNameFirstPageId) {
        assertPageType(pageAddr);

        putLong(pageAddr, PRIMARY_REPLICA_NODE_NAME_FIRST_PAGE_ID_OFF, primaryReplicaNodeNameFirstPageId);
    }

    /**
     * Returns the primary replica node name first page id.
     *
     * @param pageAddr Page address.
     * @return Primary replica node name first page id.
     */
    public long getPrimaryReplicaNodeNameFirstPageId(long pageAddr) {
        return getLong(pageAddr, PRIMARY_REPLICA_NODE_NAME_FIRST_PAGE_ID_OFF);
    }

    /**
     * Sets the estimated size of this partition.
     *
     * @param pageAddr Page address.
     * @param estimatedSize Estimated size.
     */
    public void setEstimatedSize(long pageAddr, long estimatedSize) {
        assertPageType(pageAddr);

        putLong(pageAddr, ESTIMATED_SIZE_OFF, estimatedSize);
    }

    /**
     * Returns the estimated size of this partition.
     *
     * @param pageAddr Page address.
     */
    public long getEstimatedSize(long pageAddr) {
        return getLong(pageAddr, ESTIMATED_SIZE_OFF);
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TablePartitionMeta [").nl()
                .app("lastAppliedIndex=").app(getLastAppliedIndex(addr)).nl()
                .app("lastAppliedTerm=").app(getLastAppliedTerm(addr)).nl()
                .app("lastReplicationProtocolGroupConfigFirstPageId=").app(getLastReplicationProtocolGroupConfigFirstPageId(addr)).nl()
                .app("freeListRootPageId=").appendHex(getFreeListRootPageId(addr)).nl()
                .app("versionChainTreeRootPageId=").appendHex(getVersionChainTreeRootPageId(addr)).nl()
                .app("indexTreeMetaPageId=").appendHex(getIndexTreeMetaPageId(addr)).nl()
                .app("gcQueueMetaPageId=").appendHex(getGcQueueMetaPageId(addr)).nl()
                .app("pageCount=").app(getPageCount(addr)).nl()
                .app("leaseStartTime=").app(getLeaseStartTime(addr)).nl()
                .app("primaryReplicaNodeIdFirstPageId=").app(getPrimaryReplicaNodeIdFirstPageId(addr)).nl()
                .app("primaryReplicaNodeNameFirstPageId=").app(getPrimaryReplicaNodeNameFirstPageId(addr)).nl()
                .app("estimatedSize=").app(getEstimatedSize(addr)).nl()
                .app(']');
    }
}
