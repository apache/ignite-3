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

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;

/**
 * Storage Io for partition metadata pages (version 2).
 */
public class StoragePartitionMetaIoV2 extends StoragePartitionMetaIo {
    private static final int WI_HEAD_OFF = ESTIMATED_SIZE_OFF + PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;

    /**
     * Constructor.
     */
    protected StoragePartitionMetaIoV2() {
        super(2);
    }

    /** {@inheritDoc} */
    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setWiHead(pageAddr, PageIdUtils.NULL_LINK);
    }

    /**
     * Sets the head link of the write intent list in the partition metadata.
     *
     * @param pageAddr The address of the page to update.
     * @param headLink The link value to set as the head of the write intent list.
     */
    public void setWiHead(long pageAddr, long headLink) {
        assertPageType(pageAddr);

        putLong(pageAddr, WI_HEAD_OFF, headLink);
    }

    @Override
    public long getWiHead(long pageAddr) {
        return getLong(pageAddr, WI_HEAD_OFF);
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
                .app("primaryReplicaNodeId=").app(getPrimaryReplicaNodeId(addr)).nl()
                .app("primaryReplicaNodeNameFirstPageId=").app(getPrimaryReplicaNodeNameFirstPageId(addr)).nl()
                .app("estimatedSize=").app(getEstimatedSize(addr)).nl()
                .app("wiHead=").app(getWiHead(addr)).nl()
                .app(']');
    }
}
