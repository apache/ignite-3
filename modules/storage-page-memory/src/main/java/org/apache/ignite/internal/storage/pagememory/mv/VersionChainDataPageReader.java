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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.datapage.NonFragmentableDataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.jetbrains.annotations.Nullable;

/**
 * {@link DataPageReader} for {@link VersionChain} instances.
 */
class VersionChainDataPageReader extends NonFragmentableDataPageReader<VersionChain> {
    /**
     * Constructs a new instance.
     *
     * @param pageMemory       page memory that will be used to lock and access memory
     * @param groupId          ID of the cache group with which the reader works (all pages must belong to this group)
     * @param statisticsHolder used to track statistics about operations
     */
    public VersionChainDataPageReader(PageMemory pageMemory, int groupId, IoStatisticsHolder statisticsHolder) {
        super(pageMemory, groupId, statisticsHolder);
    }

    @Nullable
    private UUID assembleTxId(long high, long low) {
        if (high == VersionChain.NULL_UUID_COMPONENT && low == VersionChain.NULL_UUID_COMPONENT) {
            return null;
        } else {
            return new UUID(high, low);
        }
    }

    @Override
    protected VersionChain readRowFromAddress(long link, long pageAddr) {
        int offset = 0;

        long high = getLong(pageAddr, offset);
        offset += Long.BYTES;
        long low = getLong(pageAddr, offset);
        offset += Long.BYTES;
        UUID txId = assembleTxId(high, low);

        long headLink = PartitionlessLinks.readFromMemory(pageAddr, offset);
        offset += PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;

        return new VersionChain(partitionOfLink(link), link, txId, headLink);
    }

    private int partitionOfLink(long link) {
        return partitionId(pageId(link));
    }

    @Override
    protected boolean handleNonExistentItemsGracefully() {
        return true;
    }

    @Override
    protected @Nullable VersionChain rowForNonExistingItem(long pageId, long itemId) {
        return null;
    }
}
