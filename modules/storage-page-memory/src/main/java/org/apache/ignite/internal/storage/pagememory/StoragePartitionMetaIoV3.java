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

import org.apache.ignite.internal.pagememory.util.PageIdUtils;

/** Storage Io for partition metadata pages (version 3). */
public class StoragePartitionMetaIoV3 extends StoragePartitionMetaIoV2 {
    private static final int WI_HEAD_OFF = ESTIMATED_SIZE_OFF + ESTIMATED_SIZE_BYTES;

    /** Constructor. */
    StoragePartitionMetaIoV3() {
        super(3);
    }

    @Override
    public void initNewPage(long pageAddr, long pageId, int pageSize) {
        initMetaIoV1(pageAddr, pageId, pageSize);

        setWiHead(pageAddr, PageIdUtils.NULL_LINK);
    }

    /**
     * Sets the head link of the write intent list in the partition metadata.
     *
     * @param pageAddr The address of the page to update.
     * @param headLink The link value to set as the head of the write intent list.
     */
    @Override
    public void setWiHead(long pageAddr, long headLink) {
        assertPageType(pageAddr);

        putLong(pageAddr, WI_HEAD_OFF, headLink);
    }

    @Override
    public long getWiHead(long pageAddr) {
        return getLong(pageAddr, WI_HEAD_OFF);
    }

    // WI head link overlapped with first 4 bytes of estimated size field, but we assume that there were no partitions with 4b rows and
    // only LE order was used.
    @Override
    public long getEstimatedSize(long pageAddr) {
        return getLong(pageAddr, ESTIMATED_SIZE_OFF);
    }

}
