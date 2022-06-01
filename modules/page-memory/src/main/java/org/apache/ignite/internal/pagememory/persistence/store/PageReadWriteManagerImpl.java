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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * {@link org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager} implementation.
 */
class PageReadWriteManagerImpl implements org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager {
    @IgniteToStringExclude
    protected final FilePageStoreManager filePageStoreManager;

    /**
     * Constructor.
     *
     * @param filePageStoreManager File page store manager.
     */
    public PageReadWriteManagerImpl(
            FilePageStoreManager filePageStoreManager
    ) {
        this.filePageStoreManager = filePageStoreManager;
    }

    /** {@inheritDoc} */
    @Override
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        FilePageStore pageStore = filePageStoreManager.getStore(grpId, partitionId(pageId));

        try {
            pageStore.read(pageId, pageBuf, keepCrc);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    public PageStore write(
            int grpId,
            long pageId,
            ByteBuffer pageBuf,
            int tag,
            boolean calculateCrc
    ) throws IgniteInternalCheckedException {
        FilePageStore pageStore = filePageStoreManager.getStore(grpId, partitionId(pageId));

        try {
            pageStore.write(pageId, pageBuf, tag, calculateCrc);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }

        return pageStore;
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId >= 0 && (partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION) : partId;

        FilePageStore pageStore = filePageStoreManager.getStore(grpId, partId);

        try {
            long pageIdx = pageStore.allocatePage();

            return pageId(partId, flags, (int) pageIdx);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PageReadWriteManagerImpl.class, this);
    }
}
