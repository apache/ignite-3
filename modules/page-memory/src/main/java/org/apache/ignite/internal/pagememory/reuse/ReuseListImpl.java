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

package org.apache.ignite.internal.pagememory.reuse;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.PagesList;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.jetbrains.annotations.Nullable;

/**
 * Reuse list.
 */
public class ReuseListImpl extends PagesList implements ReuseList {
    private static final AtomicReferenceFieldUpdater<ReuseListImpl, Stripe[]> bucketUpdater =
            AtomicReferenceFieldUpdater.newUpdater(ReuseListImpl.class, Stripe[].class, "bucket");

    private volatile Stripe[] bucket;

    /** Onheap pages cache. */
    private final PagesCache bucketCache;

    /**
     * Constructor.
     *
     * @param name Structure name (for debug purpose).
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param log Logger.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteInternalCheckedException If failed.
     */
    public ReuseListImpl(
            String name,
            int grpId,
            int partId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            IgniteLogger log,
            long metaPageId,
            boolean initNew,
            @Nullable AtomicLong pageListCacheLimit
    ) throws IgniteInternalCheckedException {
        super(
                name,
                grpId,
                partId,
                pageMem,
                lockLsnr,
                log,
                1,
                0,
                null
        );

        bucketCache = new PagesCache(pageListCacheLimit);

        reuseList = this;

        init(metaPageId, initNew);
    }

    /** {@inheritDoc} */
    @Override
    protected boolean isReuseBucket(int bucket) {
        assert bucket == 0 : bucket;

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void addForRecycle(ReuseBag bag) throws IgniteInternalCheckedException {
        put(bag, 0, 0, 0, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public long takeRecycledPage() throws IgniteInternalCheckedException {
        return takeEmptyPage(0, null, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public long initRecycledPage(long pageId, byte flag, PageIo initIo) throws IgniteInternalCheckedException {
        return initRecycledPage0(pageId, flag, initIo);
    }

    /** {@inheritDoc} */
    @Override
    public long recycledPagesCount() throws IgniteInternalCheckedException {
        return storedPagesCount(0);
    }

    /** {@inheritDoc} */
    @Override
    protected Stripe[] getBucket(int bucket) {
        return this.bucket;
    }

    /** {@inheritDoc} */
    @Override
    protected int getBucketIndex(int freeSpace) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        return bucketUpdater.compareAndSet(this, exp, upd);
    }

    /** {@inheritDoc} */
    @Override
    protected PagesCache getBucketCache(int bucket, boolean create) {
        return bucketCache;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "ReuseList [name=" + name() + ']';
    }
}
