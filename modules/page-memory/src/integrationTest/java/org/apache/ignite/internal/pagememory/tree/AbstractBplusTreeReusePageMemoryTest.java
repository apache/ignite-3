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

package org.apache.ignite.internal.pagememory.tree;

import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageLockListener;

/**
 * An abstract class for testing {@link BplusTree} with {@link ReuseList} using different implementations of {@link PageMemory}.
 */
public abstract class AbstractBplusTreeReusePageMemoryTest extends AbstractBplusTreePageMemoryTest {
    /** {@inheritDoc} */
    @Override
    protected ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) throws IgniteInternalCheckedException {
        return new TestReuseList(
                "test",
                grpId,
                partId,
                pageMem,
                new TestPageLockListener(),
                rootId,
                initNew
        );
    }

    /** {@inheritDoc} */
    @Override
    protected void assertNoLocks() {
        super.assertNoLocks();

        assertTrue(TestReuseList.checkNoLocks());
    }

    /**
     * Test extension of {@link FreeListImpl}.
     */
    private static class TestReuseList extends FreeListImpl {
        /**
         * Constructor.
         *
         * @param name Structure name (for debug purpose).
         * @param grpId Group ID.
         * @param partId Partition ID.
         * @param pageMem Page memory.
         * @param lockLsnr Page lock listener.
         * @param metaPageId Metadata page ID.
         * @param initNew {@code True} if new metadata should be initialized.
         * @throws IgniteInternalCheckedException If failed.
         */
        public TestReuseList(
                String name,
                int grpId,
                int partId,
                PageMemory pageMem,
                PageLockListener lockLsnr,
                long metaPageId,
                boolean initNew
        ) throws IgniteInternalCheckedException {
            super(name, grpId, partId, pageMem, lockLsnr, metaPageId, initNew, null, IoStatisticsHolderNoOp.INSTANCE);
        }

        static boolean checkNoLocks() {
            return TestPageLockListener.readLocks.get().isEmpty() && TestPageLockListener.writeLocks.get().isEmpty();
        }
    }

    /**
     * {@link PageLockListener} implementation for the test.
     */
    private static class TestPageLockListener implements PageLockListener {
        private static final ThreadLocal<Set<Long>> readLocks = ThreadLocal.withInitial(HashSet::new);

        private static final ThreadLocal<Set<Long>> writeLocks = ThreadLocal.withInitial(HashSet::new);

        /** {@inheritDoc} */
        @Override
        public void onBeforeReadLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            checkPageId(pageId, pageAddr);

            assertTrue(readLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            if (pageAddr == 0L) {
                return; // Failed to lock.
            }

            checkPageId(pageId, pageAddr);

            assertTrue(writeLocks.get().add(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            assertEquals(effectivePageId(pageId), effectivePageId(getPageId(pageAddr)));

            assertTrue(writeLocks.get().remove(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            // No-op.
        }
    }
}
