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

package org.apache.ignite.internal.pagememory.tree.inmemory;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;

/**
 * Test with fake reuse list.
 */
public class ItBplusTreeFakeReuseVolatilePageMemoryTest extends ItBplusTreeVolatilePageMemoryTest {
    /** {@inheritDoc} */
    @Override
    protected ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) {
        return new FakeReuseList();
    }

    /**
     * Fake reuse list.
     */
    private static class FakeReuseList implements ReuseList {
        private final ConcurrentLinkedDeque<Long> deque = new ConcurrentLinkedDeque<>();

        /** {@inheritDoc} */
        @Override
        public void addForRecycle(ReuseBag bag) {
            long pageId;

            while ((pageId = bag.pollFreePage()) != 0L) {
                deque.addFirst(pageId);
            }
        }

        /** {@inheritDoc} */
        @Override
        public long takeRecycledPage() {
            Long pageId = deque.pollFirst();

            return pageId == null ? 0L : pageId;
        }

        /** {@inheritDoc} */
        @Override
        public long recycledPagesCount() {
            return deque.size();
        }

        /** {@inheritDoc} */
        @Override
        public long initRecycledPage(long pageId, byte flag, PageIo initIo) {
            return pageId;
        }
    }
}
