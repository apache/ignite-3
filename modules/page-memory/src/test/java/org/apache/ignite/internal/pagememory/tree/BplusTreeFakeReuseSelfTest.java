package org.apache.ignite.internal.pagememory.tree;

import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Test with fake reuse list.
 */
public class BplusTreeFakeReuseSelfTest extends BplusTreeSelfTest {
    /** {@inheritDoc} */
    @Override
    protected ReuseList createReuseList(
            int grpId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) throws IgniteInternalCheckedException {
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
