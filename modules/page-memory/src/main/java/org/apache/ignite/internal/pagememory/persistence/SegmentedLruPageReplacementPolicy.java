package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.INVALID_REL_PTR;
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.OUTDATED_REL_PTR;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.Segment;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Segmented-LRU page replacement policy implementation.
 */
public class SegmentedLruPageReplacementPolicy extends PageReplacementPolicy {
    /** LRU list. */
    private final SegmentedLruPageList lruList;

    /**
     * Constructor.
     *
     * @param seg Page memory segment.
     * @param ptr Pointer to memory region.
     * @param pagesCnt Pages count.
     */
    protected SegmentedLruPageReplacementPolicy(Segment seg, long ptr, int pagesCnt) {
        super(seg);

        lruList = new SegmentedLruPageList(pagesCnt, ptr);
    }

    /** {@inheritDoc} */
    @Override
    public void onHit(long relPtr) {
        int pageIdx = (int) seg.pageIndex(relPtr);

        lruList.moveToTail(pageIdx);
    }

    /** {@inheritDoc} */
    @Override
    public void onMiss(long relPtr) {
        int pageIdx = (int) seg.pageIndex(relPtr);

        lruList.addToTail(pageIdx, false);
    }

    /** {@inheritDoc} */
    @Override
    public void onRemove(long relPtr) {
        int pageIdx = (int) seg.pageIndex(relPtr);

        lruList.remove(pageIdx);
    }

    /** {@inheritDoc} */
    @Override
    public long replace() throws IgniteInternalCheckedException {
        LoadedPagesMap loadedPages = seg.loadedPages();

        for (int i = 0; i < loadedPages.size(); i++) {
            int pageIdx = lruList.poll();

            long relPtr = seg.relative(pageIdx);
            long absPtr = seg.absolute(relPtr);

            FullPageId fullId = fullPageId(absPtr);

            // Check loaded pages map for outdated page.
            relPtr = loadedPages.get(
                    fullId.groupId(),
                    fullId.effectivePageId(),
                    seg.partGeneration(fullId.groupId(), partitionId(fullId.pageId())),
                    INVALID_REL_PTR,
                    OUTDATED_REL_PTR
            );

            assert relPtr != INVALID_REL_PTR;

            if (relPtr == OUTDATED_REL_PTR) {
                return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);
            }

            if (seg.tryToRemovePage(fullId, absPtr)) {
                return relPtr;
            }

            // Return page to the LRU list.
            lruList.addToTail(pageIdx, true);
        }

        throw seg.oomException("no pages to replace");
    }
}
