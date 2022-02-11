package org.apache.ignite.internal.pagememory.tree.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.copyMemory;

/**
 * Abstract IO routines for B+Tree leaf pages.
 */
public abstract class BplusLeafIo<L> extends BplusIo<L> {
    /**
     * Constructor.
     *
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    protected BplusLeafIo(int type, int ver, int itemSize) {
        super(type, ver, true, true, itemSize);
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxCount(long pageAddr, int pageSize) {
        return (pageSize - ITEMS_OFF) / getItemSize();
    }

    /** {@inheritDoc} */
    @Override
    public final void copyItems(
            long srcPageAddr,
            long dstPageAddr,
            int srcIdx,
            int dstIdx,
            int cnt,
            boolean cpLeft
    ) {
        assert srcIdx != dstIdx || srcPageAddr != dstPageAddr;
        assertPageType(dstPageAddr);

        copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt * getItemSize());
    }

    /** {@inheritDoc} */
    @Override
    public final int offset(int idx) {
        assert idx >= 0 : idx;

        return ITEMS_OFF + idx * getItemSize();
    }
}
