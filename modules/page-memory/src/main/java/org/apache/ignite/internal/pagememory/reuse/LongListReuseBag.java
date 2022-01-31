package org.apache.ignite.internal.pagememory.reuse;

import java.io.Externalizable;
import org.apache.ignite.internal.util.IgniteLongList;

/**
 * {@link IgniteLongList}-based reuse bag.
 */
public final class LongListReuseBag extends IgniteLongList implements ReuseBag {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor for {@link Externalizable}.
     */
    public LongListReuseBag() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addFreePage(long pageId) {
        add(pageId);
    }

    /** {@inheritDoc} */
    @Override public long pollFreePage() {
        return isEmpty() ? 0 : remove();
    }
}

