package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.FullPageId;

/** Dirty pages to write during next checkpoint. */
public class DirtyPages {
    private final Collection<FullPageId> modifiedPages;
    private final Collection<FullPageId> newPages;
    private final AtomicLong dirtyPagesCntr;

    /** Constructor. */
    public DirtyPages(Collection<FullPageId> modifiedPages, Collection<FullPageId> newPages) {
        this.modifiedPages = modifiedPages;
        this.newPages = newPages;
        this.dirtyPagesCntr = new AtomicLong(modifiedPages.size() + newPages.size());
    }

    /** Get pages that were previously checkpointed. */
    public Collection<FullPageId> modifiedPages() {
        return modifiedPages;
    }

    /** Get newly allocated pages. */
    public Collection<FullPageId> newPages() {
        return newPages;
    }

    /** Get total number of pages to write. */
    public long size() {
        return dirtyPagesCntr.get();
    }

    /** Remove page from checkpoint. */
    public boolean remove(FullPageId pageId) {
        boolean removed = modifiedPages.remove(pageId) || newPages.remove(pageId);
        if (removed) {
            dirtyPagesCntr.decrementAndGet();
        }

        return removed;
    }

    /** Check if the page should be written. */
    public boolean contains(FullPageId pageId) {
        return modifiedPages.contains(pageId) || newPages.contains(pageId);
    }

    /** Add page to write. */
    public boolean add(FullPageId pageId, boolean newPage) {
        boolean added = newPage ? newPages.add(pageId) : modifiedPages.add(pageId);
        if (added) {
            dirtyPagesCntr.incrementAndGet();
        }

        return added;
    }
}
