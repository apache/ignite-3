package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.util.Collection;
import org.apache.ignite.internal.pagememory.FullPageId;

public class PagesToWrite {
    private final Collection<FullPageId> dirtyPages;
    private final Collection<FullPageId> newPages;

    public PagesToWrite(Collection<FullPageId> dirtyPages, Collection<FullPageId> newPages) {
        this.dirtyPages = dirtyPages;
        this.newPages = newPages;
    }

    public Collection<FullPageId> dirtyPages() {
        return dirtyPages;
    }

    public Collection<FullPageId> newPages() {
        return newPages;
    }

    public int size() {
        return dirtyPages.size() + newPages.size();
    }

    public boolean remove(FullPageId pageId) {
        return dirtyPages.remove(pageId) || newPages.remove(pageId);
    }

    public boolean contains(FullPageId pageId) {
        return dirtyPages.contains(pageId) || newPages.contains(pageId);
    }
}
