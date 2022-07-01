package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointDirtyPages} testing.
 */
public class CheckpointDirtyPagesTest {
    @Test
    void testEmptyCheckpointDirtyPages() {
        CheckpointDirtyPages empty = CheckpointDirtyPages.EMPTY;

        assertEquals(0, empty.dirtyPagesCount());
        assertNull(empty.nextView(null));
    }

    @Test
    void testDirtyPagesCount() {
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages0 = createDirtyPages();
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages1 = createDirtyPages(of(0, 0, 0));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages2 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages3 = createDirtyPages(of(2, 0, 0), of(2, 0, 1), of(2, 0, 1));

        assertEquals(0, new CheckpointDirtyPages(dirtyPages0).dirtyPagesCount());
        assertEquals(1, new CheckpointDirtyPages(dirtyPages1).dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(dirtyPages2).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(dirtyPages3).dirtyPagesCount());

        assertEquals(1, new CheckpointDirtyPages(dirtyPages0, dirtyPages1).dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(dirtyPages0, dirtyPages2).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(dirtyPages0, dirtyPages3).dirtyPagesCount());

        assertEquals(3, new CheckpointDirtyPages(dirtyPages1, dirtyPages2).dirtyPagesCount());
        assertEquals(4, new CheckpointDirtyPages(dirtyPages1, dirtyPages3).dirtyPagesCount());

        assertEquals(5, new CheckpointDirtyPages(dirtyPages2, dirtyPages3).dirtyPagesCount());

        assertEquals(6, new CheckpointDirtyPages(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3).dirtyPagesCount());
    }

    @Test
    void testNextView() {
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages0 = createDirtyPages();
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages1 = createDirtyPages(of(0, 0, 0));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages2 = createDirtyPages(
                of(1, 0, 0), of(1, 0, 1),
                of(1, 1, 0), of(1, 1, 1), of(1, 1, 2),
                of(1, 2, 0)
        );
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages3 = createDirtyPages(
                of(2, 0, 0), of(2, 0, 1), of(2, 0, 2),
                of(3, 1, 10),
                of(777, 666, 100), of(777, 666, 100500)
        );

        CheckpointDirtyPages checkpointDirtyPages0 = new CheckpointDirtyPages(dirtyPages0, dirtyPages1);

        CheckpointDirtyPagesView dirtyPagesView = checkpointDirtyPages0.nextView(null);

        assertThat(dirtyPagesView, equalTo(List.of(of(0, 0, 0))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages1.get1()));

        assertNull(checkpointDirtyPages0.nextView(dirtyPagesView));

        CheckpointDirtyPages checkpointDirtyPages1 = new CheckpointDirtyPages(dirtyPages1, dirtyPages0);

        dirtyPagesView = checkpointDirtyPages1.nextView(null);

        assertThat(dirtyPagesView, equalTo(List.of(of(0, 0, 0))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages1.get1()));

        assertNull(checkpointDirtyPages1.nextView(dirtyPagesView));

        CheckpointDirtyPages checkpointDirtyPages2 = new CheckpointDirtyPages(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3);

        assertNull(checkpointDirtyPages2.nextView(checkpointDirtyPages0.nextView(null)));
        assertNull(checkpointDirtyPages2.nextView(checkpointDirtyPages1.nextView(null)));

        dirtyPagesView = checkpointDirtyPages2.nextView(null);

        assertThat(dirtyPagesView, equalTo(List.of(of(0, 0, 0))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages1.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(1, 0, 0), of(1, 0, 1))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages2.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(1, 1, 0), of(1, 1, 1), of(1, 1, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages2.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(1, 2, 0))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages2.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(2, 0, 0), of(2, 0, 1), of(2, 0, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages3.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(3, 1, 10))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages3.get1()));

        dirtyPagesView = checkpointDirtyPages2.nextView(dirtyPagesView);

        assertThat(dirtyPagesView, equalTo(List.of(of(777, 666, 100), of(777, 666, 100500))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dirtyPages3.get1()));

        assertNull(checkpointDirtyPages2.nextView(dirtyPagesView));
    }

    private FullPageId of(int grpId, int partId, int pageIdx) {
        return new FullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), grpId);
    }

    private IgniteBiTuple<PersistentPageMemory, FullPageId[]> createDirtyPages(FullPageId... fullPageIds) {
        Arrays.sort(fullPageIds, Comparator.comparing(FullPageId::groupId).thenComparing(FullPageId::effectivePageId));

        return new IgniteBiTuple<>(mock(PersistentPageMemory.class), fullPageIds);
    }
}
