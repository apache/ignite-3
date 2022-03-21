package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.ClosedCursorException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Test class to verify {@link org.apache.ignite.internal.sql.engine.exec.AsyncWrapper}. */
public class AsyncWrapperSelfTest {
    /**
     * The very first invocation of {@link AsyncCursor#requestNext requestNext} on the empty cursor should complete normally, follow
     * invocation should be completed exceptionally.
     */
    @Test
    public void testEmpty() {
        var cursor = new AsyncWrapper<>(Collections.emptyIterator());

        await(cursor.requestNext(20).thenAccept(batch -> assertThat(batch.items(), empty())));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request the exact amount of rows, follow invocation of {@link AsyncCursor#requestNext requestNext} should be completed exceptionally.
     */
    @Test
    public void testNotEmptyRequestExact() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNext(data.size()).thenAccept(batch -> assertThat(batch.items(), equalTo(data))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request several times by 1 row. After the whole iterator will be drained, the next invocation
     * of {@link AsyncCursor#requestNext requestNext} should be completed exceptionally.
     */
    @Test
    public void testNotEmptyRequestLess() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNext(1).thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 1)))));
        await(cursor.requestNext(1).thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(1, 2)))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request the greater amount of rows, follow invocation of {@link AsyncCursor#requestNext requestNext} should complete exceptionally.
     */
    @Test
    public void testNotEmptyRequestMore() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNext(data.size() * 2).thenAccept(batch -> assertThat(batch.items(), equalTo(data))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Call to {@link AsyncCursor#close()} should be passed to delegate in case the latter implements {@link AutoCloseable}.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testClosePropagatedToDelegate() throws Exception {
        var mockIt = (ClosableIterator<Object>) Mockito.mock(ClosableIterator.class);
        var cursor = new AsyncWrapper<>(mockIt);

        await(cursor.close());

        Mockito.verify(mockIt).close();
    }

    /**
     * All calls to {@link AsyncCursor#requestNext(int)} should be chained and executed in the proper order.
     */
    @Test
    public void testRequestsChainedAndExecutedAfterCursorInited() {
        var data = List.of(1, 2);
        var initFut = new CompletableFuture<Iterator<Integer>>();
        var cursor = new AsyncWrapper<>(initFut, ForkJoinPool.commonPool());

        var stage1 = cursor.requestNext(1)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 1))));
        var stage2 = cursor.requestNext(1)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(1, 2))));
        var stage3 = cursor.requestNext(1)
                .exceptionally(ex -> {
                    assertInstanceOf(NoSuchElementException.class, ex);

                    return null;
                });

        assertFalse(stage1.toCompletableFuture().isDone());
        assertFalse(stage2.toCompletableFuture().isDone());
        assertFalse(stage3.toCompletableFuture().isDone());

        initFut.complete(data.iterator());

        await(stage1);
        await(stage2);
        await(stage3);
    }

    /**
     * Call to {@link AsyncCursor#close()} should be chained as well.
     */
    @Test
    public void testCloseChainedAsWell() {
        var data = List.of(1, 2);
        var initFut = new CompletableFuture<Iterator<Integer>>();
        var cursor = new AsyncWrapper<>(initFut, ForkJoinPool.commonPool());

        var stage1 = cursor.requestNext(1)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 1))));
        var stage2 = cursor.close();
        var stage3 = cursor.requestNext(1)
                .exceptionally(ex -> {
                    assertInstanceOf(ClosedCursorException.class, ex);

                    return null;
                });

        assertFalse(stage1.toCompletableFuture().isDone());
        assertFalse(stage2.toCompletableFuture().isDone());
        assertTrue(stage3.toCompletableFuture().isDone());

        initFut.complete(data.iterator());

        await(stage1);
        await(stage2);
    }

    private static void assertCursorHasNoMoreRow(AsyncCursor<?> cursor) {
        await(cursor.requestNext(1).exceptionally(ex -> {
            assertInstanceOf(NoSuchElementException.class, ex);

            return null;
        }));
    }

    private interface ClosableIterator<T> extends Iterator<T>, AutoCloseable {

    }
}