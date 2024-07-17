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

package org.apache.ignite.internal.util;

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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.CursorClosedException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Test class to verify {@link AsyncWrapper}. */
public class AsyncWrapperSelfTest extends BaseIgniteAbstractTest {
    /**
     * The very first invocation of {@link AsyncCursor#requestNextAsync requestNext} on the empty cursor should complete normally, follow
     * invocation should be completed exceptionally.
     */
    @Test
    public void testEmpty() {
        var cursor = new AsyncWrapper<>(Collections.emptyIterator());

        await(cursor.requestNextAsync(20).thenAccept(batch -> assertThat(batch.items(), empty())));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request the exact amount of rows, follow invocation of {@link AsyncCursor#requestNextAsync requestNext} should be completed
     * exceptionally.
     */
    @Test
    public void testNotEmptyRequestExact() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNextAsync(data.size()).thenAccept(batch -> assertThat(batch.items(), equalTo(data))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request several times by 1 row. After the whole iterator will be drained, the next invocation
     * of {@link AsyncCursor#requestNextAsync requestNext} should be completed exceptionally.
     */
    @Test
    public void testNotEmptyRequestLess() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNextAsync(1).thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 1)))));
        await(cursor.requestNextAsync(1).thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(1, 2)))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Request the greater amount of rows, follow invocation of {@link AsyncCursor#requestNextAsync requestNext} should complete
     * exceptionally.
     */
    @Test
    public void testNotEmptyRequestMore() {
        var data = List.of(1, 2);
        var cursor = new AsyncWrapper<>(data.iterator());

        await(cursor.requestNextAsync(data.size() * 2).thenAccept(batch -> assertThat(batch.items(), equalTo(data))));

        assertCursorHasNoMoreRow(cursor);
    }

    /**
     * Call to {@link AsyncCursor#closeAsync()} should be passed to delegate in case the latter implements {@link AutoCloseable}.
     */
    @Test
    public void testClosePropagatedToDelegate() throws Exception {
        var mockIt = (ClosableIterator<Object>) Mockito.mock(ClosableIterator.class);
        var cursor = new AsyncWrapper<>(mockIt);

        await(cursor.closeAsync());

        Mockito.verify(mockIt).close();
    }

    @Test
    public void wrapperCanBeClosedEvenIfIteratorFutureCompletedExceptionally() {
        var cursor = new AsyncWrapper<>(CompletableFuture.failedFuture(new RuntimeException()), Runnable::run);

        await(cursor.closeAsync());
    }

    /**
     * All calls to {@link AsyncCursor#requestNextAsync(int)} should be chained and executed in the proper order.
     */
    @Test
    public void testRequestsChainedAndExecutedAfterCursorInited() {
        var data = List.of(1, 2, 3, 4, 5, 6);
        var initFut = new CompletableFuture<Iterator<Integer>>();
        var cursor = new AsyncWrapper<>(initFut, ForkJoinPool.commonPool());

        var stage1 = cursor.requestNextAsync(3)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 3))));
        var stage2 = cursor.requestNextAsync(3)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(3, 6))));
        var stage3 = cursor.requestNextAsync(1)
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
     * Call to {@link AsyncCursor#closeAsync()} should be chained as well.
     */
    @Test
    public void testCloseCancelsIncompleteFutures() {
        var data = List.of(1, 2);
        var initFut = new CompletableFuture<Iterator<Integer>>();
        var cursor = new AsyncWrapper<>(initFut, ForkJoinPool.commonPool());

        var stage1 = cursor.requestNextAsync(1)
                .thenAccept(batch -> assertThat(batch.items(), equalTo(data.subList(0, 1))))
                .exceptionally(ex -> {
                    assertInstanceOf(CompletionException.class, ex);
                    assertInstanceOf(CursorClosedException.class, ex.getCause());

                    return null;
                });
        var stage2 = cursor.closeAsync();
        var stage3 = cursor.requestNextAsync(1)
                .exceptionally(ex -> {
                    assertInstanceOf(CursorClosedException.class, ex);

                    return null;
                });

        assertTrue(stage1.toCompletableFuture().isDone());
        assertFalse(stage2.toCompletableFuture().isDone());
        assertTrue(stage3.toCompletableFuture().isDone());

        initFut.complete(data.iterator());

        await(stage1);
        await(stage2);
        await(stage3);
    }

    private static void assertCursorHasNoMoreRow(AsyncCursor<?> cursor) {
        await(cursor.requestNextAsync(1).exceptionally(ex -> {
            assertInstanceOf(NoSuchElementException.class, ex);

            return null;
        }));
    }

    private interface ClosableIterator<T> extends Iterator<T>, AutoCloseable {

    }
}
