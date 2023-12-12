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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;
import static org.apache.ignite.internal.util.IgniteUtils.byteBufferToByteArray;
import static org.apache.ignite.internal.util.IgniteUtils.copyStateTo;
import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;
import static org.apache.ignite.internal.util.IgniteUtils.isPow2;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link IgniteUtils}.
 */
class IgniteUtilsTest extends BaseIgniteAbstractTest {
    /**
     * Tests that all resources are closed by the {@link IgniteUtils#closeAll} even if {@link AutoCloseable#close} throws an exception.
     */
    @Test
    void testCloseAll() {
        class TestCloseable implements AutoCloseable {
            private boolean closed = false;

            /** {@inheritDoc} */
            @Override
            public void close() throws Exception {
                closed = true;

                throw new IOException();
            }
        }

        var closeables = List.of(new TestCloseable(), new TestCloseable(), new TestCloseable());

        Exception e = assertThrows(IOException.class, () -> IgniteUtils.closeAll(closeables));

        assertThat(e.getSuppressed(), arrayWithSize(2));

        closeables.forEach(c -> assertTrue(c.closed));
    }

    @Test
    public void testIsPow2() {
        // Checks int value.

        assertTrue(isPow2(1));
        assertTrue(isPow2(2));
        assertTrue(isPow2(4));
        assertTrue(isPow2(8));
        assertTrue(isPow2(16));
        assertTrue(isPow2(16 * 16));
        assertTrue(isPow2(32 * 32));

        assertFalse(isPow2(-4));
        assertFalse(isPow2(-3));
        assertFalse(isPow2(-2));
        assertFalse(isPow2(-1));
        assertFalse(isPow2(0));
        assertFalse(isPow2(3));
        assertFalse(isPow2(5));
        assertFalse(isPow2(6));
        assertFalse(isPow2(7));
        assertFalse(isPow2(9));

        // Checks long value.

        assertTrue(isPow2(1L));
        assertTrue(isPow2(2L));
        assertTrue(isPow2(4L));
        assertTrue(isPow2(8L));
        assertTrue(isPow2(16L));
        assertTrue(isPow2(16L * 16L));
        assertTrue(isPow2(32L * 32L));

        assertFalse(isPow2(-4L));
        assertFalse(isPow2(-3L));
        assertFalse(isPow2(-2L));
        assertFalse(isPow2(-1L));
        assertFalse(isPow2(0L));
        assertFalse(isPow2(3L));
        assertFalse(isPow2(5L));
        assertFalse(isPow2(6L));
        assertFalse(isPow2(7L));
        assertFalse(isPow2(9L));
    }

    @Test
    void testGetUninterruptibly() throws Exception {
        assertThat(getUninterruptibly(trueCompletedFuture()), equalTo(true));
        assertThat(Thread.currentThread().isInterrupted(), equalTo(false));

        ExecutionException exception0 = assertThrows(
                ExecutionException.class,
                () -> getUninterruptibly(failedFuture(new Exception("test")))
        );

        assertThat(exception0.getCause(), instanceOf(Exception.class));
        assertThat(exception0.getCause().getMessage(), equalTo("test"));
        assertThat(Thread.currentThread().isInterrupted(), equalTo(false));

        CompletableFuture<?> canceledFuture = new CompletableFuture<>();
        canceledFuture.cancel(false);

        assertThrows(CancellationException.class, () -> getUninterruptibly(canceledFuture));
        assertThat(Thread.currentThread().isInterrupted(), equalTo(false));

        // Checks interrupt.

        runAsync(() -> {
            try {
                Thread.currentThread().interrupt();

                getUninterruptibly(nullCompletedFuture());

                assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }).get(1, TimeUnit.SECONDS);
    }

    @Test
    void testAwaitForWorkersStop() throws Exception {
        IgniteWorker worker0 = mock(IgniteWorker.class);
        IgniteWorker worker1 = mock(IgniteWorker.class);

        doThrow(InterruptedException.class).when(worker1).join();

        assertDoesNotThrow(() -> awaitForWorkersStop(List.of(worker0, worker1), false, log));

        verify(worker0, times(0)).cancel();
        verify(worker1, times(0)).cancel();

        verify(worker0, times(1)).join();
        verify(worker1, times(1)).join();

        assertDoesNotThrow(() -> awaitForWorkersStop(List.of(worker0, worker1), true, log));

        verify(worker0, times(1)).cancel();
        verify(worker1, times(1)).cancel();

        verify(worker0, times(2)).join();
        verify(worker1, times(2)).join();
    }

    @Test
    void testCopyStateToNormal() {
        CompletableFuture<Number> result = new CompletableFuture<>();

        completedFuture(2).whenComplete(copyStateTo(result));

        assertThat(result, willBe(equalTo(2)));
    }

    @Test
    void testCopyStateToException() {
        CompletableFuture<Number> result = new CompletableFuture<>();

        CompletableFuture.<Integer>failedFuture(new NumberFormatException()).whenComplete(copyStateTo(result));

        assertThat(result, willThrow(NumberFormatException.class));
    }

    @Test
    void testByteBufferToByteArray() {
        ByteBuffer heapBuffer = ByteBuffer.wrap(new byte[]{0, 1, 2, 3, 4}, 1, 3).slice();
        assertArrayEquals(new byte[] {1, 2, 3}, byteBufferToByteArray(heapBuffer));

        ByteBuffer bigDirectBuffer = ByteBuffer.allocateDirect(5);
        bigDirectBuffer.put(new byte[]{0, 1, 2, 3, 4});

        ByteBuffer smallDirectBuffer = bigDirectBuffer.position(1).limit(4).slice();
        assertArrayEquals(new byte[] {1, 2, 3}, byteBufferToByteArray(smallDirectBuffer));
    }
}
