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

package org.apache.ignite.lang;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CancelHandleHelper}.
 */
public class CancelHandleHelperSelfTest extends BaseIgniteAbstractTest {

    @Test
    public void testCancelSync() throws InterruptedException {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        // Initially is not cancelled
        assertFalse(cancelHandle.isCancelled());

        CountDownLatch operationLatch = new CountDownLatch(1);
        CompletableFuture<Void> cancelFut = new CompletableFuture<>();

        Runnable cancelAction = () -> {
            try {
                operationLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            cancelFut.complete(null);
        };

        CancelHandleHelper.addCancelAction(token, cancelAction, cancelFut);

        CountDownLatch cancelHandleLatch = new CountDownLatch(1);

        // Call cancel in another thread.
        Thread thread = new Thread(() -> {
            cancelHandle.cancel();
            cancelHandleLatch.countDown();
        });
        thread.start();

        // Make it possible for cancelAction to complete, because cancelHandle calls it in its thread.
        operationLatch.countDown();

        // Wait until sync cancel returns.
        cancelHandleLatch.await();

        // Cancellation has completed
        assertTrue(cancelHandle.cancelAsync().isDone());
        assertTrue(cancelHandle.isCancelled());
        assertTrue(cancelHandle.cancelAsync().isDone());

        // Should have no affect
        cancelHandle.cancel();
    }

    @Test
    public void testCancelAsync() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        // Initially is not cancelled
        assertFalse(cancelHandle.isCancelled());
        CountDownLatch operationLatch = new CountDownLatch(1);
        CompletableFuture<Void> cancelFut = new CompletableFuture<>();

        Runnable cancelAction = () -> {
            // Run in another thread to avoid blocking.
            Thread thread = new Thread(() -> {
                try {
                    operationLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                cancelFut.complete(null);
            });
            thread.start();
        };

        CancelHandleHelper.addCancelAction(token, cancelAction, cancelFut);

        // Request cancellation and keep the future, to call it later.
        CompletableFuture<Void> cancelHandleFut = cancelHandle.cancelAsync();
        assertTrue(cancelHandle.isCancelled());

        assertFalse(cancelHandleFut.isDone());
        operationLatch.countDown();

        // Await for cancellation to complete
        cancelHandleFut.join();

        assertTrue(cancelHandle.isCancelled());
        assertTrue(cancelHandle.cancelAsync().isDone());
    }

    @Test
    public void testCancelAsyncReturnsCopy() {
        CancelHandle cancelHandle = CancelHandle.create();

        CompletableFuture<Void> f1 = cancelHandle.cancelAsync();
        CompletableFuture<Void> f2 = cancelHandle.cancelAsync();
        assertNotSame(f1, f2);
    }

    @Test
    public void testRunCancelActionImmediatelyIfCancelSyncCalled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();
        assertTrue(cancelHandle.isCancelled());

        AtomicInteger counter = new AtomicInteger();
        Runnable action = counter::incrementAndGet;
        CompletableFuture<Void> f = new CompletableFuture<>();

        // Attach it to some operation hasn't completed yet
        CancelHandleHelper.addCancelAction(token, action, f);
        assertThat(counter.get(), is(1));

        cancelHandle.cancelAsync().join();
        // We do not wait for cancellation to complete because
        // operation has not started yet.
        assertFalse(f.isDone());

        // Action runs immediately
        CancelHandleHelper.addCancelAction(token, action, f);
        assertThat(counter.get(), is(2));
    }

    @Test
    public void testRunCancelActionImmediatelyIfCancelAsyncCalled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancelAsync();
        assertTrue(cancelHandle.isCancelled());

        AtomicInteger counter = new AtomicInteger();
        Runnable action = counter::incrementAndGet;
        CompletableFuture<Void> f = new CompletableFuture<>();

        // Attach it to some operation hasn't completed yet
        CancelHandleHelper.addCancelAction(token, action, f);
        assertThat(counter.get(), is(1));

        cancelHandle.cancelAsync().join();
        // We do not wait for cancellation to complete because
        // operation has not started yet.
        assertFalse(f.isDone());

        // Action runs immediately
        CancelHandleHelper.addCancelAction(token, action, f);
        assertThat(counter.get(), is(2));
    }

    @Test
    public void testArgumentsMustNotBeNull() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = () -> {};
        CompletableFuture<Void> f = nullCompletedFuture();

        assertThrows(
                NullPointerException.class,
                () -> CancelHandleHelper.addCancelAction(null, action, f),
                "token"
        );

        assertThrows(
                NullPointerException.class,
                () -> CancelHandleHelper.addCancelAction(token, null, f),
                "cancelAction"
        );

        assertThrows(
                NullPointerException.class,
                () -> CancelHandleHelper.addCancelAction(token, action, null),
                "completionFut"
        );
    }

    @Test
    public void testMultipleOperations() {
        class Operation {
            private final CountDownLatch latch = new CountDownLatch(1);
            private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();
            private final Runnable cancelAction = () -> {
                // Run in another thread to avoid blocking.
                Thread thread = new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    cancelFut.complete(null);
                });
                thread.start();
            };
        }

        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        Operation operation1 = new Operation();
        Operation operation2 = new Operation();

        CancelHandleHelper.addCancelAction(token, operation1.cancelAction, operation1.cancelFut);
        CancelHandleHelper.addCancelAction(token, operation2.cancelAction, operation2.cancelFut);

        cancelHandle.cancelAsync();
        assertFalse(operation1.cancelFut.isDone());

        // Cancel the first operation
        operation1.latch.countDown();
        operation1.cancelFut.join();

        // The cancelHandle is still not done
        assertFalse(cancelHandle.cancelAsync().isDone());

        // Cancel the second operation
        operation2.latch.countDown();
        operation2.cancelFut.join();

        cancelHandle.cancelAsync().join();
        assertTrue(cancelHandle.cancelAsync().isDone());
    }

    @Test
    public void testExceptionsInCancelActionsAreWrapped() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        RuntimeException e1 = new RuntimeException("e1");
        Runnable r1 = () -> {
            throw e1;
        };
        CompletableFuture<Object> f1 = new CompletableFuture<>();

        RuntimeException e2 = new RuntimeException("e2");
        Runnable r2 = () -> {
            throw e2;
        };
        CompletableFuture<Object> f2 = new CompletableFuture<>();

        CancelHandleHelper.addCancelAction(token, r1, f1);
        CancelHandleHelper.addCancelAction(token, r2, f2);

        f1.complete(null);
        f2.complete(null);

        Throwable err = assertThrowsWithCode(
                IgniteException.class,
                Common.INTERNAL_ERR,
                cancelHandle::cancel,
                "Failed to cancel an operation"
        );

        assertThat(err.getSuppressed(), arrayContaining(e1, e2));
    }

    @Test
    public void testTokenIsCancelled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        assertThat(token.isCancelled(), is(false));

        cancelHandle.cancel();

        assertThat(token.isCancelled(), is(true));
    }

    @Test
    public void testTokenIsCancelledAsync() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        assertThat(token.isCancelled(), is(false));

        cancelHandle.cancelAsync();

        assertThat(token.isCancelled(), is(true));
    }

    @Test
    public void testListenCallbackInvokedOnCancel() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AtomicBoolean callbackCalled = new AtomicBoolean();
        token.addListener(() -> callbackCalled.set(true));

        assertThat(callbackCalled.get(), is(false));

        cancelHandle.cancel();

        assertThat(callbackCalled.get(), is(true));
    }

    @Test
    public void testListenCallbackInvokedImmediatelyIfAlreadyCancelled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();

        AtomicBoolean callbackCalled = new AtomicBoolean();
        token.addListener(() -> callbackCalled.set(true));

        assertThat(callbackCalled.get(), is(true));
    }

    @Test
    public void testListenMultipleCallbacks() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AtomicInteger counter = new AtomicInteger();
        token.addListener(counter::incrementAndGet);
        token.addListener(counter::incrementAndGet);
        token.addListener(counter::incrementAndGet);

        cancelHandle.cancel();

        assertThat(counter.get(), is(3));
    }

    @Test
    public void testListenCloseRemovesCallback() throws Exception {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AtomicBoolean callbackCalled = new AtomicBoolean();
        AutoCloseable handle = token.addListener(() -> callbackCalled.set(true));

        // Close the handle before cancellation
        handle.close();

        cancelHandle.cancel();

        // Callback should not have been called
        assertThat(callbackCalled.get(), is(false));
    }

    @Test
    public void testListenCloseAfterCancelIsNoOp() throws Exception {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        AtomicBoolean callbackCalled = new AtomicBoolean();
        AutoCloseable handle = token.addListener(() -> callbackCalled.set(true));

        cancelHandle.cancel();
        assertThat(callbackCalled.get(), is(true));

        // Close after cancel - should not throw
        handle.close();
    }

    @Test
    public void testListenNullCallbackThrows() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        assertThrows(
                NullPointerException.class,
                () -> token.addListener(null),
                "callback"
        );
    }

    @Test
    public void testExceptionInListenerCallbackWrappedWhenAlreadyCancelled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancel();

        // Listener registered after cancellation should wrap exceptions consistently
        // with the cancel() path (IgniteException with INTERNAL_ERR).
        assertThrowsWithCode(
                IgniteException.class,
                Common.INTERNAL_ERR,
                () -> token.addListener(() -> {
                    throw new RuntimeException("listener error");
                }),
                "Failed to cancel an operation"
        );
    }

    @Test
    public void testExceptionsInListenerCallbacksAreWrapped() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        RuntimeException e1 = new RuntimeException("e1");
        token.addListener(() -> {
            throw e1;
        });

        RuntimeException e2 = new RuntimeException("e2");
        token.addListener(() -> {
            throw e2;
        });

        Throwable err = assertThrowsWithCode(
                IgniteException.class,
                Common.INTERNAL_ERR,
                cancelHandle::cancel,
                "Failed to cancel an operation"
        );

        assertThat(err.getSuppressed(), arrayContaining(e1, e2));
    }
}
