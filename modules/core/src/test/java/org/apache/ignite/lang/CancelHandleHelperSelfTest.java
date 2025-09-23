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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = new CompletableFuture<>();

        // Attach it to some operation hasn't completed yet
        CancelHandleHelper.addCancelAction(token, action, f);
        verify(action, times(1)).run();

        cancelHandle.cancelAsync().join();
        // We do not wait for cancellation to complete because
        // operation has not started yet.
        assertFalse(f.isDone());

        // Action runs immediately
        CancelHandleHelper.addCancelAction(token, action, f);
        verify(action, times(2)).run();
    }

    @Test
    public void testRunCancelActionImmediatelyIfCancelAsyncCalled() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();

        cancelHandle.cancelAsync();
        assertTrue(cancelHandle.isCancelled());

        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = new CompletableFuture<>();

        // Attach it to some operation hasn't completed yet
        CancelHandleHelper.addCancelAction(token, action, f);
        verify(action, times(1)).run();

        cancelHandle.cancelAsync().join();
        // We do not wait for cancellation to complete because
        // operation has not started yet.
        assertFalse(f.isDone());

        // Action runs immediately
        CancelHandleHelper.addCancelAction(token, action, f);
        verify(action, times(2)).run();
    }

    @Test
    public void testArgumentsMustNotBeNull() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = nullCompletedFuture();

        {
            NullPointerException err = assertThrows(
                    NullPointerException.class,
                    () -> CancelHandleHelper.addCancelAction(null, action, f)
            );
            assertEquals("token", err.getMessage());
        }

        {
            NullPointerException err = assertThrows(
                    NullPointerException.class,
                    () -> CancelHandleHelper.addCancelAction(token, null, f)
            );
            assertEquals("cancelAction", err.getMessage());
        }

        {
            NullPointerException err = assertThrows(
                    NullPointerException.class,
                    () -> CancelHandleHelper.addCancelAction(token, action, null)
            );
            assertEquals("completionFut", err.getMessage());
        }
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

        IgniteException err = assertThrows(IgniteException.class, cancelHandle::cancel);

        assertEquals("Failed to cancel an operation", err.getMessage());
        assertEquals(Common.INTERNAL_ERR, err.code(), err.toString());
        assertEquals(Arrays.asList(e1, e2), Arrays.asList(err.getSuppressed()));
    }
}
