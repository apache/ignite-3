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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link CancelHandleHelper}.
 */
public class CancelHandleHelperTest extends BaseIgniteAbstractTest {

    @Test
    public void testRunCancelAction() throws InterruptedException {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = new CompletableFuture<>();

        CancelHandleHelper.addCancelAction(token, action,  f);
        assertFalse(f.isDone());
        verify(action, times(0)).run();

        CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread(() -> {
            // Complete cancellation in another thread because cancel() awaits its completion
            f.complete(null);
            latch.countDown();
        });
        thread.start();

        // Cancel asynchronously
        cancelHandle.cancelAsync();

        latch.await();
        assertTrue(cancelHandle.isCancelled());

        cancelHandle.cancelAsync().join();

        verify(action, times(1)).run();
    }

    @Test
    public void testRunCancelActionImmediatley() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = new CompletableFuture<>();

        cancelHandle.cancel();
        assertTrue(cancelHandle.isCancelled());

        CancelHandleHelper.addCancelAction(token, action,  f);
        verify(action, times(1)).run();
    }

    @Test
    public void testRunCancelActionIfFutureIsDone() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = CompletableFuture.completedFuture(null);

        CancelHandleHelper.addCancelAction(token, action,  f);

        cancelHandle.cancel();
        verify(action, times(1)).run();

        cancelHandle.cancel();

        assertTrue(cancelHandle.isCancelled());
    }

    @Test
    public void testArgumentsMustNotBeNull() {
        CancelHandle cancelHandle = CancelHandle.create();
        CancellationToken token = cancelHandle.token();
        Runnable action = Mockito.mock(Runnable.class);
        CompletableFuture<Void> f = CompletableFuture.completedFuture(null);

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
}
