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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Future utils test.
 */
public class ClientFutureUtilsTest {
    @Test
    public void testGetNowSafe() {
        assertNull(ClientFutureUtils.getNowSafe(nullCompletedFuture()));
        assertNull(ClientFutureUtils.getNowSafe(CompletableFuture.failedFuture(new Exception("fail"))));
        assertNull(ClientFutureUtils.getNowSafe(new CompletableFuture<>()));
        assertEquals("test", ClientFutureUtils.getNowSafe(CompletableFuture.completedFuture("test")));
    }

    @Test
    public void testDoWithRetryAsyncWithCompletedFutureReturnsResult() {
        var res = ClientFutureUtils.doWithRetryAsync(
                () -> CompletableFuture.completedFuture("test"),
                ctx -> false
        ).join();

        assertEquals("test", res);
    }

    @Test
    public void testDoWithRetryAsyncWithFailedFutureReturnsExceptionWithSuppressedList() {
        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> CompletableFuture.failedFuture(new Exception("fail_" + counter.get())),
                ctx -> counter.incrementAndGet() < 3
        );

        var completionEx = assertThrows(CompletionException.class, fut::join);
        var ex = (Exception) completionEx.getCause();

        assertEquals(2, ex.getSuppressed().length);

        assertEquals("fail_0", ex.getMessage());
        assertEquals("fail_1", ex.getSuppressed()[0].getMessage());
        assertEquals("fail_2", ex.getSuppressed()[1].getMessage());
    }

    @Test
    public void testDoWithRetryAsyncSucceedsAfterRetries() {
        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> counter.getAndIncrement() < 3
                        ? CompletableFuture.failedFuture(new Exception("fail"))
                        : CompletableFuture.completedFuture("test"),
                ctx -> {
                    assertNotNull(ctx.lastError());

                    //noinspection DataFlowIssue
                    assertEquals("fail", ctx.lastError().getMessage());

                    return ctx.attempt < 5;
                }
        );

        assertEquals("test", fut.join());
    }

    @Test
    public void testDoWithRetryAsyncWithExceptionInDelegateReturnsFailedFuture() {
        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> {
                    if (counter.incrementAndGet() > 1) {
                        throw new RuntimeException("fail1");
                    } else {
                        return CompletableFuture.failedFuture(new Exception("fail2"));
                    }
                },
                ctx -> true
        );

        var ex = assertThrows(CompletionException.class, fut::join);
        assertEquals("fail1", ex.getCause().getMessage());
    }

    @Test
    public void testDoWithRetryAsyncPreventsDuplicatesAndSelfReferenceInSuppressedExceptions() {
        var counter = new AtomicInteger();

        var ex1 = new Exception("1");
        var ex2 = new Exception("2");

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> {
                    switch (counter.get()) {
                        case 0: // Self.
                            return CompletableFuture.failedFuture(ex1);

                        case 1: // Other.
                            return CompletableFuture.failedFuture(ex2);

                        case 2: // Self wrapped.
                            return CompletableFuture.failedFuture(new Exception(ex1));

                        case 3: // Other wrapped.
                            return CompletableFuture.failedFuture(new Exception(ex2));

                        default:
                            return CompletableFuture.failedFuture(new Exception("Other"));
                    }
                },
                ctx -> counter.incrementAndGet() < 4
        );

        var completionEx = assertThrows(CompletionException.class, fut::join);
        var ex = (Exception) completionEx.getCause();

        assertEquals(ex1, ex, "Expected the first exception to be the main one.");

        Throwable[] suppressed = ex.getSuppressed();
        assertEquals(1, suppressed.length, "Should not have duplicate suppressed exceptions.");
        assertEquals(ex2, unwrapCause(suppressed[0]));
    }
}
