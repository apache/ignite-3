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
    public void testDoWithRetryAsyncPreventsSelfReferenceInSuppressedExceptions() {
        var sameException = new Exception("same");
        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> CompletableFuture.failedFuture(sameException),
                ctx -> counter.incrementAndGet() < 3
        );

        var completionEx = assertThrows(CompletionException.class, fut::join);
        var ex = (Exception) completionEx.getCause();

        // Should be the same exception instance.
        assertEquals(sameException, ex);
        // Should not have added itself as suppressed (would create self-reference).
        assertEquals(0, ex.getSuppressed().length);
    }

    @Test
    public void testDoWithRetryAsyncPreventsCircularReferenceInSuppressedChain() {
        var ex1 = new Exception("ex1");
        var ex2 = new Exception("ex2");
        var ex3 = new Exception("ex3");

        // Create a potential circular reference: ex3 has ex1 as suppressed.
        ex3.addSuppressed(ex1);

        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> {
                    int attempt = counter.getAndIncrement();
                    if (attempt == 0) {
                        return CompletableFuture.failedFuture(ex1);
                    } else if (attempt == 1) {
                        return CompletableFuture.failedFuture(ex2);
                    } else {
                        return CompletableFuture.failedFuture(ex3);
                    }
                },
                ctx -> counter.get() < 3
        );

        var completionEx = assertThrows(CompletionException.class, fut::join);
        var resultEx = (Exception) completionEx.getCause();

        // Result should be ex1 with ex2 added as suppressed.
        assertEquals("ex1", resultEx.getMessage());
        assertEquals(1, resultEx.getSuppressed().length);
        assertEquals("ex2", resultEx.getSuppressed()[0].getMessage());
        // ex3 should NOT be added because it would create a circular reference (ex3 already has ex1 suppressed).
    }

    @Test
    public void testDoWithRetryAsyncPreventsCircularReferenceInCauseChain() {
        var ex1 = new Exception("ex1");
        var ex2 = new Exception("ex2", ex1); // ex2's cause is ex1

        var counter = new AtomicInteger();

        var fut = ClientFutureUtils.doWithRetryAsync(
                () -> {
                    int attempt = counter.getAndIncrement();
                    if (attempt == 0) {
                        return CompletableFuture.failedFuture(ex1);
                    } else {
                        return CompletableFuture.failedFuture(ex2);
                    }
                },
                ctx -> counter.get() < 2
        );

        var completionEx = assertThrows(CompletionException.class, fut::join);
        var resultEx = (Exception) completionEx.getCause();

        // Result should be ex1.
        assertEquals("ex1", resultEx.getMessage());
        // ex2 should NOT be added as suppressed because it would create a circular reference (ex2's cause is ex1).
        assertEquals(0, resultEx.getSuppressed().length);
    }
}
