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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Future utils test.
 */
public class ClientFutureUtilsTest {
    @Test
    public void testGetNowSafe() {
        assertNull(ClientFutureUtils.getNowSafe(CompletableFuture.completedFuture(null)));
        assertNull(ClientFutureUtils.getNowSafe(CompletableFuture.failedFuture(new Exception("fail"))));
        assertNull(ClientFutureUtils.getNowSafe(new CompletableFuture<>()));
        assertEquals("test", ClientFutureUtils.getNowSafe(CompletableFuture.completedFuture("test")));
    }

    @Test
    public void testDoWithRetryAsyncWithCompletedFutureReturnsResult() {
        var res = ClientFutureUtils.doWithRetryAsync(
            () -> CompletableFuture.completedFuture("test"),
            null,
            ctx -> false
        ).join();

        assertEquals("test", res);
    }

    @Test
    public void testDoWithRetryAsyncWithResultValidatorRejectsAllThrowsIllegalState() {
        var fut = ClientFutureUtils.doWithRetryAsync(
            () -> CompletableFuture.completedFuture("test"),
            x -> false,
            ctx -> false
        );

        var ex = assertThrows(CompletionException.class, fut::join);
        assertSame(IllegalStateException.class, ex.getCause().getClass());
    }
}
