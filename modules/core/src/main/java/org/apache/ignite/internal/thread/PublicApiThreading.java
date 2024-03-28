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

package org.apache.ignite.internal.thread;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Logic related to the threading concern of public APIs: it allows to prevent Ignite thread hijack by the user's code and provides
 * mechanisms supporting this protection.
 */
public class PublicApiThreading {
    /**
     * Prevents Ignite internal threads from being hijacked by the user code. If that happened, the user code could have blocked
     * Ignite threads deteriorating progress.
     *
     * <p>This is done by completing the future in the async continuation thread pool if it would have been completed in an Ignite thread.
     *
     * @param originalFuture Operation future.
     * @param asyncContinuationExecutor Executor to which execution will be resubmitted when leaving asynchronous public API endpoints
     *     (to prevent the user from stealing Ignite threads).
     * @return Future that will be completed in the async continuation thread pool ({@link ForkJoinPool#commonPool()} by default).
     */
    public static <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture, Executor asyncContinuationExecutor) {
        if (originalFuture.isDone()) {
            return originalFuture;
        }

        // The future is not complete yet, so it will be completed on an Ignite thread, so we need to complete the user-facing future
        // in the continuation pool.
        return originalFuture.whenCompleteAsync((res, ex) -> {}, asyncContinuationExecutor);
    }
}
