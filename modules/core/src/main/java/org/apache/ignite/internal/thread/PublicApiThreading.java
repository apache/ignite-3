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
import java.util.function.Supplier;

/**
 * Logic related to the threading concern of public APIs: it allows to prevent Ignite thread hijack by the user's code and provides
 * mechanisms supporting this protection.
 */
public class PublicApiThreading {
    private static final ThreadLocal<Boolean> INTERNAL_CALL = new ThreadLocal<>();

    /**
     * Raises the 'internal call' state; the state is stored in the current thread.
     */
    public static void startInternalCall() {
        INTERNAL_CALL.set(true);
    }

    /**
     * Clears the 'internal call' state; the state is stored in the current thread.
     */
    public static void endInternalCall() {
        INTERNAL_CALL.remove();
    }

    /**
     * Executes the call while the 'internal call' state is raised; so if the call invokes {@link #inInternalCall()},
     * if will get {@code true}.
     *
     * @param call Call to execute as internal.
     * @return Call result.
     */
    public static <T> T doInternalCall(Supplier<T> call) {
        boolean wasInInternalCallBefore = inInternalCall();

        startInternalCall();

        try {
            return call.get();
        } finally {
            if (!wasInInternalCallBefore) {
                endInternalCall();
            }
        }
    }

    /**
     * Returns {@code} true if the 'internal call' status is currently raised in the current thread.
     */
    public static boolean inInternalCall() {
        Boolean value = INTERNAL_CALL.get();
        return value != null && value;
    }

    /**
     * Prevents Ignite internal threads from being hijacked by the user code. If that happened, the user code could have blocked
     * Ignite threads deteriorating progress.
     *
     * <p>This is done by completing the future in the async continuation thread pool if it would have been completed in an Ignite thread.
     *
     * <p>The switch to the async continuation pool is also skipped when it's known that the call is made by other Ignite component
     * and not by the user.
     *
     * @param originalFuture Operation future.
     * @param asyncContinuationExecutor Executor to which execution will be resubmitted when leaving asynchronous public API endpoints
     *     (to prevent the user from stealing Ignite threads).
     * @return Future that will be completed in the async continuation thread pool ({@link ForkJoinPool#commonPool()} by default).
     */
    public static  <T> CompletableFuture<T> preventThreadHijack(CompletableFuture<T> originalFuture, Executor asyncContinuationExecutor) {
        if (originalFuture.isDone() || inInternalCall()) {
            return originalFuture;
        }

        // The future is not complete yet, so it will be completed on an Ignite thread, so we need to complete the user-facing future
        // in the continuation pool.
        return originalFuture.whenCompleteAsync((res, ex) -> {}, asyncContinuationExecutor);
    }
}
