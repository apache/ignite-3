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
import org.jetbrains.annotations.Nullable;

/**
 * Logic related to the threading concern of public APIs: it allows to prevent Ignite thread hijack by the user's code and provides
 * mechanisms supporting this protection.
 */
public class PublicApiThreading {
    private static final ThreadLocal<ApiEntryRole> THREAD_ROLE = new ThreadLocal<>();

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

    /**
     * Returns the role of the current thread.
     *
     * @see ApiEntryRole
     */
    private static @Nullable ApiEntryRole getThreadRole() {
        return THREAD_ROLE.get();
    }

    /**
     * Changes the role of the current thread.
     *
     * @param role New role.
     * @see ApiEntryRole
     */
    public static void setThreadRole(@Nullable ApiEntryRole role) {
        THREAD_ROLE.set(role);
    }

    /**
     * Returns {@code true} if the current thread is executing a sync public API call.
     *
     * @see ApiEntryRole#SYNC_PUBLIC_API
     */
    public static boolean executingSyncPublicApi() {
        return getThreadRole() == ApiEntryRole.SYNC_PUBLIC_API;
    }

    /**
     * Returns {@code true} if the current thread is executing an async public API call.
     *
     * @see ApiEntryRole#ASYNC_PUBLIC_API
     */
    public static boolean executingAsyncPublicApi() {
        return getThreadRole() == ApiEntryRole.ASYNC_PUBLIC_API;
    }

    /**
     * Executes an operation marking the current thread as {@link ApiEntryRole#SYNC_PUBLIC_API} if it's not
     * already marked with any role.
     *
     * <p>A thread having this role is allowed to do any {@link ThreadOperation}.
     *
     * @param operation Operation to execute.
     * @return Whatever the operation returns.
     * @see ApiEntryRole#SYNC_PUBLIC_API
     */
    public static <T> T execUserSyncOperation(Supplier<T> operation) {
        return executeWithRole(ApiEntryRole.SYNC_PUBLIC_API, operation);
    }

    /**
     * Executes an operation marking the current thread as {@link ApiEntryRole#SYNC_PUBLIC_API} if it's not
     * already marked with any role.
     *
     * <p>A thread having this role is allowed to do any {@link ThreadOperation}.
     *
     * @param operation Operation to execute.
     * @see ApiEntryRole#SYNC_PUBLIC_API
     */
    public static void execUserSyncOperation(Runnable operation) {
        execUserSyncOperation(() -> {
            operation.run();
            return null;
        });
    }

    /**
     * Executes an operation marking the current thread as {@link ApiEntryRole#ASYNC_PUBLIC_API} if it's not
     * already marked with any role.
     *
     * <p>A thread having this role will have switched to an appropriate internal Ignite thread if it tries to touch the storage.
     *
     * @param operation Operation to execute.
     * @return Whatever the operation returns.
     * @see ApiEntryRole#ASYNC_PUBLIC_API
     */
    public static <T> CompletableFuture<T> execUserAsyncOperation(Supplier<CompletableFuture<T>> operation) {
        return executeWithRole(ApiEntryRole.ASYNC_PUBLIC_API, operation);
    }

    private static <T> T executeWithRole(ApiEntryRole newRole, Supplier<T> operation) {
        ApiEntryRole oldRole = getThreadRole();

        if (oldRole != null) {
            return operation.get();
        } else {
            setThreadRole(newRole);

            try {
                return operation.get();
            } finally {
                setThreadRole(oldRole);
            }
        }
    }

    /**
     * Role that a thread plays in relation to the public API.
     */
    public enum ApiEntryRole {
        /**
         * The thread executes a sync public API call. A thread having this role is allowed to do any {@link ThreadOperation}.
         */
        SYNC_PUBLIC_API,
        /**
         * The thread executes an async public API call. A thread having this role will have switched to an appropriate internal
         * Ignite thread if it tries to touch the storage.
         */
        ASYNC_PUBLIC_API
    }
}
