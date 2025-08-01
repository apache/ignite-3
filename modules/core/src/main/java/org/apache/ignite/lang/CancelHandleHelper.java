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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.CancelHandleImpl.CancellationTokenImpl;

/**
 * Utility class to provide direct access to internals of {@link CancelHandleImpl}.
 */
public final class CancelHandleHelper {

    private CancelHandleHelper() {

    }

    /**
     * Attaches a cancellable operation to the given token. A cancellation procedure started its handle completes
     * when {@code completionFut} completes.
     *
     * <p>NOTE: If a handle, this token is associated with, was cancelled or its cancellation was requested,
     * this method immediately invokes {@code cancelAction.run()} and it this case
     * <b>it never waits for {@code completionFut} to complete</b>.
     *
     * <p>The following methods request cancellation of a handle:
     * <ul>
     *     <li>{@link CancelHandle#cancel()}</li>
     *     <li>{@link CancelHandle#cancelAsync()}</li>
     * </ul>
     *
     * @param token Cancellation token.
     * @param cancelAction Action that terminates an operation.
     * @param completionFut Future that completes when operation completes and all resources it created are released.
     */
    public static void addCancelAction(
            CancellationToken token,
            Runnable cancelAction,
            CompletableFuture<?> completionFut
    ) {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(cancelAction, "cancelAction");
        Objects.requireNonNull(completionFut, "completionFut");

        CancellationTokenImpl t = unwrapToken(token);
        t.addCancelAction(cancelAction, completionFut);
    }

    /**
     * Attaches a future to the given token. A cancellation procedure call {@link CompletableFuture#cancel} and handle completes
     * when {@code completionFut} completes.
     *
     * @param token Cancellation token.
     * @param completionFut Future that completes when operation completes and all resources it created are released.
     */
    public static void addCancelAction(
            CancellationToken token,
            CompletableFuture<?> completionFut
    ) {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(completionFut, "completionFut");

        addCancelAction(token, () -> completionFut.cancel(true), completionFut);
    }

    /**
     * Flag indicating whether cancellation was requested or not.
     *
     * <p>This method will return true even if cancellation has not been completed yet.
     *
     * @return {@code True} if a cancellation was previously requested, {@code false} otherwise.
     */
    public static boolean isCancelled(CancellationToken token) {
        return unwrapToken(token).isCancelled();
    }

    private static CancellationTokenImpl unwrapToken(CancellationToken token) {
        if (token instanceof CancellationTokenImpl) {
            return (CancellationTokenImpl) token;
        } else {
            throw new IllegalArgumentException("Unexpected CancellationToken: " + token.getClass());
        }
    }
}
