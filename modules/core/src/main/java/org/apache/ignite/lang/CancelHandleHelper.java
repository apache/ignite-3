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
     * Attaches a cancellable operation to the given token.
     *
     * <p>NOTE: If this token has already been cancelled, this method immediately invokes the {@code cancelAction.run()}.
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

        if (token instanceof CancellationTokenImpl) {
            CancellationTokenImpl t = (CancellationTokenImpl) token;
            t.addCancelAction(cancelAction, completionFut);
        } else {
            throw new IllegalArgumentException("Unexpected CancellationToken: " + token.getClass());
        }
    }

    /**
     * Returns a future associated with this cancellation token's handle.
     *
     * @param token Cancellation token.
     */
    public static CompletableFuture<Void> getCancellationFuture(CancellationToken token) {
        if (token instanceof CancellationTokenImpl) {
            CancellationTokenImpl t = (CancellationTokenImpl) token;
            return t.cancelHandleFut();
        } else {
            throw new IllegalArgumentException("Unexpected CancellationToken: " + token.getClass());
        }
    }
}
