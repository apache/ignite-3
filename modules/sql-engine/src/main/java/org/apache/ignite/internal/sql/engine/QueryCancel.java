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

package org.apache.ignite.internal.sql.engine;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.Cancellable;
import org.jetbrains.annotations.Nullable;

/**
 * Holds query cancel state.
 */
public class QueryCancel {
    private final List<Cancellable> cancelActions = new ArrayList<>(3);

    private Reason reason;

    private volatile CompletableFuture<Void> timeoutFut;

    /**
     * Adds a cancel action. If operation has already been canceled, throws a {@link QueryCancelledException}.
     *
     * <p>NOTE: If the operation is cancelled, this method will immediately invoke the given action
     * and then throw a {@link QueryCancelledException}.
     *
     * @param clo Cancel action.
     */
    public synchronized void add(Cancellable clo) throws QueryCancelledException {
        assert clo != null;

        if (reason != null) {
            boolean timeout = reason == Reason.TIMEOUT;

            // Immediately invoke a cancel action, if already cancelled.
            // Otherwise the caller is required to catch QueryCancelledException and call an action manually.
            try {
                clo.cancel(timeout);
            } catch (Exception ignore) {
                // Do nothing
            }

            String message = timeout ? QueryCancelledException.TIMEOUT_MSG : QueryCancelledException.CANCEL_MSG;
            throw new QueryCancelledException(message);
        }

        cancelActions.add(clo);
    }

    /**
     * Removes the given callback.
     *
     * @param clo Callback.
     */
    public synchronized void remove(Cancellable clo) {
        assert clo != null;

        cancelActions.remove(clo);
    }

    /**
     * Schedules a timeout after {@code timeoutMillis} milliseconds.
     * Call be called only once.
     *
     * @param scheduler Scheduler to trigger an action.
     * @param timeoutMillis Timeout in milliseconds.
     * @return Future that will be completed when the timeout is reached.
     */
    public synchronized CompletableFuture<Void> setTimeout(ScheduledExecutorService scheduler, long timeoutMillis) {
        assert reason == null : "Cannot set a timeout when cancelled";
        assert timeoutFut == null : "Timeout has already been set";

        CompletableFuture<Void> fut = new CompletableFuture<>();
        fut.thenAccept((r) -> doCancel(Reason.TIMEOUT));

        ScheduledFuture<?> f = scheduler.schedule(() -> {
            fut.complete(null);
        }, timeoutMillis, MILLISECONDS);

        add((timeout) -> {
            // Cancel the future if we didn't timeout,
            // since in the case of a timeout it is already completed.
            if (!timeout) {
                f.cancel(false);
            }
        });

        this.timeoutFut = fut;
        return fut;
    }

    /**
     * Returns the deadline of the operation.
     *
     * <p>Can be null if a query has no timeout.
     */
    public @Nullable CompletableFuture<Void> timeoutFuture() {
        return timeoutFut;
    }

    /**
     * Executes cancel closure.
     */
    public synchronized void cancel() {
        doCancel(Reason.CANCEL);
    }

    private void doCancel(Reason reason) {
        if (this.reason != null) {
            return;
        }

        boolean timeout = reason == Reason.TIMEOUT;
        this.reason = reason;

        IgniteInternalException ex = null;

        // Run actions in the reverse order.
        for (int i = cancelActions.size() - 1; i >= 0; i--) {
            try {
                Cancellable act = cancelActions.get(i);

                act.cancel(timeout);
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteInternalException(INTERNAL_ERR, e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    enum Reason {
        CANCEL,
        TIMEOUT
    }
}
