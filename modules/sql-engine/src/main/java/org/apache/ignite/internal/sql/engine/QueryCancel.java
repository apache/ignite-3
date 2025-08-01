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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.util.Cancellable;

/**
 * Holds query cancel state.
 */
public class QueryCancel {
    private final CompletableFuture<Reason> state = new CompletableFuture<>();

    /**
     * Adds a cancel action. If operation has already been canceled, throws a {@link QueryCancelledException}.
     *
     * <p>NOTE: If the operation is cancelled, this method will immediately invoke the given action
     * and then throw a {@link QueryCancelledException}.
     *
     * @param clo Cancel action.
     * @throws QueryCancelledException If operation has been already cancelled.
     */
    public void add(Cancellable clo) throws QueryCancelledException {
        assert clo != null;

        state.thenAccept(reason -> clo.cancel(reason == Reason.TIMEOUT));

        throwIfCancelled();
    }

    /**
     * Attach child cancellation to the current one. If this has already been canceled, throws a {@link QueryCancelledException}.
     *
     * <p>NOTE: If the operation is cancelled, this method will immediately trigger cancellation of a given the given cancellation
     * and then throw a {@link QueryCancelledException}.
     *
     * @param another Another cancellation.
     * @throws QueryCancelledException If operation has been already cancelled.
     */
    public void attach(QueryCancel another) throws QueryCancelledException {
        state.thenAccept(another.state::complete);

        throwIfCancelled();
    }

    /**
     * Schedules a timeout after {@code timeoutMillis} milliseconds.
     * Call be called only once.
     *
     * @param scheduler Scheduler to trigger an action.
     * @param timeoutMillis Timeout in milliseconds.
     */
    public void setTimeout(ScheduledExecutorService scheduler, long timeoutMillis) {
        scheduler.schedule(() -> state.complete(Reason.TIMEOUT), timeoutMillis, MILLISECONDS);
    }

    /**
     * Executes cancel closure.
     */
    public void cancel() {
        state.complete(Reason.CANCEL);
    }

    /** Returns {@code true} if the cancellation procedure has already been started. */
    public boolean isCancelled() {
        return state.isDone();
    }

    /** Throws {@link QueryCancelledException} If operation has been already cancelled.*/
    public void throwIfCancelled() throws QueryCancelledException {
        if (!state.isDone()) {
            return;
        }

        Reason reason = state.join();

        throw new QueryCancelledException(
                reason == Reason.TIMEOUT
                        ? QueryCancelledException.TIMEOUT_MSG
                        : QueryCancelledException.CANCEL_MSG
        );
    }

    enum Reason {
        CANCEL,
        TIMEOUT
    }
}
