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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Implementation of {@link CancelHandle}. */
public final class CancelHandleImpl implements CancelHandle {

    private final ConcurrentLinkedQueue<CancellationTokenImpl> tokens = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean cancelled = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) {
            return;
        }
        doCancelSync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> cancelAsync() {
        if (!cancelled.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return doCancelAsync();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    /** {@inheritDoc} */
    @Override
    public CancellationToken token() {
        CancellationTokenImpl token = new CancellationTokenImpl(this);
        tokens.add(token);
        return token;
    }

    private void doCancelSync() {
        for (CancellationTokenImpl token : tokens) {
            try {
                token.cancel().join();
            } catch (Throwable ignore) {
                // ignore
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private CompletableFuture<Void> doCancelAsync() {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (CancellationTokenImpl token : tokens) {
            tasks.add(token.cancel());
        }
        CompletableFuture[] futures = tasks.toArray(new CompletableFuture[]{});
        return CompletableFuture.allOf(futures);
    }

    static final class CancellationTokenImpl implements CancellationToken {

        private final CancelHandleImpl handle;

        // Actions that trigger cancellations - action that triggers a cancellation + a future that completes, when a resource is closed.
        private final ConcurrentLinkedQueue<Cancellation> cancellations = new ConcurrentLinkedQueue<>();

        private final AtomicReference<CompletableFuture<Void>> cancelFutRef = new AtomicReference<>();

        CancellationTokenImpl(CancelHandleImpl handle) {
            this.handle = handle;
        }

        boolean isCancelled() {
            return handle.isCancelled();
        }

        void addCancelAction(Runnable cancelAction, CompletableFuture<Void> completionFut) {
            assert cancelAction != null : "cancelAction must be not null";
            assert completionFut != null : "completionFut must be not null";

            cancellations.add(new Cancellation(cancelAction, completionFut));
        }

        @SuppressWarnings("rawtypes")
        private CompletableFuture<Void> cancel() {
            CompletableFuture<Void> f = cancelFutRef.get();
            if (f != null) {
                return f;
            }

            // First assemble all completion futures
            CompletableFuture[] futures = cancellations.stream()
                    .map(c -> c.completionFut)
                    .toArray(CompletableFuture[]::new);

            CompletableFuture<Void> cancelAll = CompletableFuture.allOf(futures);
            CompletableFuture<Void> fut = cancelFutRef.compareAndExchange(null, cancelAll);

            // If cancel future has not been set, trigger cancellation actions.
            if (fut == null) {
                for (Cancellation cancellation : cancellations) {
                    cancellation.run();
                }
            }

            return fut != null ? fut : cancelAll;
        }
    }

    /**
     * Stores an action that triggers a cancellation and a completable future that completes when a resource is closed.
     */
    private static class Cancellation {

        private final Runnable cancelAction;

        private final CompletableFuture<Void> completionFut;

        private Cancellation(Runnable cancelAction, CompletableFuture<Void> completionFut) {
            this.cancelAction = cancelAction;
            this.completionFut = completionFut;
        }

        private void run() {
            try {
                cancelAction.run();
            } catch (Throwable ignore) {
                // Ignore
            }
        }
    }
}
