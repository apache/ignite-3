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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/** Implementation of {@link CancelHandle}. */
final class CancelHandleImpl implements CancelHandle {

    private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

    private final CancellationTokenImpl token;

    CancelHandleImpl() {
        this.token = new CancellationTokenImpl(this);
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        doCancelAsync();

        cancelFut.join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> cancelAsync() {
        doCancelAsync();

        return cancelFut;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isCancelled() {
        return token.isCancelled();
    }

    /** {@inheritDoc} */
    @Override
    public CancellationToken token() {
        return token;
    }

    private void doCancelAsync() {
        token.cancel().whenComplete((r, t) -> {
            if (t != null) {
                cancelFut.completeExceptionally(t);
            } else {
                cancelFut.complete(null);
            }
        });
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
            return cancelFutRef.get() != null;
        }

        void addCancelAction(Runnable cancelAction, CompletableFuture<?> completionFut) {
            assert cancelAction != null : "cancelAction must be not null";
            assert completionFut != null : "completionFut must be not null";

            Cancellation cancellation = new Cancellation(cancelAction, completionFut);
            if (handle.isCancelled()) {
                cancellation.run();
            } else {
                cancellations.add(cancellation);
            }
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

        private final CompletableFuture<?> completionFut;

        private Cancellation(Runnable cancelAction, CompletableFuture<?> completionFut) {
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
