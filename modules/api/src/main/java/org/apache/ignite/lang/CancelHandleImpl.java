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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
        return token.cancelled;
    }

    /** {@inheritDoc} */
    @Override
    public CancellationToken token() {
        return token;
    }

    private void doCancelAsync() {
        token.cancel();
    }

    static final class CancellationTokenImpl implements CancellationToken {

        private final ArrayDeque<Cancellation> cancellations = new ArrayDeque<>();

        private final CancelHandleImpl handle;

        private volatile CompletableFuture<Void> fut;

        private volatile boolean cancelled;

        CancellationTokenImpl(CancelHandleImpl handle) {
            this.handle = handle;
        }

        void addCancelAction(Runnable action, CompletableFuture<?> fut) {
            Cancellation cancellation = new Cancellation(action, fut);

            synchronized (this) {
                if (!cancelled) {
                    cancellations.add(cancellation);
                    return;
                }
            }

            cancellation.run();
        }

        @SuppressWarnings("rawtypes")
        CompletableFuture<Void> cancel() {
            ensureNotCancelled();

            List<Cancellation> cancellationList;

            synchronized (this) {
                if (cancelled) {
                    return fut;
                }

                cancelled = true;

                CompletableFuture[] futures = cancellations.stream()
                        .map(c -> c.completionFut)
                        .toArray(CompletableFuture[]::new);

                fut = CompletableFuture.allOf(futures);
                cancellationList = new ArrayList<>(cancellations);

                fut.whenComplete((r,t) -> {
                    handle.cancelFut.complete(null);
                });
            }

            cancellationList.forEach(Cancellation::run);

            return fut;
        }

        private void ensureNotCancelled() {
            if (cancelled) {
                throw new IllegalStateException("Not reenterable yet");
            }
        }

        public CompletableFuture<Void> cancelHandleFut() {
            return handle.cancelFut;
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
            cancelAction.run();
        }
    }
}
