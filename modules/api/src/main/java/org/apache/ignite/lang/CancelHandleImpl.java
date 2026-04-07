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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.ErrorGroups.Common;

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

        // Make a copy of internal future, so that it is not possible to complete it
        return cancelFut.copy();
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
        token.cancel();
    }

    static final class CancellationTokenImpl implements CancellationToken {

        private final ArrayDeque<Cancellation> cancellations = new ArrayDeque<>();

        private final List<Runnable> listeners = new ArrayList<>();

        private final CancelHandleImpl handle;

        private final Object mux = new Object();

        private volatile CompletableFuture<Void> cancelFut;

        CancellationTokenImpl(CancelHandleImpl handle) {
            this.handle = handle;
        }

        void addCancelAction(Runnable action, CompletableFuture<?> fut) {
            Cancellation cancellation = new Cancellation(action, fut);

            if (cancelFut != null) {
                cancellation.run();
            } else {
                synchronized (mux) {
                    if (cancelFut == null) {
                        cancellations.add(cancellation);
                        return;
                    }
                }

                cancellation.run();
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelFut != null;
        }

        @Override
        public AutoCloseable addListener(Runnable callback) {
            Objects.requireNonNull(callback, "callback");

            if (cancelFut != null) {
                runListener(callback);
                return () -> { };
            }

            synchronized (mux) {
                if (cancelFut == null) {
                    listeners.add(callback);
                    return () -> {
                        synchronized (mux) {
                            listeners.remove(callback);
                        }
                    };
                }
            }

            runListener(callback);
            return () -> { };
        }

        private static void runListener(Runnable callback) {
            try {
                callback.run();
            } catch (Throwable t) {
                throw new IgniteException(Common.INTERNAL_ERR, "Failed to cancel an operation", t);
            }
        }

        @SuppressWarnings("rawtypes")
        void cancel() {
            List<Runnable> listenersCopy;

            if (cancelFut != null) {
                return;
            }

            synchronized (mux) {
                if (cancelFut != null) {
                    return;
                }

                // First assemble all completion futures
                CompletableFuture[] futures = cancellations.stream()
                        .map(c -> c.completionFut)
                        .toArray(CompletableFuture[]::new);

                // handle.cancelFut completes when all cancellation futures complete.
                cancelFut = CompletableFuture.allOf(futures).whenComplete((r, t) -> {
                    handle.cancelFut.complete(null);
                });

                listenersCopy = new ArrayList<>(listeners);
                listeners.clear();
            }

            IgniteException error = null;

            // Run cancellation actions outside of lock
            for (Cancellation cancellation : cancellations) {
                try {
                    cancellation.run();
                } catch (Throwable t) {
                    if (error == null) {
                        error = new IgniteException(Common.INTERNAL_ERR, "Failed to cancel an operation");
                    }
                    error.addSuppressed(t);
                }
            }

            // Release references to cancellation actions after execution
            cancellations.clear();

            // Run listener callbacks outside of lock
            for (Runnable listener : listenersCopy) {
                try {
                    listener.run();
                } catch (Throwable t) {
                    if (error == null) {
                        error = new IgniteException(Common.INTERNAL_ERR, "Failed to cancel an operation");
                    }
                    error.addSuppressed(t);
                }
            }

            if (error != null) {
                throw error;
            }
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
