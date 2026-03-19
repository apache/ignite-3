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

package org.apache.ignite.internal.metastorage.impl;

import java.util.List;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.raft.ReadCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Subscription used to notify a subscriber on Meta Storage cursor data.
 *
 * @implNote Subscription is always processed sequentially in one of the provided executor's thread, so no explicit synchronization is used.
 *     It is expected that subscription methods (i.e. {@code request} and {@code cancel}) are called from the same thread that
 *     was used to invoke {@code onNext}.
 */
class CursorSubscription implements Subscription {
    private final MetaStorageServiceContext context;

    private final Subscriber<? super Entry> subscriber;

    private final Function<byte[], ReadCommand> nextBatchCommandSupplier;

    /** Flag indicating that either the whole data range has been exhausted or the subscription has been cancelled. */
    private boolean isDone = false;

    /** Cached response from the Meta Storage leader. {@code null} if no requests have been issued yet. */
    @Nullable
    private BatchResponse cachedResponse;

    /** Index inside the cached response. */
    private int responseIndex;

    /** Amount of entries requested by the subscriber. */
    private long demand;

    CursorSubscription(
            MetaStorageServiceContext context,
            Function<byte[], ReadCommand> nextBatchCommandSupplier,
            Subscriber<? super Entry> subscriber
    ) {
        this.context = context;
        this.nextBatchCommandSupplier = nextBatchCommandSupplier;
        this.subscriber = subscriber;
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            onError(new IllegalArgumentException("Requested amount must be greater than zero, got: " + n));

            return;
        }

        if (isDone) {
            return;
        }

        if (!context.busyLock().enterBusy()) {
            onError(new NodeStoppingException());

            return;
        }

        try {
            // Increase the number of requested items.
            demand += n;

            if (demand <= 0) {
                onError(new IllegalArgumentException("Long overflow"));

                return;
            }

            // Start the processing if it has not been started yet (async operation).
            if (cachedResponse == null) {
                requestNextBatch(null);
            }
        } finally {
            context.busyLock().leaveBusy();
        }
    }

    /**
     * Does the actual request processing. Must always be called on {@link #context} executor.
     */
    private void processRequest() {
        if (isDone) {
            return;
        }

        if (!context.busyLock().enterBusy()) {
            onError(new NodeStoppingException());
        }

        try {
            assert cachedResponse != null;

            List<Entry> entries = cachedResponse.entries();

            while (demand > 0 && !isDone) {
                if (responseIndex < entries.size()) {
                    subscriber.onNext(entries.get(responseIndex));

                    responseIndex++;
                    demand--;
                } else {
                    if (cachedResponse.hasNextBatch()) {
                        assert !entries.isEmpty();

                        byte[] lastProcessedKey = entries.get(entries.size() - 1).key();

                        requestNextBatch(lastProcessedKey);
                    } else {
                        isDone = true;

                        subscriber.onComplete();
                    }

                    return;
                }
            }
        } catch (Throwable e) {
            onError(e);
        } finally {
            context.busyLock().leaveBusy();
        }
    }

    private void requestNextBatch(byte @Nullable [] lastProcessedKey) {
        ReadCommand nextBatchCommand = nextBatchCommandSupplier.apply(lastProcessedKey);

        context.raftService().<BatchResponse>run(nextBatchCommand)
                .whenCompleteAsync((resp, e) -> {
                    if (e == null) {
                        cachedResponse = resp;
                        responseIndex = 0;

                        processRequest();
                    } else {
                        onError(e);
                    }
                }, context.executorService());
    }

    @Override
    public void cancel() {
        isDone = true;
    }

    private void onError(Throwable e) {
        cancel();

        subscriber.onError(e);
    }
}
