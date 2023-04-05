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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.cursor.CloseCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.NextBatchCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Subscription used to notify a subscriber on Meta Storage cursor data.
 *
 * @implNote Subscription is always processed sequentially in one of the provided executor's thread, so no explicit synchronization is used.
 *     It is expected that subscription methods (i.e. {@code request} and {@code cancel}) are called from the same thread that
 *     was used to invoke {@code onNext}.
 */
class CursorSubscription implements Subscription {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageService.class);

    /** Default batch size that is requested from the remote server. */
    private static final int BATCH_SIZE = 1000;

    private final MetaStorageServiceContext context;

    private final IgniteUuid cursorId;

    private final Subscriber<? super Entry> subscriber;

    /** Flag indicating that either the whole data range has been exhausted or the subscription has been cancelled. */
    private boolean isDone = false;

    /** Cached response from the Meta Storage leader. {@code null} if no requests have been issued yet. */
    @Nullable
    private BatchResponse cachedResponse;

    /** Index inside the cached response. */
    private int responseIndex;

    /** Amount of entries requested by the subscriber. */
    private long demand;

    CursorSubscription(MetaStorageServiceContext context, IgniteUuid cursorId, Subscriber<? super Entry> subscriber) {
        this.context = context;
        this.cursorId = cursorId;
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
                requestNextBatch();
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
                        requestNextBatch();
                    } else {
                        isDone = true;

                        subscriber.onComplete();
                    }

                    return;
                }
            }
        } catch (Exception e) {
            onError(e);
        } finally {
            context.busyLock().leaveBusy();
        }
    }

    private void requestNextBatch() {
        NextBatchCommand command = context.commandsFactory().nextBatchCommand()
                .cursorId(cursorId)
                .batchSize(BATCH_SIZE)
                .build();

        context.raftService().<BatchResponse>run(command)
                .whenCompleteAsync((resp, e) -> {
                    if (!context.busyLock().enterBusy()) {
                        onError(new NodeStoppingException());
                    }

                    try {
                        if (e == null) {
                            cachedResponse = resp;
                            responseIndex = 0;

                            processRequest();
                        } else {
                            onError(e);
                        }
                    } finally {
                        context.busyLock().leaveBusy();
                    }
                }, context.executorService());
    }

    @Override
    public void cancel() {
        isDone = true;

        CloseCursorCommand command = context.commandsFactory().closeCursorCommand().cursorId(cursorId).build();

        context.raftService().run(command).whenComplete((v, e) -> {
            if (e != null) {
                LOG.error("Unable to close cursor " + cursorId, e);
            }
        });
    }

    private void onError(Throwable e) {
        cancel();

        subscriber.onError(e);
    }
}
