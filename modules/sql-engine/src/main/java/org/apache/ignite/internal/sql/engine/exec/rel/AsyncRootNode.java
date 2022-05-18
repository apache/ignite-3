/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.exec.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.ClosedCursorException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionCancelledException;
import org.jetbrains.annotations.Nullable;

/**
 * An async iterator over the execution tree.
 */
public class AsyncRootNode<InRowT, OutRowT> implements Downstream<InRowT>, AsyncCursor<OutRowT> {
    private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

    /**
     * This lock used to avoid races between requesting another batch (adding to the {@link #pendingRequests queue}) and closing the node
     * (changing the flag and clearing all pending requests).
     */
    private final Object lock = new Object();

    private final AtomicReference<Throwable> ex = new AtomicReference<>();

    private final AbstractNode<InRowT> source;

    private final Function<InRowT, OutRowT> converter;

    private final AtomicBoolean taskScheduled = new AtomicBoolean();

    private final Queue<PendingRequest<OutRowT>> pendingRequests = new LinkedBlockingQueue<>();

    private volatile boolean closed = false;

    /**
     * Amount of rows which were requested from a source.
     *
     * <p>Note: this variable should be accessed from an execution task only.
     */
    private int waiting;

    /**
     * Last row that pushed to this downstream.
     *
     * <p>{@link org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult} requires information about whether there are more
     * rows available or not. To meet this requirement, we request an extra row from source at the very first requested batch. Thus,
     * we have an extra row which should be the very first row of the next batch.
     *
     * <p>Note: this variable should be accessed from an execution task only.
     */
    private @Nullable OutRowT lastRow;

    /**
     * Whether the very first batch was already requested or not. See {@link #lastRow} for details.
     *
     * <p>Note: this variable should be accessed from an execution task only.
     *
     * @see #lastRow
     */
    private boolean firstRequest = true;

    /**
     * Constructor.
     *
     * @param source A source to requests rows from.
     * @param converter A converter to convert rows from an internal format to desired output format.
     */
    public AsyncRootNode(AbstractNode<InRowT> source, Function<InRowT, OutRowT> converter) {
        this.source = source;
        this.converter = converter;
    }

    /** {@inheritDoc} */
    @Override
    public void push(InRowT row) throws Exception {
        assert waiting > 0;

        waiting--;

        var currentReq = pendingRequests.peek();

        assert currentReq != null;

        if (currentReq.buff.size() < currentReq.requested) {
            currentReq.buff.add(converter.apply(row));
        } else {
            assert waiting == 0;

            lastRow = converter.apply(row);

            flush();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0;

        waiting = -1;

        var currentReq = pendingRequests.peek();

        assert currentReq != null;

        if (currentReq.buff.size() < currentReq.requested && lastRow != null) {
            currentReq.buff.add(lastRow);

            lastRow = null;
        }

        flush();
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable e) {
        if (closed) {
            return;
        }

        if (!ex.compareAndSet(null, e)) {
            ex.get().addSuppressed(e);
        }

        closeAsync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<BatchedResult<OutRowT>> requestNextAsync(int rows) {
        CompletableFuture<BatchedResult<OutRowT>> next = new CompletableFuture<>();

        Throwable t = ex.get();

        if (t != null) {
            next.completeExceptionally(t);

            return next;
        }

        synchronized (lock) {
            if (closed) {
                next.completeExceptionally(new ClosedCursorException());

                return next;
            }

            pendingRequests.add(new PendingRequest<>(rows, next));

            scheduleTask();
        }

        return next;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed) {
            synchronized (lock) {
                if (!closed) {
                    Throwable th = ex.get();

                    if (th == null) {
                        th = new ExecutionCancelledException();
                    }

                    Throwable th0 = th;

                    pendingRequests.forEach(req -> req.fut.completeExceptionally(th0));
                    pendingRequests.clear();

                    source.context().execute(() -> {
                        try {
                            source.close();
                            source.context().cancel();

                            cancelFut.complete(null);
                        } catch (Throwable t) {
                            cancelFut.completeExceptionally(t);

                            throw t;
                        }
                    }, source::onError);

                    closed = true;
                }
            }
        }

        return cancelFut.thenApply(Function.identity());
    }

    private void flush() {
        var currentReq = pendingRequests.remove();

        assert currentReq != null;

        taskScheduled.set(false);

        currentReq.fut.complete(new BatchedResult<>(currentReq.buff, waiting != -1 || lastRow != null));

        boolean hasMoreRow = waiting != -1 || lastRow != null;

        if (hasMoreRow) {
            scheduleTask();
        } else {
            closeAsync();
        }
    }

    /**
     * Schedules a task to request another batch if there is at least one pending request and no active task exists.
     */
    private void scheduleTask() {
        if (!pendingRequests.isEmpty() && taskScheduled.compareAndSet(false, true)) {
            var nextTask = pendingRequests.peek();

            assert nextTask != null;

            source.context().execute(() -> {
                // for the very first request we need to request one extra row
                // to be able to determine whether there is more rows or not
                if (firstRequest) {
                    waiting = nextTask.requested + 1;
                    firstRequest = false;
                } else {
                    waiting = nextTask.requested;

                    assert lastRow != null;

                    nextTask.buff.add(lastRow);
                    lastRow = null;
                }

                source.request(waiting);
            }, source::onError);
        }
    }

    private static class PendingRequest<OutRowT> {
        /**
         * A future to complete when {@link #buff buffer} will be filled.
         */
        private final CompletableFuture<BatchedResult<OutRowT>> fut;

        /**
         * A count of requested rows.
         */
        private final int requested;

        /**
         * A buffer to keep rows before the head of a {@link #pendingRequests} will be completed.
         */
        private final List<OutRowT> buff;

        public PendingRequest(int requested, CompletableFuture<BatchedResult<OutRowT>> fut) {
            this.requested = requested;
            this.fut = fut;
            this.buff = new ArrayList<>(requested);
        }
    }
}
