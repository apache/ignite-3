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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.util.Commons.IN_BUFFER_SIZE;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.lang.CursorClosedException;
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

    private final Queue<OutRowT> buff = new ArrayDeque<>(IN_BUFFER_SIZE);

    private final AtomicBoolean taskScheduled = new AtomicBoolean();

    private final Queue<PendingRequest<OutRowT>> pendingRequests = new ConcurrentLinkedQueue<>();

    private final CompletableFuture<Void> prefetchFut = new CompletableFuture<>();

    private volatile boolean closed = false;

    /**
     * Amount of rows which were requested from a source.
     *
     * <p>Note: this variable should be accessed from an execution task only.
     */
    private int waiting;

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
        assert waiting > 0 : waiting;

        buff.add(converter.apply(row));

        if (--waiting == 0) {
            completePrefetchFuture(null);

            flush();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0 : waiting;

        waiting = -1;

        completePrefetchFuture(null);

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
    public CompletableFuture<BatchedResult<OutRowT>> requestNextAsync(int rows) {
        CompletableFuture<BatchedResult<OutRowT>> next = new CompletableFuture<>();

        Throwable t = ex.get();

        if (t != null) {
            next.completeExceptionally(t);

            return next;
        }

        synchronized (lock) {
            if (closed) {
                next.completeExceptionally(new CursorClosedException());

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

                    if (!pendingRequests.isEmpty()) {
                        if (th == null) {
                            th = new QueryCancelledException();
                        }

                        Throwable th0 = th;

                        pendingRequests.forEach(req -> req.fut.completeExceptionally(th0));
                        pendingRequests.clear();
                    }

                    source.context().execute(() -> {
                        try {
                            source.close();

                            cancelFut.complete(null);
                        } catch (Throwable t) {
                            cancelFut.completeExceptionally(t);

                            throw t;
                        }
                    }, source::onError);

                    completePrefetchFuture(th);

                    closed = true;
                }
            }
        }

        return cancelFut;
    }

    /**
     * Starts the execution of the fragment and keeps the result in the intermediate buffer.
     *
     * <p>Note: this method must be called by the same thread that will execute the whole fragment.
     *
     * @return Future representing pending completion of the prefetch operation.
     */
    public CompletableFuture<Void> startPrefetch() {
        assert source.context().description().prefetch();

        if (waiting == 0) {
            try {
                source.request(waiting = IN_BUFFER_SIZE);
            } catch (Exception ex) {
                onError(ex);
            }
        }

        return prefetchFut;
    }

    public boolean isClosed() {
        return cancelFut.isDone();
    }

    private void flush() throws Exception {
        // flush may be triggered by prefetching, so let's do nothing in this case
        if (pendingRequests.isEmpty()) {
            return;
        }

        PendingRequest<OutRowT> currentReq = pendingRequests.peek();

        assert currentReq != null;

        taskScheduled.set(false);

        while (!buff.isEmpty() && currentReq.buff.size() < currentReq.requested) {
            currentReq.buff.add(buff.remove());
        }

        boolean hasMoreRows = waiting != -1 || !buff.isEmpty();

        if (currentReq.buff.size() == currentReq.requested || !hasMoreRows) {
            pendingRequests.remove();

            currentReq.fut.complete(new BatchedResult<>(currentReq.buff, hasMoreRows));
        }

        if (waiting == 0) {
            source.request(waiting = IN_BUFFER_SIZE);
        } else if (waiting == -1 && buff.isEmpty()) {
            closeAsync();
        }
    }

    /**
     * Schedules a task to request another batch if there is at least one pending request and no active task exists.
     */
    private void scheduleTask() {
        if (!pendingRequests.isEmpty() && taskScheduled.compareAndSet(false, true)) {
            source.context().execute(this::flush, source::onError);
        }
    }

    /**
     * Completes prefetch future if it has not already been completed.
     *
     * @param ex Exceptional completion cause or {@code null} if the future must complete successfully.
     */
    private void completePrefetchFuture(@Nullable Throwable ex) {
        if (!prefetchFut.isDone()) {
            if (ex != null) {
                prefetchFut.completeExceptionally(ex);
            } else {
                prefetchFut.complete(null);
            }
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

        private PendingRequest(int requested, CompletableFuture<BatchedResult<OutRowT>> fut) {
            this.requested = requested;
            this.fut = fut;
            this.buff = new ArrayList<>(requested);
        }
    }
}
