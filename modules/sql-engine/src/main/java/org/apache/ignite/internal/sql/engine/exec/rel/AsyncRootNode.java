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

import static org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode.NOT_WAITING;
import static org.apache.ignite.internal.sql.engine.util.Commons.IN_BUFFER_SIZE;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.lang.Debuggable;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.lang.CursorClosedException;
import org.jetbrains.annotations.Nullable;

/**
 * An async iterator over the execution tree.
 */
public class AsyncRootNode<InRowT, OutRowT> implements Downstream<InRowT>, AsyncCursor<OutRowT> {
    public static final IgniteLogger LOGGER = Loggers.forClass(AsyncRootNode.class);
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

    // Metrics
    private long rowsReceived;
    private long queryStartTime = -1L;
    private long prefetchTime = -1L;
    private long queryTime = -1L;
    private final long fragmentId;
    private final UUID queryId;

    /**
     * Constructor.
     *
     * @param source A source to requests rows from.
     * @param converter A converter to convert rows from an internal format to desired output format.
     */
    public AsyncRootNode(ExecutionContext<InRowT> ctx, AbstractNode<InRowT> source, Function<InRowT, OutRowT> converter) {
        this.source = source;
        this.converter = converter;
        queryId = ctx.queryId();
        fragmentId = ctx.description().fragmentId();
    }

    /** {@inheritDoc} */
    @Override
    public void push(InRowT row) throws Exception {
        assert waiting > 0 : waiting;

        onRowReceived();

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

        waiting = NOT_WAITING;

        completePrefetchFuture(null);

        flush();

        if (ExecutionContext.DUMP_METRICS) {
            dumpQueryMetrics();
        }
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

        synchronized (lock) {
            Throwable t = ex.get();

            if (t != null) {
                next.completeExceptionally(t);

                return next;
            }

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

        onQueryStarted();

        if (waiting == 0) {
            try {
                source.checkState();

                //noinspection NestedAssignment
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
        PendingRequest<OutRowT> currentReq = pendingRequests.peek();

        // There may be no pending requests in two cases:
        //   1) flush has been triggered by prefetch
        //   2) concurrent cancellation already cleared the queue
        // In both cases we should just return immediately.
        if (currentReq == null) {
            return;
        }

        while (!buff.isEmpty() && currentReq.buff.size() < currentReq.requested) {
            currentReq.buff.add(buff.remove());
        }

        HasMore hasMore;
        if (waiting == NOT_WAITING && buff.isEmpty()) {
            hasMore = HasMore.NO;
        } else if (!buff.isEmpty()) {
            hasMore = HasMore.YES;
        } else {
            hasMore = HasMore.UNCERTAIN;
        }

        // Even if demand is fulfilled we should not complete request
        // if we are not sure whether there are more rows or not to
        // avoid returning false-positive result.
        if ((currentReq.buff.size() == currentReq.requested && hasMore != HasMore.UNCERTAIN) || hasMore == HasMore.NO) {
            // use poll() instead of remove() because latter throws exception when queue is empty,
            // and queue may be cleared concurrently by cancellation
            pendingRequests.poll();

            currentReq.fut.complete(new BatchedResult<>(currentReq.buff, hasMore == HasMore.YES));
        }

        if (buff.isEmpty()) {
            if (waiting == 0) {
                //noinspection NestedAssignment
                source.request(waiting = IN_BUFFER_SIZE);
            } else if (waiting == NOT_WAITING) {
                assert hasMore == HasMore.NO : hasMore;

                onQueryFinish();
                closeAsync();
            }
        } else if (!pendingRequests.isEmpty()) {
            scheduleTask();
        }
    }

    /**
     * Schedules a task to request another batch if there is at least one pending request and no active task exists.
     */
    private void scheduleTask() {
        if (!pendingRequests.isEmpty() && taskScheduled.compareAndSet(false, true)) {
            source.execute(() -> {
                onQueryStarted();

                taskScheduled.set(false);

                flush();
            });
        }
    }

    /**
     * Completes prefetch future if it has not already been completed.
     *
     * @param ex Exceptional completion cause or {@code null} if the future must complete successfully.
     */
    private void completePrefetchFuture(@Nullable Throwable ex) {
        if (!prefetchFut.isDone()) {
            onPrefetchFinished();

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

    private enum HasMore {
        YES, NO, UNCERTAIN
    }

    private void dumpQueryMetrics() {
        IgniteStringBuilder sb = new IgniteStringBuilder();
        sb.app("Dump metrics for executed query: queryId=").app(queryId).app(", fragmentId=").app(fragmentId).nl();
        sb.app("RootNode: rows=").app(rowsReceived)
                .app(", prefetch=").app(MetricsAwareNode.beautifyNanoTime(prefetchTime))
                .app(", totalTime=").app(MetricsAwareNode.beautifyNanoTime((queryTime)))
                .nl();

        MetricsAwareNode.dumpChildNodesMetrics(sb, Debuggable.childIndentation(""), List.of(source));

        LOGGER.info(sb.toString());
    }

    private void onRowReceived() {
        rowsReceived++;
    }

    private void onQueryStarted() {
        if (queryStartTime == -1) {
            queryStartTime = System.nanoTime();
        }
    }

    private void onPrefetchFinished() {
        if (prefetchTime == -1) {
            prefetchTime = System.nanoTime() - queryStartTime;
        }
    }

    private void onQueryFinish() {
        if (queryStartTime != -1) {
            onPrefetchFinished();

            queryTime += System.nanoTime() - queryStartTime;
            queryStartTime = -1;
        }
    }
}
