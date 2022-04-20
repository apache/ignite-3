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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.ClosedCursorException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Client iterator.
 */
public class AsyncRootNode<InRowT, OutRowT> implements Downstream<InRowT>, AsyncCursor<OutRowT> {
    private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

    private final Object lock = new Object();

    /** The tail of the request chain. Guarded by {@link #lock}. */
    private CompletableFuture<BatchedResult<OutRowT>> requestChainTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<BatchedResult<OutRowT>> currentRequest = null;

    private volatile boolean cancelled = false;

    private boolean firstRequest = true;

    private final AtomicReference<Throwable> ex = new AtomicReference<>();

    private final Runnable onClose;

    private final AbstractNode<InRowT> source;

    private final Function<InRowT, OutRowT> converter;

    private int waiting;

    private List<OutRowT> buff;

    private InRowT lastRow;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param rowType Rel data type.
     * @param onClose Runnable.
     */
    public AsyncRootNode(AbstractNode<InRowT> source, Function<InRowT, OutRowT> converter, Runnable onClose) {
        this.onClose = onClose;
        this.source = source;
        this.converter = converter;
    }

    /** {@inheritDoc} */
    @Override
    public void push(InRowT row) throws Exception {
        assert waiting > 0;

        waiting--;

        buff.add(converter.apply(row));

        if (waiting == 0) {
            CompletableFuture<BatchedResult<OutRowT>> current = currentRequest;
            List<OutRowT> currentBatch = buff;

            buff = null;
            currentRequest = null;

            current.complete(new BatchedResult<>(currentBatch, true));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0;

        waiting = -1;

        CompletableFuture<BatchedResult<OutRowT>> current = currentRequest;
        List<OutRowT> currentBatch = buff;

        buff = null;
        currentRequest = null;

        current.complete(new BatchedResult<>(currentBatch, false));
    }

    @Override
    public void onError(Throwable e) {
        if (!ex.compareAndSet(null, e)) {
            ex.get().addSuppressed(e);
        }

        if (currentRequest != null) {
            currentRequest.completeExceptionally(e);
        }

        closeAsync();
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<BatchedResult<OutRowT>> requestNextAsync(int rows) {
        CompletableFuture<BatchedResult<OutRowT>> next = new CompletableFuture<>();
        CompletableFuture<BatchedResult<OutRowT>> prev;

        synchronized (lock) {
            if (cancelled) {
                next.completeExceptionally(new ClosedCursorException());

                return next;
            }

            prev = requestChainTail;
            requestChainTail = next;
        }

        prev.thenRun(() -> {
            assert buff == null;

            buff = new ArrayList<>(waiting = rows);
            currentRequest = next;

            source.context().execute(() -> source.request(rows), source::onError);
        }).exceptionally(t -> {
            next.completeExceptionally(t);

            return null;
        });

        return next;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!cancelled) {
            synchronized (lock) {
                if (!cancelled) {
//                    requestChainTail = CompletableFuture.failedFuture(new ClosedCursorException());

                    source.context().execute(() -> {
                        try {
                            onClose.run();

                            cancelFut.complete(null);
                        } catch (Throwable t) {
                            cancelFut.completeExceptionally(t);

                            throw t;
                        }
                    }, source::onError);

                    cancelled = true;
                }
            }
        }

        return cancelFut.thenApply(Function.identity());
    }

    private void checkException() {
        Throwable e = ex.get();

        if (e == null) {
            return;
        }

        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new IgniteInternalException("An error occurred while query executing.", e);
        }
        // TODO: rework with SQL error code
        //        if (e instanceof IgniteSQLException)
        //            throw (IgniteSQLException)e;
        //        else
        //            throw new IgniteSQLException("An error occurred while query executing.", IgniteQueryErrorCode.UNKNOWN, e);
    }
}
