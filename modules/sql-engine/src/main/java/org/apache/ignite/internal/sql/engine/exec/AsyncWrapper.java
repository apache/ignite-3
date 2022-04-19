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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.ClosedCursorException;

/**
 * Wrapper that converts a synchronous iterator to an asynchronous one.
 *
 * @param <T> Type of the item.
 */
public class AsyncWrapper<T> implements AsyncCursor<T> {
    /**
     * Future returning iterator that should be converted to async.
     */
    private final CompletableFuture<Iterator<T>> cursorFut;

    private final CompletableFuture<Void> cancelFut = new CompletableFuture<>();

    private final Executor exec;

    private final Object lock = new Object();

    /** The tail of the request chain. Guarded by {@link #lock}. */
    private CompletableFuture<BatchedResult<T>> requestChainTail = CompletableFuture.completedFuture(null);

    private volatile boolean cancelled = false;

    private volatile boolean firstRequest = true;

    /**
     * Constructor.
     *
     * <p>The execution will be in the thread invoking particular method of this cursor.
     *
     * @param source An iterator to wrap.
     */
    public AsyncWrapper(Iterator<T> source) {
        this(CompletableFuture.completedFuture(source), Runnable::run);
    }

    /**
     * Constructor.
     *
     * @param initFut Initialization future.
     * @param exec An executor to delegate execution.
     */
    public AsyncWrapper(CompletableFuture<Iterator<T>> initFut, Executor exec) {
        this.cursorFut = initFut;
        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override
    public CompletionStage<BatchedResult<T>> requestNextAsync(int rows) {
        CompletableFuture<BatchedResult<T>> next = new CompletableFuture<>();
        CompletableFuture<BatchedResult<T>> prev;

        synchronized (lock) {
            if (cancelled) {
                next.completeExceptionally(new ClosedCursorException());

                return next;
            }

            prev = requestChainTail;
            requestChainTail = next;
        }

        prev.thenCompose(tmp -> cursorFut).thenAcceptAsync(cursor -> {
            int remains = rows;

            if (!cursor.hasNext() && !firstRequest) {
                next.completeExceptionally(new NoSuchElementException());

                return;
            }

            List<T> batch = new ArrayList<>(rows);

            firstRequest = false;

            while (remains-- > 0 && cursor.hasNext()) {
                batch.add(cursor.next());
            }

            next.complete(new BatchedResult<>(batch, cursor.hasNext()));
        }, exec).exceptionally(t -> {
            next.completeExceptionally(t);

            return null;
        });

        return next;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!cancelled) {
            synchronized (lock) {
                if (!cancelled) {
                    requestChainTail.completeExceptionally(new ClosedCursorException());

                    cursorFut.thenAcceptAsync(cursor -> {
                        if (cursor instanceof AutoCloseable) {
                            try {
                                ((AutoCloseable) cursor).close();

                                cancelFut.complete(null);
                            } catch (Exception e) {
                                cancelFut.completeExceptionally(e);
                            }
                        } else {
                            cancelFut.complete(null);
                        }
                    }, exec);

                    cancelled = true;
                }
            }
        }

        return cancelFut.thenApply(Function.identity());
    }
}
