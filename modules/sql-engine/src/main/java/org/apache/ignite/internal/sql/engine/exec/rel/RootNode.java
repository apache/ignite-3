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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.ExceptionUtils;

/**
 * Client iterator.
 */
public class RootNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT>, Iterator<RowT> {
    private final ReentrantLock lock = new ReentrantLock();

    private final Condition cond = lock.newCondition();

    private final Runnable onClose;

    private final AtomicReference<Throwable> ex = new AtomicReference<>();

    private final Function<RowT, RowT> converter;

    private int waiting;

    private Deque<RowT> inBuff = new ArrayDeque<>(inBufSize);

    private Deque<RowT> outBuff = new ArrayDeque<>(inBufSize);

    private volatile boolean closed;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx Execution context.
     */
    public RootNode(ExecutionContext<RowT> ctx) {
        this(ctx, Function.identity());
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx Execution context.
     * @param converter Output rows converter.
     */
    public RootNode(ExecutionContext<RowT> ctx, Function<RowT, RowT> converter) {
        super(ctx);

        this.converter = converter;
        this.onClose = this::closeInternal;
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx Execution context.
     * @param converter Output rows converter.
     * @param onClose Runnable.
     */
    public RootNode(ExecutionContext<RowT> ctx, Function<RowT, RowT> converter, Runnable onClose) {
        super(ctx);

        this.converter = converter;
        this.onClose = onClose;
    }

    public UUID queryId() {
        return context().queryId();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (closed) {
            return;
        }

        lock.lock();
        try {
            if (waiting != -1 || !outBuff.isEmpty()) {
                ex.compareAndSet(null, new QueryCancelledException());
            }

            closed = true; // an exception has to be set first to get right check order

            cond.signalAll();
        } finally {
            lock.unlock();
        }

        onClose.run();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        context().execute(() -> sources().forEach(Commons::closeQuiet), this::onError);
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        lock.lock();
        try {
            assert waiting > 0;

            checkState();

            waiting--;

            inBuff.offer(row);

            if (inBuff.size() == inBufSize) {
                cond.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0;

        lock.lock();
        try {
            checkState();

            waiting = -1;

            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable e) {
        if (!ex.compareAndSet(null, e)) {
            ex.get().addSuppressed(e);
        }

        Commons.closeQuiet(this);
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        checkException();

        if (!outBuff.isEmpty()) {
            return true;
        }

        if (closed && ex.get() == null) {
            return false;
        }

        exchangeBuffers();

        return !outBuff.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public RowT next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return converter.apply(outBuff.remove());
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) {
        throw new UnsupportedOperationException();
    }

    private void exchangeBuffers() {
        assert !nullOrEmpty(sources()) && sources().size() == 1;

        lock.lock();
        try {
            while (ex.get() == null) {
                assert outBuff.isEmpty();

                if (inBuff.size() == inBufSize || waiting == -1) {
                    Deque<RowT> tmp = inBuff;
                    inBuff = outBuff;
                    outBuff = tmp;
                }

                if (waiting == -1 && outBuff.isEmpty()) {
                    close();
                } else if (inBuff.isEmpty() && waiting == 0) {
                    int req = waiting = inBufSize;
                    context().execute(() -> source().request(req), this::onError);
                }

                if (!outBuff.isEmpty() || waiting == -1) {
                    break;
                }

                cond.await();
            }
        } catch (InterruptedException e) {
            throw new QueryCancelledException(e);
        } finally {
            lock.unlock();
        }

        checkException();
    }

    private void checkException() {
        Throwable e = ex.get();

        if (e == null) {
            return;
        }

        ExceptionUtils.sneakyThrow(e);
    }
}
