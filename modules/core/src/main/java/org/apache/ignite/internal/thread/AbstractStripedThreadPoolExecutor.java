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

package org.apache.ignite.internal.thread;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.tostring.S;

/**
 * An abstract executor that executes submitted tasks using pooled grid threads.
 */
public abstract class AbstractStripedThreadPoolExecutor<E extends ExecutorService> implements ExecutorService {
    /** Executors. */
    private final E[] execs;

    /** Used to obtain a random executor. */
    private final Random random = new Random();

    /**
     * The constructor.
     *
     * @param execs Executors.
     */
    public AbstractStripedThreadPoolExecutor(E[] execs) {
        this.execs = execs;
    }

    /**
     * Executes the given command at some time in the future. The command with the same {@code index} will be executed in the same thread.
     *
     * @param task The runnable task.
     * @param idx Striped index.
     * @throws RejectedExecutionException If this task cannot be accepted for execution.
     * @throws NullPointerException If command is null.
     */
    public void execute(Runnable task, int idx) {
        stripeExecutor(idx).execute(task);
    }

    /** {@inheritDoc} */
    @Override
    public void execute(Runnable task) {
        stripeExecutor(random.nextInt(execs.length)).execute(task);
    }

    /**
     * Submits a {@link Runnable} task for execution and returns a {@link CompletableFuture} representing that task. The command with the
     * same {@code index} will be executed in the same thread.
     *
     * @param task The task to submit.
     * @param idx Striped index.
     * @return A {@link Future} representing pending completion of the task.
     * @throws RejectedExecutionException If the task cannot be scheduled for execution.
     * @throws NullPointerException If the task is {@code null}.
     */
    public CompletableFuture<?> submit(Runnable task, int idx) {
        return CompletableFuture.runAsync(task, stripeExecutor(idx));
    }

    /** {@inheritDoc} */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T> Future<T> submit(Runnable task, T res) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
        for (E exec : execs) {
            exec.shutdown();
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<Runnable> shutdownNow() {
        if (execs.length == 0) {
            return Collections.emptyList();
        }

        List<Runnable> res = new ArrayList<>(execs.length);

        for (E exec : execs) {
            for (Runnable r : exec.shutdownNow()) {
                res.add(r);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isShutdown() {
        for (E exec : execs) {
            if (!exec.isShutdown()) {
                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isTerminated() {
        for (E exec : execs) {
            if (!exec.isTerminated()) {
                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean res = true;

        for (E exec : execs) {
            res &= exec.awaitTermination(timeout, unit);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(AbstractStripedThreadPoolExecutor.class, this);
    }

    /**
     * Return an executor by an index.
     *
     * @param idx Index of executor.
     * @return Executor.
     */
    public E stripeExecutor(int idx) {
        return execs[threadId(idx)];
    }

    /**
     * Sets stripped thread ID.
     *
     * @param idx Index.
     * @return Stripped thread ID.
     */
    private int threadId(int idx) {
        assert idx >= 0 : "Index is negative: " + idx;

        return idx < execs.length ? idx : idx % execs.length;
    }
}
