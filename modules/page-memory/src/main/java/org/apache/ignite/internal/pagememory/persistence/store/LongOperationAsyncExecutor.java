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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.util.worker.IgniteWorker;

/**
 * Synchronization wrapper for long operations that should be executed asynchronously and operations that can not be executed in parallel
 * with long operation.
 *
 * <p>Uses {@link ReadWriteLock} to provide such synchronization scenario.
 */
public class LongOperationAsyncExecutor {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final String igniteInstanceName;

    private final IgniteLogger log;

    private final Set<IgniteWorker> workers = ConcurrentHashMap.newKeySet();

    private static final AtomicLong WORKER_COUNTER = new AtomicLong(0);

    /**
     * Constructor.
     *
     * @param igniteInstanceName Name of the Ignite instance this runnable is used in.
     * @param log Logger.
     */
    public LongOperationAsyncExecutor(String igniteInstanceName, IgniteLogger log) {
        this.igniteInstanceName = igniteInstanceName;

        this.log = log;
    }

    /**
     * Executes long operation in dedicated thread.
     *
     * <p>Uses write lock as such operations can't run simultaneously.
     *
     * @param operation Long operation.
     * @param name name of the operation, used as part of the thread name.
     */
    public CompletableFuture<Void> async(RunnableX operation, String name) {
        String workerName = "async-" + name + "-task-" + WORKER_COUNTER.getAndIncrement();

        CompletableFuture<Void> future = new CompletableFuture<>();

        IgniteWorker worker = new IgniteWorker(log, igniteInstanceName, workerName, null) {
            @Override
            protected void body() {
                readWriteLock.writeLock().lock();

                try {
                    operation.run();

                    future.complete(null);
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                } finally {
                    readWriteLock.writeLock().unlock();

                    workers.remove(this);
                }
            }
        };

        workers.add(worker);

        // TODO: IGNITE-18269 replace with thread pool
        new IgniteThread(worker).start();

        return future;
    }

    /**
     * Executes long operation in dedicated thread.
     *
     * <p>Uses write lock as such operations can't run simultaneously.
     *
     * @param operation Long operation.
     * @param name name of the operation, used as part of the thread name.
     */
    public CompletableFuture<Void> async(Runnable operation, String name) {
        return async((RunnableX) operation::run, name);
    }

    /**
     * Executes supplier that can't run in parallel with long operation that is executed by {@link LongOperationAsyncExecutor#async}.
     *
     * <p>Uses read lock as such closures can run in parallel with each other.
     *
     * @param supplier Supplier.
     * @param <T> Return type.
     * @return Value that is returned by {@code supplier}.
     */
    public <T> T afterAsyncCompletion(Supplier<T> supplier) {
        readWriteLock.readLock().lock();

        try {
            return supplier.get();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Cancels async tasks.
     */
    public void awaitAsyncTaskCompletion(boolean cancel) {
        awaitForWorkersStop(workers, cancel, log);
    }
}
