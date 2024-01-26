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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Implementation of query task executor for any SQL related execution stage.
 */
public class QueryTaskExecutorImpl implements QueryTaskExecutor, Thread.UncaughtExceptionHandler {
    private static final IgniteLogger LOG = Loggers.forClass(QueryTaskExecutorImpl.class);

    private static final UUID QUERY_ID_STUB = UUID.randomUUID();

    private final String nodeName;

    private volatile StripedThreadPoolExecutor stripedThreadPoolExecutor;

    private final int concurrencyLevel;

    private Thread.UncaughtExceptionHandler exHnd;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param concurrencyLevel concurrency Level for execution thread pool.
     */
    public QueryTaskExecutorImpl(String nodeName, int concurrencyLevel) {
        this.nodeName = nodeName;
        this.concurrencyLevel = concurrencyLevel;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        this.stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                concurrencyLevel,
                NamedThreadFactory.threadPrefix(nodeName, "sql-execution-pool"),
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );
    }

    /**
     * Set uncaught exception handler.
     *
     * @param exHnd Uncaught exception handler.
     */
    public void exceptionHandler(Thread.UncaughtExceptionHandler exHnd) {
        this.exHnd = exHnd;
    }

    /** {@inheritDoc} */
    @Override
    public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
        int commandIdx = hash(qryId, fragmentId);
        stripedThreadPoolExecutor.execute(
                () -> {
                    try {
                        qryTask.run();
                    } catch (Throwable e) {
                        LOG.debug("Uncaught exception", e);

                        /*
                         * No exceptions are rethrown here to preserve the current thread from being destroyed,
                         * because other queries may be pinned to the current thread id.
                         * However, unrecoverable errors must be processed by FailureHandler.
                         */
                        uncaughtException(Thread.currentThread(), e);
                    }
                },
                commandIdx
        );
    }

    /** {@inheritDoc} */
    @Override
    public void execute(Runnable command) {
        execute(
                QUERY_ID_STUB,
                ThreadLocalRandom.current().nextLong(1024),
                command
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> submit(UUID qryId, long fragmentId, Runnable qryTask) {
        return stripedThreadPoolExecutor.submit(qryTask, hash(qryId, fragmentId));
    }

    /** {@inheritDoc} */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (exHnd != null) {
            exHnd.uncaughtException(t, e);
        }
    }

    private static int hash(UUID qryId, long fragmentId) {
        // inlined Objects.hash(...)
        return IgniteUtils.safeAbs(31 * (31 + (qryId != null ? qryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (stripedThreadPoolExecutor != null) {
            stripedThreadPoolExecutor.shutdownNow();
        }
    }
}
