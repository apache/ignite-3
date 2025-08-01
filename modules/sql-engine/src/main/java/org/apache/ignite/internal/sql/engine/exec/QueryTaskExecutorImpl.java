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

import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.THREAD_POOLS_METRICS_SOURCE_NAME;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.StripedThreadPoolMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of query task executor for any SQL related execution stage.
 */
public class QueryTaskExecutorImpl implements QueryTaskExecutor {
    private static final IgniteLogger LOG = Loggers.forClass(QueryTaskExecutorImpl.class);

    private static final UUID QUERY_ID_STUB = UUID.randomUUID();

    private static final String QUERY_EXECUTOR_SOURCE_NAME = THREAD_POOLS_METRICS_SOURCE_NAME + "sql-executor";

    private final String nodeName;

    private volatile StripedThreadPoolExecutor stripedThreadPoolExecutor;

    private final int concurrencyLevel;

    private final FailureManager failureManager;

    private final MetricManager metricManager;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param concurrencyLevel concurrency Level for execution thread pool.
     * @param failureManager Failure processor.
     * @param metricManager Metric manager.
     */
    public QueryTaskExecutorImpl(
            String nodeName,
            int concurrencyLevel,
            FailureManager failureManager,
            MetricManager metricManager
    ) {
        this.nodeName = nodeName;
        this.concurrencyLevel = concurrencyLevel;
        this.failureManager = failureManager;
        this.metricManager = metricManager;
    }

    @Override
    public void start() {
        this.stripedThreadPoolExecutor = new StripedThreadPoolExecutor(
                concurrencyLevel,
                IgniteThreadFactory.create(nodeName, "sql-execution-pool", LOG, NOTHING_ALLOWED),
                false,
                0
        );

        metricManager.registerSource(new StripedThreadPoolMetricSource(QUERY_EXECUTOR_SOURCE_NAME, null, stripedThreadPoolExecutor));
        metricManager.enable(QUERY_EXECUTOR_SOURCE_NAME);
    }

    @Override
    public void execute(UUID qryId, long fragmentId, Runnable qryTask) {
        int commandIdx = hash(qryId, fragmentId);
        stripedThreadPoolExecutor.execute(
                () -> {
                    try {
                        qryTask.run();
                    } catch (Throwable e) {
                        /*
                         * No exceptions are rethrown here to preserve the current thread from being destroyed,
                         * because other queries may be pinned to the current thread id.
                         * However, any exception here considered as Unexpected and must be processed by FailureHandler.
                         */

                        String message = String.format(
                                "Unexpected error during execute fragment %d of query %s",
                                fragmentId,
                                qryId);

                        failureManager.process(
                                new FailureContext(CRITICAL_ERROR, new IgniteException(INTERNAL_ERR, message, e))
                        );
                    }
                },
                commandIdx
        );
    }

    @Override
    public void execute(Runnable command) {
        execute(
                QUERY_ID_STUB,
                ThreadLocalRandom.current().nextLong(1024),
                command
        );
    }

    @Override
    public CompletableFuture<?> submit(UUID qryId, long fragmentId, Runnable qryTask) {
        return stripedThreadPoolExecutor.submit(qryTask, hash(qryId, fragmentId));
    }

    private static int hash(UUID qryId, long fragmentId) {
        // inlined Objects.hash(...)
        return IgniteUtils.safeAbs(31 * (31 + (qryId != null ? qryId.hashCode() : 0)) + Long.hashCode(fragmentId));
    }

    @Override
    public void stop() {
        if (stripedThreadPoolExecutor != null) {
            stripedThreadPoolExecutor.shutdownNow();

            metricManager.unregisterSource(QUERY_EXECUTOR_SOURCE_NAME);
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return stripedThreadPoolExecutor.awaitTermination(timeout, unit);
    }

    /** Returns the task queue size used by this executor. */
    @TestOnly
    public int queueSize() {
        int totalQueueSize = 0;

        for (int i = 0; i < concurrencyLevel; i++) {
            totalQueueSize += ((ThreadPoolExecutor) stripedThreadPoolExecutor.stripeExecutor(i)).getQueue().size();
        }

        return totalQueueSize;
    }
}
