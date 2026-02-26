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

package org.apache.ignite.internal.app;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource.THREAD_POOLS_METRICS_SOURCE_NAME;
import static org.apache.ignite.internal.thread.ThreadOperation.PROCESS_RAFT_REQ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.ThreadPoolMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Component that hosts thread pools which do not belong to a certain component and which are global to an Ignite instance.
 */
@Factory
public class ThreadPoolsManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadPoolsManager.class);

    /**
     * Separate executor for IO operations like partition storage initialization, partition raft group meta data persisting,
     * index storage creation...
     */
    private final ScheduledExecutorService tableIoExecutor;

    /**
     * Executor on which partition operations are executed. Might do storage reads and writes (so it's expected to execute disk I/O).
     */
    private final ExecutorService partitionOperationsExecutor;

    private final ScheduledExecutorService commonScheduler;

    /** Executor for scheduling rebalance routine. */
    private final ScheduledExecutorService rebalanceScheduler;

    private final MetricManager metricManager;

    private final List<ThreadPoolMetricSource> metricSources;

    /**
     * Constructor.
     */
    public ThreadPoolsManager(@Value("${node-name}") String nodeName, MetricManager metricManager) {
        int cpus = Runtime.getRuntime().availableProcessors();

        tableIoExecutor = Executors.newScheduledThreadPool(
                Math.min(cpus * 3, 25),
                IgniteThreadFactory.create(nodeName, "tableManager-io", LOG, STORAGE_READ, STORAGE_WRITE)
        );

        int partitionsOperationsThreads = Math.min(cpus * 3, 25);
        partitionOperationsExecutor = Executors.newFixedThreadPool(
                partitionsOperationsThreads,
                IgniteThreadFactory.create(
                        nodeName,
                        "partition-operations",
                        LOG,
                        STORAGE_READ,
                        STORAGE_WRITE,
                        TX_STATE_STORAGE_ACCESS,
                        PROCESS_RAFT_REQ
                )
        );

        commonScheduler = Executors.newSingleThreadScheduledExecutor(IgniteThreadFactory.create(nodeName, "common-scheduler", LOG));

        rebalanceScheduler = Executors.newSingleThreadScheduledExecutor(IgniteThreadFactory.create(nodeName, "rebalance-scheduler", LOG));

        this.metricManager = metricManager;

        metricSources = new ArrayList<>();
        metricSources.add(
                new ThreadPoolMetricSource(
                        THREAD_POOLS_METRICS_SOURCE_NAME + "partitions-executor",
                        "The partitions-executor pool handles all the table related operations.",
                        (ThreadPoolExecutor) partitionOperationsExecutor)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        metricSources.forEach(metricSource -> {
            metricManager.registerSource(metricSource);
            metricManager.enable(metricSource);
        });

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        IgniteUtils.shutdownAndAwaitTermination(tableIoExecutor, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(commonScheduler, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(rebalanceScheduler, 10, SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Returns executor used to create/destroy storages, start partition Raft groups, create index storages...
     */
    @Singleton
    @Named("tableIoExecutor")
    public ScheduledExecutorService tableIoExecutor() {
        return tableIoExecutor;
    }

    /**
     * Returns the executor of partition operations.
     */
    @Singleton
    @Named("partitionOperationsExecutor")
    public ExecutorService partitionOperationsExecutor() {
        return partitionOperationsExecutor;
    }

    /**
     * Returns a global {@link ScheduledExecutorService}. Only small tasks should be scheduled.
     */
    @Singleton
    @Named("commonScheduler")
    public ScheduledExecutorService commonScheduler() {
        return commonScheduler;
    }

    /** Returns executor for scheduling rebalance routine. */
    @Singleton
    @Named("rebalanceScheduler")
    public ScheduledExecutorService rebalanceScheduler() {
        return rebalanceScheduler;
    }
}
