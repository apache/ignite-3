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

package org.apache.ignite.internal.metrics.logstorage;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.thread.IgniteThreadFactory;

/**
 * Component that collects metrics about log storages.
 */
public class LogStorageMetrics implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(LogStorageMetrics.class);

    private static final long DEFAULT_UPDATE_PERIOD_MS = 1000;

    private final String nodeName;

    private final MetricManager metricManager;

    private final LogStorageManager cmgLogStorageManager;
    private final LogStorageManager metastorageLogStorageManager;
    private final LogStorageManager partitionsLogStorageManager;
    private final VolatileLogStorageManagerCreator volatileLogStorageManagerCreator;

    private final long updatePeriodMs;

    private final LogStorageMetricSource metricSource = new LogStorageMetricSource();

    private volatile ScheduledExecutorService executorService;
    private volatile ScheduledFuture<?> taskFuture;

    /** Constructor. */
    public LogStorageMetrics(
            String nodeName,
            MetricManager metricManager,
            LogStorageManager cmgLogStorageManager,
            LogStorageManager metastorageLogStorageManager,
            LogStorageManager partitionsLogStorageManager,
            VolatileLogStorageManagerCreator volatileLogStorageManagerCreator
    ) {
        this(
                nodeName,
                metricManager,
                cmgLogStorageManager,
                metastorageLogStorageManager,
                partitionsLogStorageManager,
                volatileLogStorageManagerCreator,
                DEFAULT_UPDATE_PERIOD_MS
        );
    }

    /** Constructor. */
    LogStorageMetrics(
            String nodeName,
            MetricManager metricManager,
            LogStorageManager cmgLogStorageManager,
            LogStorageManager metastorageLogStorageManager,
            LogStorageManager partitionsLogStorageManager,
            VolatileLogStorageManagerCreator volatileLogStorageManagerCreator,
            long updatePeriodMs
    ) {
        this.nodeName = nodeName;
        this.metricManager = metricManager;
        this.cmgLogStorageManager = cmgLogStorageManager;
        this.metastorageLogStorageManager = metastorageLogStorageManager;
        this.partitionsLogStorageManager = partitionsLogStorageManager;
        this.volatileLogStorageManagerCreator = volatileLogStorageManagerCreator;
        this.updatePeriodMs = updatePeriodMs;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        executorService = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "log-storage-metrics-collector", LOG)
        );
        taskFuture = executorService.scheduleAtFixedRate(this::updateMetrics, 0, updatePeriodMs, MILLISECONDS);

        return nullCompletedFuture();
    }

    private void updateMetrics() {
        try {
            metricSource.cmgLogStorageSize(cmgLogStorageManager.totalBytesOnDisk());
            metricSource.metastorageLogStorageSize(metastorageLogStorageManager.totalBytesOnDisk());
            metricSource.partitionsLogStorageSize(
                    partitionsLogStorageManager.totalBytesOnDisk() + volatileLogStorageManagerCreator.totalBytesOnDisk()
            );
        } catch (Exception | AssertionError e) {
            if (!hasCause(e, NodeStoppingException.class)) {
                LOG.warn("Failed to update log storage metrics", e);
            }
        } catch (Error e) {
            LOG.error("Failed to update log storage metrics, no more updates will happen", e);

            throw e;
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        ScheduledFuture<?> futureToCancel = taskFuture;
        if (futureToCancel != null) {
            futureToCancel.cancel(true);
        }

        ScheduledExecutorService executorToShutdown = executorService;
        if (executorToShutdown != null) {
            executorToShutdown.shutdownNow();

            // If we fail to finish waiting, our task might end up hitting a stopped RocksDB instance that will crash the node.
            try {
                executorToShutdown.awaitTermination(Long.MAX_VALUE, SECONDS);
            } catch (InterruptedException e) {
                // Ok, we are interrupted, cannot wait any longer.
                Thread.currentThread().interrupt();
            }
        }

        metricManager.unregisterSource(metricSource);

        return nullCompletedFuture();
    }
}
