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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.thread.ThreadOperation.TX_STATE_STORAGE_ACCESS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Component that hosts thread pools which do not belong to a certain component and which are global to an Ignite instance.
 */
public class ThreadPoolsManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadPoolsManager.class);

    /**
     * Separate executor for IO operations like partition storage initialization, partition raft group meta data persisting,
     * index storage creation...
     */
    private final ExecutorService tableIoExecutor;

    /**
     * Executor on which partition operations are executed. Might do storage reads and writes (so it's expected to execute disk I/O).
     */
    private final ExecutorService partitionOperationsExecutor;

    private final ScheduledExecutorService commonScheduler;

    /**
     * Constructor.
     */
    public ThreadPoolsManager(String nodeName) {
        int cpus = Runtime.getRuntime().availableProcessors();

        tableIoExecutor = new ThreadPoolExecutor(
                Math.min(cpus * 3, 25),
                Integer.MAX_VALUE,
                100,
                MILLISECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "tableManager-io", LOG, STORAGE_READ, STORAGE_WRITE));

        int partitionsOperationsThreads = Math.min(cpus * 3, 25);
        partitionOperationsExecutor = new ThreadPoolExecutor(
                partitionsOperationsThreads,
                partitionsOperationsThreads,
                0, SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "partition-operations", LOG, STORAGE_READ, STORAGE_WRITE, TX_STATE_STORAGE_ACCESS)
        );

        commonScheduler = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory.create(nodeName, "common-scheduler", LOG));
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        // No-op.
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        IgniteUtils.shutdownAndAwaitTermination(tableIoExecutor, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(commonScheduler, 10, SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Returns executor used to create/destroy storages, start partition Raft groups, create index storages...
     */
    public ExecutorService tableIoExecutor() {
        return tableIoExecutor;
    }

    /**
     * Returns the executor of partition operations.
     */
    public ExecutorService partitionOperationsExecutor() {
        return partitionOperationsExecutor;
    }

    /**
     * Returns a global {@link ScheduledExecutorService}. Only small tasks should be scheduled.
     */
    public ScheduledExecutorService commonScheduler() {
        return commonScheduler;
    }
}
