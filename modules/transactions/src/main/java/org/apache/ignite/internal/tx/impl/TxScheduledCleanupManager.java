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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Manager that is responsible for the scheduling of transaction cleanup procedures.
 */
public class TxScheduledCleanupManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxScheduledCleanupManager.class);

    private static final int RESOURCE_CLEANUP_EXECUTOR_SIZE = 1;

    /** System property name. */
    public static final String RESOURCE_CLEANUP_INTERVAL_MILLISECONDS_PROPERTY = "RESOURCE_CLEANUP_INTERVAL_MILLISECONDS";

    private final int resourceCleanupIntervalMilliseconds = IgniteSystemProperties
            .getInteger(RESOURCE_CLEANUP_INTERVAL_MILLISECONDS_PROPERTY, 30_000);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ScheduledExecutorService resourceCleanupExecutor;

    private final List<Runnable> scheduledOperations = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param nodeName Name of the Ignite node.
     */
    public TxScheduledCleanupManager(String nodeName) {
        resourceCleanupExecutor = Executors.newScheduledThreadPool(
                RESOURCE_CLEANUP_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "resource-cleanup-executor", LOG)
        );
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> start() {
        resourceCleanupExecutor.scheduleAtFixedRate(
                this::runOperations,
                0,
                resourceCleanupIntervalMilliseconds,
                TimeUnit.MILLISECONDS
        );

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        busyLock.block();

        shutdownAndAwaitTermination(resourceCleanupExecutor, 10, TimeUnit.SECONDS);
    }

    private void runOperations() {
        inBusyLock(busyLock, () -> {
            for (Runnable operation : scheduledOperations) {
                operation.run();
            }
        });
    }

    /**
     * Register scheduled operation.
     *
     * @param operation The operation.
     */
    public void registerScheduledOperation(Runnable operation) {
        scheduledOperations.add(operation);
    }
}
