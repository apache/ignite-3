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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Component that hosts thread pools which do not belong to a certain component and which are global to an Ignite instance.
 */
public class ThreadPoolsManager implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadPoolsManager.class);

    private final StripedThreadPoolExecutor partitionOperationsExecutor;

    private final ScheduledExecutorService commonScheduler;

    /**
     * Constructor.
     */
    public ThreadPoolsManager(String nodeName) {
        partitionOperationsExecutor = new StripedThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 25),
                NamedThreadFactory.threadPrefix(nodeName, "partition-operations"),
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );

        commonScheduler = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory.create(nodeName, "common-scheduler", LOG));
    }

    @Override
    public CompletableFuture<Void> start() {
        // No-op.
        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(commonScheduler, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns the executor of partition operations.
     */
    public StripedThreadPoolExecutor partitionOperationsExecutor() {
        return partitionOperationsExecutor;
    }

    /**
     * Returns a global {@link ScheduledExecutorService}. Only small tasks should be scheduled.
     */
    public ScheduledExecutorService commonScheduler() {
        return commonScheduler;
    }
}
