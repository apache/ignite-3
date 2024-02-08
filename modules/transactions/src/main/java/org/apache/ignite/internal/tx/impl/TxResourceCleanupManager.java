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

import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

public class TxResourceCleanupManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxResourceCleanupManager.class);

    private static final int RESOURCE_CLEANUP_EXECUTOR_SIZE = 1;

    private static final int RESOURCE_CLEANUP_INTERVAL_MILLISECONDS = 1;

    private final ScheduledExecutorService resourceCleanupExecutor;

    public TxResourceCleanupManager(String nodeName) {
        resourceCleanupExecutor = new ScheduledThreadPoolExecutor(RESOURCE_CLEANUP_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "resource-cleanup-executor", LOG));
    }

    public void start() {
        resourceCleanupExecutor.scheduleAtFixedRate(
                this::cleanupOrphanCursors,
                0,
                RESOURCE_CLEANUP_INTERVAL_MILLISECONDS,
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        shutdownAndAwaitTermination(resourceCleanupExecutor, 10, TimeUnit.SECONDS);
    }

    private void cleanupOrphanCursors() {

    }
}
