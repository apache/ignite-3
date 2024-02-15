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
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.impl.CursorManager.CursorInfo;
import org.apache.ignite.network.ClusterNodeResolver;

/**
 * Manager that is responsible for cleaning up the orphan transaction resources.
 */
public class TxResourceCleanupManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxResourceCleanupManager.class);

    private static final int RESOURCE_CLEANUP_EXECUTOR_SIZE = 1;

    private static final int RESOURCE_CLEANUP_INTERVAL_MILLISECONDS = IgniteSystemProperties
            .getInteger("RESOURCE_CLEANUP_INTERVAL_MILLISECONDS", 30_000);

    private final ScheduledExecutorService resourceCleanupExecutor;

    private final CursorManager cursorManager;

    private final ClusterNodeResolver clusterNodeResolver;

    /**
     * Constructor.
     *
     * @param nodeName Name of the Ignite node.
     * @param cursorManager Cursor manager.
     * @param clusterNodeResolver Cluster node resolver.
     */
    public TxResourceCleanupManager(
            String nodeName,
            CursorManager cursorManager,
            ClusterNodeResolver clusterNodeResolver
    ) {
        resourceCleanupExecutor = Executors.newScheduledThreadPool(
                RESOURCE_CLEANUP_EXECUTOR_SIZE,
                NamedThreadFactory.create(nodeName, "resource-cleanup-executor", LOG)
        );
        this.cursorManager = cursorManager;
        this.clusterNodeResolver = clusterNodeResolver;
    }

    @Override
    public CompletableFuture<Void> start() {
        resourceCleanupExecutor.scheduleAtFixedRate(
                this::cleanupOrphanTxResources,
                0,
                RESOURCE_CLEANUP_INTERVAL_MILLISECONDS,
                TimeUnit.MILLISECONDS
        );

        return nullCompletedFuture();
    }

    @Override
    public void stop() {
        shutdownAndAwaitTermination(resourceCleanupExecutor, 10, TimeUnit.SECONDS);
    }

    private void cleanupOrphanTxResources() {
        Map<IgniteUuid, CursorInfo> cursorInfos = cursorManager.cursors();

        try {
            for (Map.Entry<IgniteUuid, CursorInfo> cursorInfoEntry : cursorInfos.entrySet()) {
                CursorInfo cursorInfo = cursorInfoEntry.getValue();

                if (clusterNodeResolver.getById(cursorInfo.txCoordinatorId()) == null) {
                    try {
                        cursorManager.closeCursor(cursorInfoEntry.getKey());
                    } catch (Exception e) {
                        LOG.warn("Error occured during the orphan cursor closing [txCoordinatorId={}]", e, cursorInfo.txCoordinatorId());
                    }
                }
            }
        } catch (Throwable err) {
            LOG.error("Error occured during the orphan cursors closing.", err);

            if (err instanceof Error) {
                throw err;
            }
        }
    }
}
