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

package org.apache.ignite.internal.index;

import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockSafe;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.RemoveIndexCommand;
import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Component is responsible for starting and stopping {@link ChangeIndexStatusTask}. */
class ChangeIndexStatusTaskScheduler implements ManuallyCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(ChangeIndexStatusTaskScheduler.class);

    private final CatalogManager catalogManager;

    private final ClusterService clusterService;

    private final LogicalTopologyService logicalTopologyService;

    private final ClockService clockService;

    private final PlacementDriver placementDriver;

    private final IndexMetaStorage indexMetaStorage;

    private final Executor executor;

    private final Map<ChangeIndexStatusTaskId, ChangeIndexStatusTask> taskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    ChangeIndexStatusTaskScheduler(
            CatalogManager catalogManager,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            ClockService clockService,
            PlacementDriver placementDriver,
            IndexMetaStorage indexMetaStorage,
            Executor executor
    ) {
        this.catalogManager = catalogManager;
        this.clusterService = clusterService;
        this.logicalTopologyService = logicalTopologyService;
        this.clockService = clockService;
        this.placementDriver = placementDriver;
        this.indexMetaStorage = indexMetaStorage;
        this.executor = executor;
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        stopAllTasks();
    }

    /**
     * Schedules a task for that will transfer the given index to the {@link CatalogIndexStatus#BUILDING} state.
     */
    void scheduleStartBuildingTask(CatalogIndexDescriptor indexDescriptor) {
        assert indexDescriptor.status() == CatalogIndexStatus.REGISTERED;

        LOG.info("Scheduling starting of index building. Index: {}", indexDescriptor);

        inBusyLockSafe(busyLock, () -> {
            var taskId = new ChangeIndexStatusTaskId(indexDescriptor);

            scheduleTaskBusy(taskId, startBuildingIndexTask(indexDescriptor));
        });
    }

    private ChangeIndexStatusTask startBuildingIndexTask(CatalogIndexDescriptor indexDescriptor) {
        return new ChangeIndexStatusTask(
                indexDescriptor,
                catalogManager,
                placementDriver,
                clusterService,
                logicalTopologyService,
                clockService,
                indexMetaStorage,
                executor,
                busyLock
        ) {
            @Override
            CatalogCommand switchIndexStatusCommand() {
                return StartBuildingIndexCommand.builder().indexId(indexDescriptor.id()).build();
            }
        };
    }

    /**
     * Schedules a task for that will remove a given index from the Catalog.
     */
    void scheduleRemoveIndexTask(CatalogIndexDescriptor indexDescriptor) {
        assert indexDescriptor.status() == CatalogIndexStatus.STOPPING;

        LOG.info("Scheduling index removal. Index: {}", indexDescriptor);

        inBusyLockSafe(busyLock, () -> {
            var taskId = new ChangeIndexStatusTaskId(indexDescriptor);

            scheduleTaskBusy(taskId, removeIndexTask(indexDescriptor));
        });
    }

    private ChangeIndexStatusTask removeIndexTask(CatalogIndexDescriptor indexDescriptor) {
        return new ChangeIndexStatusTask(
                indexDescriptor,
                catalogManager,
                placementDriver,
                clusterService,
                logicalTopologyService,
                clockService,
                indexMetaStorage,
                executor,
                busyLock
        ) {
            @Override
            CatalogCommand switchIndexStatusCommand() {
                return RemoveIndexCommand.builder().indexId(indexDescriptor.id()).build();
            }
        };
    }

    private void scheduleTaskBusy(ChangeIndexStatusTaskId taskId, ChangeIndexStatusTask task) {
        // Check if the task has already been added before.
        if (taskById.putIfAbsent(taskId, task) == null) {
            task.start().whenComplete((unused, throwable) -> taskById.remove(taskId));
        } else {
            LOG.info("Skipping task scheduling, because a task with the same ID is already running.");
        }
    }

    /**
     * Stops an ongoing {@link ChangeIndexStatusTask} for a given index if it is present.
     *
     * @param indexId Index ID.
     */
    void stopStartBuildingTask(int indexId) {
        inBusyLockSafe(busyLock, () -> {
            ChangeIndexStatusTask removed = taskById.remove(new ChangeIndexStatusTaskId(indexId, CatalogIndexStatus.REGISTERED));

            if (removed != null) {
                removed.stop();
            }
        });
    }

    /**
     * Stops {@link ChangeIndexStatusTask}s that correspond to all indices of a given table.
     *
     * @param tableId Table ID.
     */
    void stopTasksForTable(int tableId) {
        inBusyLockSafe(busyLock, () -> {
            Iterator<ChangeIndexStatusTask> it = taskById.values().iterator();

            while (it.hasNext()) {
                ChangeIndexStatusTask task = it.next();

                if (task.targetIndex().tableId() == tableId) {
                    it.remove();

                    task.stop();
                }
            }
        });
    }

    private void stopAllTasks() {
        taskById.values().forEach(ChangeIndexStatusTask::stop);
        taskById.clear();
    }
}
