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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** Component is responsible for starting and stopping {@link IndexBuildingStarterTask}. */
class IndexBuildingStarter implements ManuallyCloseable {
    private final CatalogManager catalogManager;

    private final ClusterService clusterService;

    private final LogicalTopologyService logicalTopologyService;

    private final HybridClock clock;

    private final ClockWaiter clockWaiter;

    private final PlacementDriver placementDriver;

    private final Executor executor;

    private final Map<IndexBuildingStarterTaskId, IndexBuildingStarterTask> taskById = new ConcurrentHashMap<>();

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    IndexBuildingStarter(
            CatalogManager catalogManager,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            HybridClock clock,
            ClockWaiter clockWaiter,
            PlacementDriver placementDriver,
            Executor executor
    ) {
        this.catalogManager = catalogManager;
        this.clusterService = clusterService;
        this.logicalTopologyService = logicalTopologyService;
        this.clock = clock;
        this.clockWaiter = clockWaiter;
        this.placementDriver = placementDriver;
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
     * Schedules {@link IndexBuildingStarterTask} for the table index if it is not already in progress.
     *
     * @param indexDescriptor Index descriptor.
     */
    void scheduleTask(CatalogIndexDescriptor indexDescriptor) {
        inBusyLockSafe(busyLock, () -> {
            IndexBuildingStarterTaskId taskId = taskId(indexDescriptor);

            IndexBuildingStarterTask task = new IndexBuildingStarterTask(
                    indexDescriptor,
                    catalogManager,
                    placementDriver,
                    clusterService,
                    logicalTopologyService,
                    clock,
                    clockWaiter,
                    executor,
                    busyLock
            );

            if (taskById.putIfAbsent(taskId, task) != null) {
                // Task has already been added and is running.
                return;
            }

            task.start().whenComplete((unused, throwable) -> taskById.remove(taskId));
        });
    }

    /**
     * Stops {@link IndexBuildingStarterTask} for the table index if it is present.
     *
     * @param indexDescriptor Index descriptor.
     */
    void stopTask(CatalogIndexDescriptor indexDescriptor) {
        inBusyLockSafe(busyLock, () -> {
            IndexBuildingStarterTask removed = taskById.remove(taskId(indexDescriptor));

            if (removed != null) {
                removed.stop();
            }
        });
    }

    /**
     * Stops all {@link IndexBuildingStarterTask} that are present for the table.
     *
     * @param tableId Table ID.
     */
    void stopTasks(int tableId) {
        inBusyLockSafe(busyLock, () -> {
            Iterator<Entry<IndexBuildingStarterTaskId, IndexBuildingStarterTask>> it = taskById.entrySet().iterator();

            while (it.hasNext()) {
                Entry<IndexBuildingStarterTaskId, IndexBuildingStarterTask> e = it.next();

                if (e.getKey().tableId() == tableId) {
                    it.remove();

                    e.getValue().stop();
                }
            }
        });
    }

    private void stopAllTasks() {
        taskById.values().forEach(IndexBuildingStarterTask::stop);
        taskById.clear();
    }

    private static IndexBuildingStarterTaskId taskId(CatalogIndexDescriptor indexDescriptor) {
        return new IndexBuildingStarterTaskId(indexDescriptor.tableId(), indexDescriptor.id());
    }
}
