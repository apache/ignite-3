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

package org.apache.ignite.internal.deployunit;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Undeploys deployment units that are not acquired by any class loader. This class is thread-safe.
 */
class DeploymentUnitUndeployer {
    private static final IgniteLogger LOG = Loggers.forClass(DeploymentUnitUndeployer.class);

    /** Deployment units to undeploy. */
    private final Queue<DeploymentUnit> unitsToUndeploy = new ConcurrentLinkedDeque<>();

    /** Deployment unit accessor. */
    private final DeploymentUnitAccessor deploymentUnitAccessor;

    /** Executor. */
    private final ScheduledExecutorService executor;

    /** Undeploy function. */
    private final Consumer<DeploymentUnit> undeploy;

    /**
     * Creates undeployer.
     *
     * @param nodeName node name.
     * @param deploymentUnitAccessor deployment unit accessor.
     * @param undeploy undeploy function.
     */
    DeploymentUnitUndeployer(
            String nodeName,
            DeploymentUnitAccessor deploymentUnitAccessor,
            Consumer<DeploymentUnit> undeploy) {
        this.deploymentUnitAccessor = deploymentUnitAccessor;
        this.executor = Executors.newScheduledThreadPool(
                1, NamedThreadFactory.create(nodeName, "deployment-unit-undeployer", LOG));
        this.undeploy = undeploy;
    }

    /**
     * Starts the undeployer.
     *
     * @param delay delay between undeploy attempts.
     * @param unit time unit of the delay.
     */
    public void start(long delay, TimeUnit unit) {
        executor.scheduleWithFixedDelay(this::undeployUnits, 0, delay, unit);
    }

    /**
     * Stops the undeployer.
     */
    public void stop() {
        executor.shutdown();
    }

    /**
     * Undeploys deployment units that are not acquired by any class loader. If a deployment unit is acquired, it is put back to the queue.
     */
    private void undeployUnits() {
        int size = unitsToUndeploy.size();
        for (int i = 0; i < size; i++) {
            undeploy(unitsToUndeploy.remove());
        }
    }

    /**
     * Undeploys deployment unit. If the unit is acquired by any class loader, it is put to the queue.
     *
     * @param unit deployment unit to undeploy.
     */
    public void undeploy(DeploymentUnit unit) {
        if (!deploymentUnitAccessor.isAcquired(unit)) {
            undeploy.accept(unit);
        } else {
            unitsToUndeploy.offer(unit);
        }
    }
}
