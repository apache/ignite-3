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
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Executes action on the deployment unit if it is not acquired. Otherwise, puts it to the queue.
 */
class DeploymentUnitAcquiredWaiter {
    private static final IgniteLogger LOG = Loggers.forClass(DeploymentUnitAcquiredWaiter.class);

    /** Deployment units to undeploy. */
    private final Queue<DeploymentUnit> queue = new ConcurrentLinkedDeque<>();

    /** Deployment unit accessor. */
    private final DeploymentUnitAccessor deploymentUnitAccessor;

    /** Executor. */
    private final ScheduledExecutorService executor;

    /** Action. */
    private final Consumer<DeploymentUnit> action;

    /**
     * Creates processor.
     *
     * @param nodeName node name.
     * @param deploymentUnitAccessor deployment unit accessor.
     * @param action action.
     */
    DeploymentUnitAcquiredWaiter(
            String nodeName,
            DeploymentUnitAccessor deploymentUnitAccessor,
            Consumer<DeploymentUnit> action) {
        this.deploymentUnitAccessor = deploymentUnitAccessor;
        this.executor = Executors.newScheduledThreadPool(
                1, NamedThreadFactory.create(nodeName, "deployment-unit-acquired-waiter", LOG));
        this.action = action;
    }

    /**
     * Starts the processor.
     *
     * @param delay delay between undeploy attempts.
     * @param unit time unit of the delay.
     */
    public void start(long delay, TimeUnit unit) {
        executor.scheduleWithFixedDelay(this::processQueue, 0, delay, unit);
    }

    /**
     * Stops the processor.
     */
    public void stop() {
        executor.shutdown();
    }

    /**
     * Processes all deployment units in the queue.
     */
    private void processQueue() {
        int size = queue.size();
        for (int i = 0; i < size; i++) {
            submitToAcquireRelease(queue.remove());
        }
    }

    /**
     * Executes the action on the deployment unit if it is not acquired. Otherwise, puts it to the queue.
     *
     * @param unit deployment unit to undeploy.
     */
    public void submitToAcquireRelease(DeploymentUnit unit) {
        if (!deploymentUnitAccessor.computeIfNotAcquired(unit, action)) {
            queue.offer(unit);
        }
    }
}
