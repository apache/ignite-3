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

package org.apache.ignite.internal.metastorage.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Scheduler that triggers idempotent cache vacuumization with an ability to suspend and resume the triggering. It is valid but not
 * effective to have multiple vacuumizers at the same time, meaning that best-effort oneness is preferable. In order to achieve such
 * best-effort oneness it's possible to use meta storage leader collocation: start/resume triggering on leader election if the leader is
 * collocated with a local node, and suspend upon loss of collocation with the leader.
 * In case of exception within vacuumization action, vacuumizer will log an error, stop itself but will not re-throw an exception to the
 * outer environment.
 */
class IdempotentCacheVacuumizer {
    private static final IgniteLogger LOG = Loggers.forClass(IdempotentCacheVacuumizer.class);

    @Nullable
    private volatile ScheduledExecutorService scheduler;

    private final AtomicBoolean triggerVacuumization;

    /**
     * The constructor.
     *
     * @param nodeName Node name.
     * @param vacuumizationAction Action to scheduler.
     * @param initialDelay The time to delay first execution.
     * @param delay The delay between the termination of one execution and the commencement of the next
     * @param unit The time unit of the initialDelay and delay parameters.
     */
    IdempotentCacheVacuumizer(
            String nodeName,
            Runnable vacuumizationAction,
            long initialDelay,
            long delay,
            TimeUnit unit
    ) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "idempotent-cache-vacuumizer", LOG));

        this.triggerVacuumization = new AtomicBoolean(false);

        scheduler.scheduleWithFixedDelay(
                () -> {
                    if (triggerVacuumization.get()) {
                        try {
                            vacuumizationAction.run();
                        } catch (Exception e) {
                            LOG.error("An exception occurred while executing idempotent cache vacuumization action."
                                    + " Idempotent cache vacuumizer will be stopped.", e);

                            shutdown();
                        }
                    }
                },
                initialDelay,
                delay,
                unit
        );
    }

    /**
     * Starts local vacuumization triggering. Will take no effect if vacuumizer was previously stopped.
     */
    public void startLocalVacuumizationTriggering() {
        triggerVacuumization.set(true);
    }

    /**
     * Suspends further local vacuumization triggering. Will take no effect if vacuumizer was previously stopped.
     */
    public void suspendLocalVacuumizationTriggering() {
        triggerVacuumization.set(false);
    }

    /**
     * Halts local vacuumization triggering.
     */
    public synchronized void shutdown() {
        if (scheduler != null) {
            suspendLocalVacuumizationTriggering();
            scheduler.shutdownNow();

            scheduler = null;
        }
    }
}
