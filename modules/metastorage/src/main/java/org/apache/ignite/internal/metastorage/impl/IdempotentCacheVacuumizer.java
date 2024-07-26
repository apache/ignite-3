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

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Scheduler that triggers idempotent cache vacuumization with an ability to suspend and resume the triggering. It is valid but not
 * effective to have multiple vacuumizers at the same time, meaning that best-effort oneness is preferable. In order to achieve such
 * best-effort oneness it's possible to use meta storage leader collocation: start/resume triggering on leader election if the leader is
 * collocated with a local node, and suspend upon loss of collocation with the leader.
 */
class IdempotentCacheVacuumizer {
    private static final IgniteLogger LOG = Loggers.forClass(IdempotentCacheVacuumizer.class);

    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean triggerVacuumization;

    IdempotentCacheVacuumizer(String nodeName, Runnable vacuumizationAction) {
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
                            // TODO sanpwc Ensure that exception handling will halt the scheduler. And extend the javadoc respectively.
                        }
                    }
                },
                1,
                1,
                MINUTES);
    }

    /**
     * Starts local vacuumization triggering.
     */
    public void startLocalVacuumizationTriggering() {
        triggerVacuumization.set(true);
    }

    /**
     * Stops further local vacuumization triggering.
     */
    public void stopLocalVacuumizationTriggering() {
        triggerVacuumization.set(false);
    }

    /**
     * Halts local vacuumization triggering.
     */
    public void stop() {
        stopLocalVacuumizationTriggering();
        scheduler.shutdownNow();
    }
}
