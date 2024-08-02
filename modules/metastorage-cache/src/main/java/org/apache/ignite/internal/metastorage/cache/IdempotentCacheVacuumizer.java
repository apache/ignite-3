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

package org.apache.ignite.internal.metastorage.cache;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.impl.ElectionListener;
import org.apache.ignite.network.ClusterNode;

/**
 * Scheduler wrapper that triggers idempotent cache vacuumization with an ability to suspend and resume the triggering. It is valid but not
 * effective to have multiple vacuumizers at the same time, meaning that best-effort oneness is preferable. In order to achieve such
 * best-effort oneness it's possible to use meta storage leader collocation: start/resume triggering on leader election if the leader is
 * collocated with a local node, and suspend upon loss of collocation with the leader.
 * In case of exception within vacuumization action, vacuumizer will just log a warning without suspending the scheduler.
 */
public class IdempotentCacheVacuumizer implements ElectionListener {
    private static final IgniteLogger LOG = Loggers.forClass(IdempotentCacheVacuumizer.class);

    private final AtomicBoolean triggerVacuumization;

    private final String nodeName;

    /**
     * The constructor.
     *
     * @param nodeName Node name.
     * @param scheduler Scheduler to run vacuumization actions.
     * @param vacuumizationAction Action that will trigger vacuumization process.
     * @param idempotentCacheTtl Idempotent cache ttl.
     * @param clockService Clock service.
     * @param initialDelay The time to delay first execution.
     * @param delay The delay between the termination of one execution and the commencement of the next.
     * @param unit The time unit of the initialDelay and delay parameters.
     */
    public IdempotentCacheVacuumizer(
            String nodeName,
            ScheduledExecutorService scheduler,
            Consumer<HybridTimestamp> vacuumizationAction,
            ConfigurationValue<Long> idempotentCacheTtl,
            ClockService clockService,
            long initialDelay,
            long delay,
            TimeUnit unit
    ) {
        this.nodeName = nodeName;
        this.triggerVacuumization = new AtomicBoolean(false);

        scheduler.scheduleWithFixedDelay(
                () -> {
                    if (triggerVacuumization.get()) {
                        try {
                            vacuumizationAction.accept(hybridTimestamp(clockService.nowLong()
                                    - (idempotentCacheTtl.value() + clockService.maxClockSkewMillis())));
                        } catch (Exception e) {
                            LOG.warn("An exception occurred while executing idempotent cache vacuumization action."
                                    + " Idempotent cache vacuumizer won't be stopped.", e);
                        }
                    }
                },
                initialDelay,
                delay,
                unit
        );
    }

    @Override
    public void onLeaderElected(ClusterNode newLeader) {
        if (newLeader.name().equals(nodeName)) {
            startLocalVacuumizationTriggering();
        } else {
            suspendLocalVacuumizationTriggering();
        }
    }

    /**
     * Starts local vacuumization triggering. Will take no effect if vacuumizer was previously stopped.
     */
    void startLocalVacuumizationTriggering() {
        triggerVacuumization.set(true);
        LOG.info("Idempotent cache vacuumizer started.");
    }

    /**
     * Suspends further local vacuumization triggering. Will take no effect if vacuumizer was previously stopped.
     */
    void suspendLocalVacuumizationTriggering() {
        triggerVacuumization.set(false);
        LOG.info("Idempotent cache vacuumizer suspended.");
    }
}
