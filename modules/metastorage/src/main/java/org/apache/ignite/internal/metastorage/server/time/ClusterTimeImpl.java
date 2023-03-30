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

package org.apache.ignite.internal.metastorage.server.time;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;

public class ClusterTimeImpl implements ClusterTime {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ClusterTimeImpl.class);

    private final IgniteSpinBusyLock busyLock;

    private volatile @Nullable LeaderTimer leaderTimer;

    private final HybridClock clock;

    private final PendingComparableValuesTracker<HybridTimestamp> safeTime;

    public ClusterTimeImpl(IgniteSpinBusyLock busyLock, HybridClock clock) {
        this.busyLock = busyLock;
        this.clock = clock;
        this.safeTime = new PendingComparableValuesTracker<>(clock.now());
    }

    public void startLeaderTimer(MetaStorageServiceImpl service) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            assert leaderTimer == null;

            LeaderTimer newTimer = new LeaderTimer(service);

            leaderTimer = newTimer;

            newTimer.start();
        } finally {
            busyLock.leaveBusy();
        }
    }

    public void stopLeaderTimer() {
        LeaderTimer timer = leaderTimer;

        assert timer != null;

        timer.stop();

        leaderTimer = null;
    }

    @Override
    public HybridTimestamp now() {
        return clock.now();
    }

    @Override
    public CompletableFuture<Void> waitFor(HybridTimestamp time) {
        return safeTime.waitFor(time);
    }

    public void updateSafeTime(HybridTimestamp ts) {
        this.safeTime.update(ts);
    }

    public void adjust(HybridTimestamp ts) {
        this.clock.update(ts);
    }

    private class LeaderTimer {

        private final MetaStorageServiceImpl service;

        /** Scheduled executor for cluster time sync. */
        private final ScheduledExecutorService scheduledClusterTimeSyncExecutor =
                Executors.newScheduledThreadPool(1, new NamedThreadFactory("scheduled-cluster-time-sync-thread", LOG));

        private LeaderTimer(MetaStorageServiceImpl service) {
            this.service = service;
        }

        void start() {
            schedule();
        }

        private void schedule() {
            try {
                scheduledClusterTimeSyncExecutor.scheduleAtFixedRate(
                        this::disseminateTime,
                        0,
                        1,
                        TimeUnit.SECONDS
                );
            } catch (RejectedExecutionException ignored) {
                // Scheduler was stopped, it is ok.
            }
        }

        void disseminateTime() {
            if (!busyLock.enterBusy()) {
                // Shutting down.
                return;
            }

            try {
                HybridTimestamp now = clock.now();

                service.syncTime(now);
            } finally {
                busyLock.leaveBusy();
            }
        }

        void stop() {
            IgniteUtils.shutdownAndAwaitTermination(scheduledClusterTimeSyncExecutor, 1, TimeUnit.SECONDS);
        }
    }
}
