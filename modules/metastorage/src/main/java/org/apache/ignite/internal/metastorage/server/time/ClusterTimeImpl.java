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
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Cluster time implementation with additional methods to adjust time and update safe time.
 */
public class ClusterTimeImpl implements ClusterTime {
    private final IgniteSpinBusyLock busyLock;

    private volatile @Nullable LeaderTimer leaderTimer;

    private final HybridClock clock;

    private final PendingComparableValuesTracker<HybridTimestamp> safeTime;

    /**
     * Constructor.
     *
     * @param busyLock Busy lock.
     * @param clock Node's hybrid clock.
     */
    public ClusterTimeImpl(IgniteSpinBusyLock busyLock, HybridClock clock) {
        this.busyLock = busyLock;
        this.clock = clock;
        this.safeTime = new PendingComparableValuesTracker<>(clock.now());
    }

    /**
     * Starts sync time scheduler.
     *
     * @param service MetaStorage service that is used by scheduler to sync time.
     */
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

    /**
     * Stops sync time scheduler if it exists.
     */
    public void stopLeaderTimer() {
        LeaderTimer timer = leaderTimer;

        if (timer != null) {
            timer.stop();

            leaderTimer = null;
        }
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

        private LeaderTimer(MetaStorageServiceImpl service) {
            this.service = service;
        }

        void start() {
            schedule();
        }

        private void schedule() {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-19199 Only propagate safe time when ms is idle
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
            // TODO: https://issues.apache.org/jira/browse/IGNITE-19199 Stop safe time propagation
        }
    }

    @TestOnly
    public HybridTimestamp currentSafeTime() {
        return this.safeTime.current();
    }
}
