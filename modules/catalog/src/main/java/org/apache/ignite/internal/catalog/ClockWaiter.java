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

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.ClockUpdateListener;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Allows to wait for the supplied clock to reach a required timesdtamp. It only uses the clock itself,
 * no SafeTime mechanisms are involved.
 */
public class ClockWaiter implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ClockWaiter.class);

    private final String nodeName;
    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean(false);

    private final PendingComparableValuesTracker<Long, Void> nowTracker = new PendingComparableValuesTracker<>(
            HybridTimestamp.MIN_VALUE.longValue()
    );

    private final ClockUpdateListener updateListener = this::onUpdate;

    private volatile ScheduledExecutorService scheduler;

    public ClockWaiter(String nodeName, HybridClock clock) {
        this.nodeName = nodeName;
        this.clock = clock;
    }

    @Override
    public void start() {
        clock.addUpdateListener(updateListener);

        scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(nodeName + "-clock-waiter", LOG));
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        clock.removeUpdateListener(updateListener);

        nowTracker.close();

        // We do shutdownNow() right away without doing usual shutdown()+awaitTermination() because
        // this would make us wait till all the scheduled tasks get executed (which might take a lot
        // of time). An alternative would be to track all ScheduledFutures (which are not same as the
        // user-facing futures we return from the tracker), but we don't need them for anything else,
        // so it's simpler to just use shutdownNow().
        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void onUpdate(long newTs) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            nowTracker.update(newTs, null);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Wait for the clock to reach the given timestamp.
     *
     * @param targetTimestamp Timestamp to wait for.
     * @return A future that completes when the timestamp is reached by the clock's time.
     */
    public CompletableFuture<Void> waitFor(HybridTimestamp targetTimestamp) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return doWaitFor(targetTimestamp);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> doWaitFor(HybridTimestamp targetTimestamp) {
        CompletableFuture<Void> future = nowTracker.waitFor(targetTimestamp.longValue());

        ScheduledFuture<?> scheduledFuture;

        if (!future.isDone()) {
            // This triggers a clock update.
            HybridTimestamp now = clock.now();

            if (targetTimestamp.compareTo(now) <= 0) {
                assert future.isDone();

                scheduledFuture = null;
            } else {
                // Adding 1 to account for a possible non-null logical part of the targetTimestamp.
                long millisToWait = targetTimestamp.getPhysical() - now.getPhysical() + 1;

                scheduledFuture = scheduler.schedule(this::triggerClockUpdate, millisToWait, TimeUnit.MILLISECONDS);
            }
        } else {
            scheduledFuture = null;
        }

        return future.handle((res, ex) -> {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }

            if (ex != null) {
                // Let's replace a TrackerClosedException with a CancellationException as the latter makes more sense for the clients.
                if (ex instanceof TrackerClosedException) {
                    throw new CancellationException();
                } else {
                    throw new CompletionException(ex);
                }
            }

            return res;
        });
    }

    private void triggerClockUpdate() {
        clock.now();
    }
}
