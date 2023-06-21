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
import java.util.concurrent.atomic.AtomicReference;
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

    private final PendingComparableValuesTracker<HybridTimestamp, Void> nowTracker = new PendingComparableValuesTracker<>(
            new HybridTimestamp(1, 0)
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

        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void onUpdate(HybridTimestamp newTs) {
        nowTracker.update(newTs, null);
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
        CompletableFuture<Void> future = nowTracker.waitFor(targetTimestamp);

        AtomicReference<ScheduledFuture<?>> scheduledFutureRef = new AtomicReference<>();

        if (!future.isDone()) {
            // This triggers a clock update.
            HybridTimestamp now = clock.now();

            if (targetTimestamp.compareTo(now) <= 0) {
                assert future.isDone();
            } else {
                long millisToWait = targetTimestamp.getPhysical() - now.getPhysical()
                        + (Math.max(targetTimestamp.getLogical() - now.getLogical(), 0) > 0 ? 1 : 0)
                        // Adding 1 to account for a possible non-null logical part of the targetTimestamp.
                        + 1;

                ScheduledFuture<?> scheduledFuture = scheduler.schedule(this::triggerClockUpdate, millisToWait, TimeUnit.MILLISECONDS);
                scheduledFutureRef.set(scheduledFuture);
            }
        }

        return future.handle((res, ex) -> {
            ScheduledFuture<?> scheduledFuture = scheduledFutureRef.get();

            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }

            if (ex != null) {
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
