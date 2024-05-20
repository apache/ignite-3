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

package org.apache.ignite.internal.hlc;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;

/**
 * Allows to wait for the supplied clock to reach a required timesdtamp. It only uses the clock itself,
 * no SafeTime mechanisms are involved.
 */
public class ClockWaiter implements IgniteComponent {
    private final IgniteLogger log = Loggers.forClass(ClockWaiter.class);

    private final String nodeName;
    private final HybridClock clock;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean(false);

    private final PendingComparableValuesTracker<Long, Void> nowTracker = new PendingComparableValuesTracker<>(
            HybridTimestamp.MIN_VALUE.longValue()
    );

    private final ClockUpdateListener updateListener = this::onUpdate;

    private final Runnable triggerClockUpdate = this::triggerTrackerUpdate;

    /** Executor on which short-lived tasks are scheduled that are needed to timely complete awaiting futures. */
    private volatile ScheduledExecutorService scheduler;

    /** Executor that executes completion of futures returned to the user, so it might take arbitrarily heavy operations. */
    private final ExecutorService futureExecutor;

    /**
     * Creates a new {@link ClockWaiter}.
     *
     * @param nodeName Name of the current Ignite node.
     * @param clock Clock to look at.
     */
    public ClockWaiter(String nodeName, HybridClock clock) {
        this.nodeName = nodeName;
        this.clock = clock;

        futureExecutor = new ThreadPoolExecutor(
                0,
                4,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(nodeName, "clock-waiter-future-executor", log)
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
        clock.addUpdateListener(updateListener);

        scheduler = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory.create(nodeName, "clock-waiter-scheduler", log));

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
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

        IgniteUtils.shutdownAndAwaitTermination(futureExecutor, 10, TimeUnit.SECONDS);

        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
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
     * <p>If completion of the returned future triggers some I/O operations or causes the code to block, it is highly
     * recommended to execute those completion stages on a specific thread pool to avoid the waiter's pool starvation.
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
        HybridTimestamp now = clock.now();

        if (targetTimestamp.compareTo(now) <= 0) {
            return nullCompletedFuture();
        }

        CompletableFuture<Void> future = nowTracker.waitFor(targetTimestamp.longValue());

        // Adding 1 to account for a possible non-null logical part of the targetTimestamp.
        long millisToWait = targetTimestamp.getPhysical() - now.getPhysical() + 1;

        ScheduledFuture<?> scheduledFuture = scheduler.schedule(triggerClockUpdate, millisToWait, TimeUnit.MILLISECONDS);

        // The future might be completed in a random thread, so let's move its completion execution to a special thread pool
        // because the user's code following the future completion might run arbitrarily heavy operations and we don't want
        // to put them on an innocent thread invoking now()/update() on the clock.
        return future
                .handleAsync((res, ex) -> {
                    scheduledFuture.cancel(true);

                    if (ex != null) {
                        translateTrackerClosedException(ex);
                    }

                    return res;
                }, futureExecutor);
    }

    private static void translateTrackerClosedException(Throwable ex) {
        if (ex instanceof TrackerClosedException) {
            throw new CancellationException();
        } else {
            throw new CompletionException(ex);
        }
    }

    private void triggerTrackerUpdate() {
        onUpdate(clock.nowLong());
    }
}
