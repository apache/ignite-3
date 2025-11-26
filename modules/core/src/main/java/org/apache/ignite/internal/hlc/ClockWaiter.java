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

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;

/**
 * Allows to wait for the supplied clock to reach a required timestamp. It only uses the clock itself,
 * no SafeTime mechanisms are involved.
 */
public class ClockWaiter implements IgniteComponent {
    private final HybridClock clock;

    private final AtomicBoolean stopGuard = new AtomicBoolean(false);

    private final PendingComparableValuesTracker<Long, Void> nowTracker = new PendingComparableValuesTracker<>(
            HybridTimestamp.MIN_VALUE.longValue()
    );

    private final ClockUpdateListener updateListener = this::onUpdate;

    private final Runnable triggerClockUpdate = this::triggerTrackerUpdate;

    /** Executor on which short-lived tasks are scheduled that are needed to timely complete awaiting futures. */
    private final ScheduledExecutorService scheduler;

    /** Executor that executes completion of futures returned to the user, so it might take arbitrarily heavy operations. */
    private final ExecutorService futureExecutor;

    /**
     * Creates a new {@link ClockWaiter}.
     *
     * @param nodeName Name of the current Ignite node.
     * @param clock Clock to look at.
     * @param scheduler Executor on which short-lived tasks are scheduled that are needed to timely complete awaiting futures.
     */
    public ClockWaiter(String nodeName, HybridClock clock, ScheduledExecutorService scheduler) {
        this.clock = clock;
        this.scheduler = scheduler;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                2,
                10,
                SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(nodeName, "clock-waiter-future-executor", Loggers.forClass(ClockWaiter.class))
        );
        executor.allowCoreThreadTimeOut(true);

        futureExecutor = executor;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        clock.addUpdateListener(updateListener);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        clock.removeUpdateListener(updateListener);

        nowTracker.close(new NodeStoppingException());

        IgniteUtils.shutdownAndAwaitTermination(futureExecutor, 10, SECONDS);

        return nullCompletedFuture();
    }

    private void onUpdate(long newTs) {
        nowTracker.update(newTs, null);
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
        return doWaitFor(targetTimestamp);
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
        return future.thenRunAsync(() -> scheduledFuture.cancel(true), futureExecutor);
    }

    private void triggerTrackerUpdate() {
        runAsync(() -> onUpdate(clock.nowLong()), futureExecutor);
    }
}
