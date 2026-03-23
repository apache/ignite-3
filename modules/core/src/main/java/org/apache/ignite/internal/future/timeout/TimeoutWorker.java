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

package org.apache.ignite.internal.future.timeout;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.jetbrains.annotations.Nullable;

/**
 * Timeout object worker.
 */
public class TimeoutWorker extends IgniteWorker {
    /** Worker sleep interval. */
    private final long sleepInterval = getSleepInterval();

    /** Active operations. */
    public final ConcurrentMap<Long, TimeoutObject<?>> requestsMap;

    /** Closure to process throwables in the worker thread. */
    @Nullable
    private final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance this runnable is used in.
     * @param name Worker name. Note that in general thread name and worker (runnable) name are two different things. The same
     *         worker can be executed by multiple threads and therefore for logging and debugging purposes we separate the two.
     * @param requestsMap Active operations.
     * @param failureProcessor Closure to process throwables in the worker thread.
     */
    public TimeoutWorker(
            IgniteLogger log,
            String igniteInstanceName,
            String name,
            ConcurrentMap requestsMap,
            @Nullable FailureProcessor failureProcessor
    ) {
        super(log, igniteInstanceName, name);

        this.requestsMap = requestsMap;
        this.failureProcessor = failureProcessor;
    }

    @Override
    protected void body() {
        try {
            TimeoutObject<?> timeoutObject;

            while (!isCancelled()) {
                long now = coarseCurrentTimeMillis();

                for (Entry<Long, TimeoutObject<?>> entry : requestsMap.entrySet()) {
                    updateHeartbeat();

                    timeoutObject = entry.getValue();

                    assert timeoutObject != null : "Unexpected null in timeout operation map.";

                    if (timeoutObject.endTime() > 0 && now > timeoutObject.endTime()) {
                        CompletableFuture<?> fut = timeoutObject.future();

                        if (!fut.isDone()) {
                            requestsMap.remove(entry.getKey(), timeoutObject);

                            fut.completeExceptionally(new TimeoutException(timeoutObject.describe()));
                        }
                    }
                }

                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    log.info("The timeout worker was interrupted, probably the worker is stopping.");
                }

                updateHeartbeat();
            }

        } catch (Throwable t) {
            if (failureProcessor != null) {
                failureProcessor.process(new FailureContext(FailureType.SYSTEM_WORKER_TERMINATION, t));
            } else {
                log.error("Timeout worker failed and can't process the timeouts any longer [worker={}].", t, name());
            }
        }
    }

    public static long getSleepInterval() {
        return getLong("IGNITE_TIMEOUT_WORKER_SLEEP_INTERVAL", 500);
    }
}
