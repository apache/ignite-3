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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Lazily creates and manages the lifecycle of a single streamer flush {@link ScheduledExecutorService}.
 *
 * <p>The executor is created on first call to {@link #get()} and shut down on {@link #stop}.
 * {@link #beforeStop()} must be called before {@link #stop} to prevent new executor creation during shutdown.
 */
public final class StreamerFlushExecutorFactory {
    private static final IgniteLogger LOG = Loggers.forClass(StreamerFlushExecutorFactory.class);

    private final String nodeName;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    @Nullable
    private ScheduledExecutorService executor;

    /**
     * Constructor.
     *
     * @param nodeName Node name used for thread naming.
     */
    public StreamerFlushExecutorFactory(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * Returns the executor, creating it lazily if needed.
     *
     * @return Streamer flush executor.
     * @throws IgniteException If the node is stopping.
     */
    public synchronized ScheduledExecutorService get() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            if (executor == null) {
                executor = Executors.newSingleThreadScheduledExecutor(
                        IgniteThreadFactory.create(nodeName, "streamer-flush-executor", LOG, STORAGE_WRITE));
            }

            return executor;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Blocks the busy lock to prevent new executor creation during shutdown.
     */
    public void beforeStop() {
        busyLock.block();
    }

    /**
     * Shuts down the executor if it was created.
     *
     * @param timeoutSeconds Timeout in seconds to wait for termination.
     */
    public void stop(int timeoutSeconds) {
        ScheduledExecutorService local;

        synchronized (this) {
            local = this.executor;
        }

        shutdownAndAwaitTermination(local, timeoutSeconds, TimeUnit.SECONDS);
    }
}
