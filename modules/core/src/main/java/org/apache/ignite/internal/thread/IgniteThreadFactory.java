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

package org.apache.ignite.internal.thread;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.TestOnly;

/**
 * Thread factory that produces {@link IgniteThread}s with node name, prefix, allowed operations.
 */
public class IgniteThreadFactory implements ThreadFactory {
    /** Thread prefix (when computing final thread name, no dash will be added to the prefix). */
    private final String prefix;

    /** Thread counter. */
    private final AtomicInteger counter = new AtomicInteger(0);

    /** Thread daemon flag. */
    private final boolean daemon;

    /** Exception handler. */
    private final Thread.UncaughtExceptionHandler exHnd;

    private final ThreadOperation[] allowedOperations;

    /**
     * Constructor.
     */
    private IgniteThreadFactory(String nodeName, String poolName, boolean daemon, IgniteLogger log, ThreadOperation[] allowedOperations) {
        this(IgniteThread.threadPrefix(nodeName, poolName), daemon, log, allowedOperations);
    }

    /**
     * Constructor.
     */
    private IgniteThreadFactory(String prefix, boolean daemon, IgniteLogger log, ThreadOperation[] allowedOperations) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.daemon = daemon;
        this.exHnd = new LogUncaughtExceptionHandler(log);
        this.allowedOperations = allowedOperations;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new IgniteThread(prefix + counter.getAndIncrement(), r, allowedOperations);

        t.setDaemon(this.daemon);
        t.setUncaughtExceptionHandler(exHnd);

        return t;
    }

    /**
     * Creates a non-daemon thread factory based on a node's name and a name of the pool.
     *
     * @param nodeName Node name.
     * @param poolName Pool name.
     * @param logger Logger.
     * @param allowedOperations Operations that are allowed to be executed on threads produced by this factory.
     * @return Thread factory.
     */
    public static ThreadFactory create(String nodeName, String poolName, IgniteLogger logger, ThreadOperation... allowedOperations) {
        return create(nodeName, poolName, false, logger, allowedOperations);
    }

    /**
     * Creates a thread factory based on a node's name and a name of the pool.
     *
     * @param nodeName Node name.
     * @param poolName Pool name.
     * @param daemon Whether threads created by the factory should be daemon or not.
     * @param logger Logger.
     * @param allowedOperations Operations that are allowed to be executed on threads produced by this factory.
     * @return Thread factory.
     */
    public static ThreadFactory create(
            String nodeName,
            String poolName,
            boolean daemon,
            IgniteLogger logger,
            ThreadOperation... allowedOperations
    ) {
        return new IgniteThreadFactory(nodeName, poolName, daemon, logger, allowedOperations);
    }

    @TestOnly
    public static ThreadFactory withPrefix(String prefix, IgniteLogger logger, ThreadOperation... allowedOperations) {
        return new IgniteThreadFactory(prefix, false, logger, allowedOperations);
    }
}
