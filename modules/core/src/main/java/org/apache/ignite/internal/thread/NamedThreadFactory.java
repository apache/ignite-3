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

/**
 * Named thread factory with prefix.
 */
public class NamedThreadFactory implements ThreadFactory {
    /** Thread prefix. */
    private final String prefix;

    /** Thread counter. */
    private final AtomicInteger counter = new AtomicInteger(0);

    /** Thread daemon flag. */
    private final boolean daemon;

    /** Exception handler. */
    private final Thread.UncaughtExceptionHandler exHnd;

    /**
     * Constructor.
     *
     * @param prefix Thread name prefix.
     * @param log A logger instance is used to create {@link LogUncaughtExceptionHandler}.
     */
    public NamedThreadFactory(String prefix, IgniteLogger log) {
        this(prefix, false, log);
    }

    /**
     * Constructor.
     *
     * @param prefix Thread name prefix.
     * @param daemon Daemon flag.
     * @param log A logger instance is used to create {@link LogUncaughtExceptionHandler}.
     */
    public NamedThreadFactory(String prefix, boolean daemon, IgniteLogger log) {
        this(prefix, daemon, new LogUncaughtExceptionHandler(log));
    }

    /**
     * Constructor.
     *
     * @param prefix Thread name prefix.
     * @param daemon Daemon flag.
     * @param exHnd Uncaught exception handler.
     */
    public NamedThreadFactory(String prefix, boolean daemon, Thread.UncaughtExceptionHandler exHnd) {
        super();
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.daemon = daemon;
        this.exHnd = Objects.requireNonNull(exHnd, "exHnd");
    }

    /** {@inheritDoc} */
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);

        t.setDaemon(this.daemon);
        t.setUncaughtExceptionHandler(exHnd);
        t.setName(this.prefix + counter.getAndIncrement());

        return t;
    }

    /**
     * Create prefix for thread name.
     */
    public static String threadPrefix(String nodeName, String poolName) {
        return IgniteThread.threadPrefix(nodeName, poolName);
    }

    /**
     * Creates a thread factory based on a node's name and a name of the pool.
     *
     * @param nodeName Node name.
     * @param poolName Pool name.
     * @param logger Logger.
     * @return Thread factory.
     */
    public static NamedThreadFactory create(String nodeName, String poolName, IgniteLogger logger) {
        return new NamedThreadFactory(threadPrefix(nodeName, poolName), logger);
    }

    /**
     * Creates a thread factory based on a node's name and a name of the pool.
     *
     * @param nodeName Node name.
     * @param poolName Pool name.
     * @param daemon Whether threads created by the factory should be daemon or not.
     * @param logger Logger.
     * @return Thread factory.
     */
    public static NamedThreadFactory create(String nodeName, String poolName, boolean daemon, IgniteLogger logger) {
        return new NamedThreadFactory(threadPrefix(nodeName, poolName), daemon, logger);
    }
}
