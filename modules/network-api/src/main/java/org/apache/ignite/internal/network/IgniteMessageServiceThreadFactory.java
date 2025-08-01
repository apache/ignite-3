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

package org.apache.ignite.internal.network;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.ThreadOperation;

/** {@link IgniteMessageServiceThread} factory. */
public class IgniteMessageServiceThreadFactory implements ThreadFactory {
    /** Thread prefix (when computing final thread name, no dash will be added to the prefix). */
    private final String prefix;

    /** Thread counter. */
    private final AtomicInteger counter = new AtomicInteger(0);

    private final Thread.UncaughtExceptionHandler exceptionHandler;

    private final ThreadOperation[] allowedOperations;

    private IgniteMessageServiceThreadFactory(String prefix, ThreadOperation[] allowedOperations, IgniteLogger log) {
        this.prefix = prefix;
        this.allowedOperations = allowedOperations;

        this.exceptionHandler = new LogUncaughtExceptionHandler(log);
    }

    @Override
    public Thread newThread(Runnable r) {
        var t = new IgniteMessageServiceThread(prefix + counter.getAndIncrement(), r, allowedOperations);

        t.setUncaughtExceptionHandler(exceptionHandler);

        return t;
    }

    /**
     * Creates a non-daemon thread factory based on a node's name and a name of the pool.
     *
     * @param nodeName Node name.
     * @param poolName Pool name.
     * @param logger Logger.
     * @param allowedOperations Operations that are allowed to be executed on threads produced by this factory.
     */
    public static IgniteMessageServiceThreadFactory create(
            String nodeName,
            String poolName,
            IgniteLogger logger,
            ThreadOperation... allowedOperations
    ) {
        return new IgniteMessageServiceThreadFactory(IgniteThread.threadPrefix(nodeName, poolName), allowedOperations, logger);
    }
}
