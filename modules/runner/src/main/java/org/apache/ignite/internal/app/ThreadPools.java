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

package org.apache.ignite.internal.app;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.LogUncaughtExceptionHandler;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Component that hosts thread pools which do not belong to a certain component and which are global to an Ignite instance.
 */
public class ThreadPools implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(ThreadPools.class);

    private final StripedThreadPoolExecutor partitionOperationsExecutor;

    /**
     * Constructor.
     */
    public ThreadPools(String nodeName) {
        partitionOperationsExecutor = new StripedThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 25),
                NamedThreadFactory.threadPrefix(nodeName, "partition-operations"),
                new LogUncaughtExceptionHandler(LOG),
                false,
                0
        );
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(partitionOperationsExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns the executor of partition operations.
     */
    public StripedThreadPoolExecutor partitionOperationsExecutor() {
        return partitionOperationsExecutor;
    }
}
