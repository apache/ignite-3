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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;

/** Extension for creating {@link IgniteCheckpointThread}. */
class IgniteCheckpointThreadFactory extends IgniteThreadFactory {
    /** Constructor. */
    private IgniteCheckpointThreadFactory(
            String nodeName,
            String poolName,
            boolean daemon,
            IgniteLogger log,
            ThreadOperation[] allowedOperations
    ) {
        super(nodeName, poolName, daemon, log, allowedOperations);
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
    public static IgniteCheckpointThreadFactory create(
            String nodeName,
            String poolName,
            boolean daemon,
            IgniteLogger logger,
            ThreadOperation... allowedOperations
    ) {
        return new IgniteCheckpointThreadFactory(nodeName, poolName, daemon, logger, allowedOperations);
    }

    @Override
    protected IgniteCheckpointThread createIgniteThread(String finalName, Runnable r, ThreadOperation... allowedOperations) {
        return new IgniteCheckpointThread(finalName, r, allowedOperations);
    }
}
