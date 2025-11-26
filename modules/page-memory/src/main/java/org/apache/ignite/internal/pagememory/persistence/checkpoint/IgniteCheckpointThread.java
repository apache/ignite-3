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

import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.util.worker.IgniteWorker;

/** Extension for {@link CheckpointReadWriteLock}. */
class IgniteCheckpointThread extends IgniteThread {
    /**
     * Creates thread with given worker.
     *
     * @param worker Worker to create thread with.
     */
    IgniteCheckpointThread(IgniteWorker worker) {
        super(worker);
    }

    /**
     * Creates thread with given name for a given Ignite instance.
     *
     * @param nodeName Name of the Ignite instance this thread is created for.
     * @param threadName Name of thread (will be added to the node name to form final name).
     * @param r Runnable to execute.
     * @param allowedOperations Operations which this thread allows to execute.
     */
    IgniteCheckpointThread(String nodeName, String threadName, Runnable r, ThreadOperation... allowedOperations) {
        super(nodeName, threadName, r, allowedOperations);
    }

    /**
     * Creates thread with given name.
     *
     * @param finalName Name of thread.
     * @param r Runnable to execute.
     * @param allowedOperations Operations which this thread allows to execute.
     */
    IgniteCheckpointThread(String finalName, Runnable r, ThreadOperation... allowedOperations) {
        super(finalName, r, allowedOperations);
    }
}
