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

package org.apache.ignite.internal.compute.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ComputeThreadPoolExecutorTest {
    private ComputeThreadPoolExecutor computeThreadPoolExecutor;
    private BlockingQueue<Runnable> workQueue;

    @BeforeEach
    public void setup() {
        workQueue = new BoundedPriorityBlockingQueue<>(() -> 5);
        computeThreadPoolExecutor = new ComputeThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, workQueue,
                IgniteThreadFactory.create("test", "compute", false, Loggers.forClass(ComputeThreadPoolExecutorTest.class)));
    }

    @Test
    public void testRemoveFromQueue2Tasks() {
        // Add 2 tasks to the executor
        computeThreadPoolExecutor.execute(longTask());
        QueueEntry<Void> longTask = longTask();
        computeThreadPoolExecutor.execute(longTask);

        // Check longTask is in workQueue
        assertEquals(1, workQueue.size());
        assertTrue(workQueue.contains(longTask));

        // Remove task from executor
        computeThreadPoolExecutor.removeFromQueue(longTask);

        // Check longTask was removed from workQueue
        assertTrue(workQueue.isEmpty());
    }

    @Test
    public void testRemoveFromQueue1Task() {
        // Add 1 tasks to the executor
        computeThreadPoolExecutor.execute(longTask());

        // Check longTask is not in workQueue
        assertTrue(workQueue.isEmpty());
    }

    private static QueueEntry<Void> longTask() {
        return new QueueEntry<>(() -> {
            TimeUnit.MINUTES.sleep(1);
            return null;
        }, 0);
    }
}
