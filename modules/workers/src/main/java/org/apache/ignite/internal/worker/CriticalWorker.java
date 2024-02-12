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

package org.apache.ignite.internal.worker;

/**
 * A worker that performs some critical work and hence needs to be monitored for being blocked.
 */
public interface CriticalWorker {
    /**
     * Heartbeat timestamp 'value' corresponding to the state in which the thread is not running any computations during
     * which it should be monitored. This means that it either executes an (expected) blocking operation, or
     * it just waits for work to be submitted.
     */
    long NOT_MONITORED = Long.MAX_VALUE;

    /**
     * Returns ID of a thread on which the worker currently executes. Might change from time to time.
     */
    long threadId();

    /**
     * Return heartbeat timestamp (that is, the last value from {@link System#nanoTime()} that was saved by the worker.
     * It is the last moment when it was guaranteed to be alive (and not stuck in an infinite loop, for example).
     */
    long heartbeatNanos();
}
