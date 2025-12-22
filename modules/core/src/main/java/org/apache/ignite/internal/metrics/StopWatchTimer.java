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

package org.apache.ignite.internal.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Class for getting the duration of an operation.
 *
 * <p>Not thread safe.</p>
 */
public class StopWatchTimer {
    private long startNanos;

    private long endNanos;

    /** Starts timer. */
    public void start() {
        startNanos = System.nanoTime();
    }

    /** Ends timer. */
    public void end() {
        endNanos = System.nanoTime();
    }

    /** Returns the start time of the operation in nanos. */
    public long startNanos() {
        return startNanos;
    }

    /** Returns the end time of the operation in nanos. */
    public long endNanos() {
        return startNanos;
    }

    /** Returns the duration in the specified time unit. */
    public long duration(TimeUnit timeUnit) {
        return timeUnit.convert(endNanos - startNanos, TimeUnit.NANOSECONDS);
    }
}
