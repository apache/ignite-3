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

package org.apache.ignite.internal.raft;

import org.apache.ignite.raft.jraft.util.Utils;

/**
 * Utility class for retry timeout calculations.
 */
class RetryUtils {

    /**
     * Computes a monotonic-time deadline based on the provided timeout.
     *
     * <p>The returned value is expressed in the same time domain as
     * {@link Utils#monotonicMs()} and is intended for comparisons against
     * that value (for example, {@code now >= deadline}).
     * 
     * <p>Timeout semantics:
     * <ul>
     *     <li>{@code timeoutMillis < 0} or {@code timeoutMillis == Long.MAX_VALUE}
     *     — the deadline is unbounded and {@code Long.MAX_VALUE} is returned.</li>
     *     <li>{@code timeoutMillis >= 0} — the deadline is computed as
     *     {@code Utils.monotonicMs() + timeoutMillis}.</li>
     * </ul>
     *
     * @param timeoutMillis timeout in milliseconds controlling the retry window
     * @return a monotonic-time deadline in milliseconds, or {@code Long.MAX_VALUE}
     *         if retries are unbounded
     */
    static long monotonicMsAfter(long timeoutMillis) {
        if (timeoutMillis == Long.MAX_VALUE || timeoutMillis < 0) {
            // Infinite retry.
            return Long.MAX_VALUE;
        } else {
            return Utils.monotonicMs() + timeoutMillis;
        }
    }
}
