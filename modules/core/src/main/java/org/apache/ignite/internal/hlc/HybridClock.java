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

package org.apache.ignite.internal.hlc;

/**
 * A Hybrid Logical Clock.
 */
public interface HybridClock {
    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp.
     */
    long nowLong();

    /**
     * Creates a timestamp for new event.
     *
     * @return The hybrid timestamp.
     */
    HybridTimestamp now();

    /**
     * Advances the clock in accordance with the request time. If the request time is ahead of the clock,
     * the clock is advanced to the tick that is next to the request time; otherwise, it's advanced to the tick
     * that is next to the local time.
     *
     * @param requestTime Timestamp from request.
     * @return New local hybrid timestamp that is on the clock (it is ahead of both the old clock time and the request time).
     */
    HybridTimestamp update(HybridTimestamp requestTime);

    /**
     * Adds an update listener to self.
     *
     * @param listener Update listener to add.
     */
    void addUpdateListener(ClockUpdateListener listener);

    /**
     * Removes an update listener from self.
     *
     * @param listener Listener to remove.
     */
    void removeUpdateListener(ClockUpdateListener listener);
}
