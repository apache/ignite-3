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
 * Used to track updates of a {@link HybridClock}: it gets notified each time the clock 'ticks', including
 * adjustments caused by external events.
 */
@FunctionalInterface
public interface ClockUpdateListener {
    /**
     * Called when the clock's current time advances.
     *
     * @param newTs New timestamp on the clock.
     */
    void onUpdate(HybridTimestamp newTs);
}
