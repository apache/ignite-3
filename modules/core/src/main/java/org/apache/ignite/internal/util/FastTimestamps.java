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

package org.apache.ignite.internal.util;

/**
 * Provides access to fast (low-latency), but coarse-grained timestamps.
 */
public class FastTimestamps {
    private static volatile long coarseCurrentTimeMillis = System.currentTimeMillis();

    private static final long UPDATE_INTERVAL_MS = 10;

    static {
        startUpdater();
    }

    private static void startUpdater() {
        Thread updater = new Thread("FastTimestamps updater") {
            /** {@inheritDoc} */
            @Override
            public void run() {
                while (true) {
                    coarseCurrentTimeMillis = System.currentTimeMillis();
                    try {
                        Thread.sleep(UPDATE_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };

        updater.setDaemon(true);
        updater.setPriority(10);
        updater.start();
    }

    /**
     * Returns number of milliseconds passed since Unix Epoch (1970-01-01) with a coarse resolution.
     * The resolution is currently 10ms. This method works a lot faster (2 ns vs 11000 ns on a developer machine)
     * than {@link System#currentTimeMillis()}.
     *
     * @return number of milliseconds passed since Unix Epoch (1970-01-01) with a coarse resolution
     */
    public static long coarseCurrentTimeMillis() {
        return coarseCurrentTimeMillis;
    }

    private FastTimestamps() {
    }
}
