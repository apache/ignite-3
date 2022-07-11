/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.hlc;

import org.apache.ignite.internal.tostring.S;

/**
 * A Hybrid Logical Clock fo tests.
 */
public class TestHybridClock implements HybridClock {
    /** Physical time provider. */
    private final PhysicalTimeProvider physicalTimeProvider;
    /** Latest timestamp. */
    private HybridTimestamp latestTime;

    /**
     * The constructor which initializes the latest time to current time by system clock.
     */
    public TestHybridClock(PhysicalTimeProvider physicalTimeProvider) {
        this.physicalTimeProvider = physicalTimeProvider;

        this.latestTime = new HybridTimestamp(physicalTimeProvider.getPhysicalTime(), 0);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized HybridTimestamp now() {
        long currentTimeMillis = physicalTimeProvider.getPhysicalTime();

        if (latestTime.getPhysical() >= currentTimeMillis) {
            latestTime = latestTime.addTicks(1);
        } else {
            latestTime = new HybridTimestamp(currentTimeMillis, 0);
        }

        return latestTime;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized HybridTimestamp tick(HybridTimestamp requestTime) {
        HybridTimestamp now = new HybridTimestamp(physicalTimeProvider.getPhysicalTime(), -1);

        latestTime = HybridTimestamp.max(now, requestTime, latestTime);

        latestTime = latestTime.addTicks(1);

        return latestTime;
    }

    /** Physical time provider.
     *
     * @return Physical time provider.
     */
    public PhysicalTimeProvider getPhysicalTimeProvider() {
        return physicalTimeProvider;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TestHybridClock.class, this);
    }
}
