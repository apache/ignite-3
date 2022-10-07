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

package org.apache.ignite.internal.raft.server;

import org.apache.ignite.hlc.HybridClock;

/**
 * Options that are specific for replication group.
 */
public class ReplicationGroupOptions {
    /** Safe time clock. */
    private HybridClock safeTimeClock;

    /**
     * Safe time clock.
     */
    public HybridClock safeTimeClock() {
        return safeTimeClock;
    }

    /**
     * Set the safe time clock.
     *
     * @param safeTimeClock Safe time clock.
     * @return This, for chaining.
     */
    public ReplicationGroupOptions safeTimeClock(HybridClock safeTimeClock) {
        this.safeTimeClock = safeTimeClock;

        return this;
    }
}
