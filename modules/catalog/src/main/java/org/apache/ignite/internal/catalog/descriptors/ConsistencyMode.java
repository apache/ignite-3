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

package org.apache.ignite.internal.catalog.descriptors;

/**
 * Specifies the consistency mode of the zone, determining how the system balances data consistency and availability.
 */
public enum ConsistencyMode {
    /**
     * Ensures strong consistency by requiring a majority of nodes for operations. Partitions become unavailable if the majority of assigned
     * nodes are lost.
     */
    STRONG_CONSISTENCY(0),

    /**
     * Prioritizes availability over strict consistency, allowing partitions to remain available for read-write operations even when the
     * majority of assigned nodes are offline. The system reconfigures the RAFT group to include only the available nodes without changing
     * the assignments.
     */
    HIGH_AVAILABILITY(1);

    private static final ConsistencyMode[] VALS = new ConsistencyMode[values().length];

    private final int id;

    ConsistencyMode(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    static {
        for (ConsistencyMode mode : values()) {
            assert VALS[mode.id] == null : "Found duplicate id " + mode.id;

            VALS[mode.id()] = mode;
        }
    }

    /** Returns index status by identifier. */
    static ConsistencyMode forId(int id) {
        if (id >= 0 && id < VALS.length) {
            return VALS[id];
        }

        throw new IllegalArgumentException("Incorrect consistency mode identifier: " + id);
    }
}
