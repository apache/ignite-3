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

package org.apache.ignite.internal.partition.replicator.network.disaster;

import org.apache.ignite.raft.jraft.core.State;

/**
 * Enum for states of local partitions.
 */
public enum LocalPartitionStateEnum {
    /** This state might be used when partition is not yet started, or it's already stopping, for example. */
    UNAVAILABLE,

    /** Alive partition with a healthy state machine. */
    HEALTHY,

    /** Partition is starting right now. */
    INITIALIZING,

    /** Partition is installing a Raft snapshot from the leader. */
    INSTALLING_SNAPSHOT,

    /** Partition is catching up, meaning that it's not replicated part of the log yet. */
    CATCHING_UP,

    /** Partition is in broken state, usually it means that its state machine thrown an exception. */
    BROKEN;

    /** Cached array with all enum values. */
    private static final LocalPartitionStateEnum[] VALUES = values();

    /** Converts internal raft node state into public local partition state. */
    public static LocalPartitionStateEnum convert(State raftNodeState) {
        switch (raftNodeState) {
            case STATE_LEADER:
            case STATE_TRANSFERRING:
            case STATE_CANDIDATE:
            case STATE_FOLLOWER:
                return HEALTHY;

            case STATE_ERROR:
                return BROKEN;

            case STATE_UNINITIALIZED:
                return INITIALIZING;

            case STATE_SHUTTING:
            case STATE_SHUTDOWN:
            case STATE_END:
                return UNAVAILABLE;

            default:
                // Unrecognized state, better safe than sorry.
                return BROKEN;
        }
    }

    /**
     * Returns the enumerated value from its ordinal.
     *
     * @param ordinal Ordinal of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by ordinal.
     */
    public static LocalPartitionStateEnum fromOrdinal(int ordinal) throws IllegalArgumentException {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("No enum constant from ordinal: " + ordinal);
        }

        return VALUES[ordinal];
    }
}
