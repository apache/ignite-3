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

package org.apache.ignite.internal.raft.service;

import org.apache.ignite.internal.raft.Peer;

/**
 * The class represents a leader metadata.
 */
public class LeaderMetadata {
    /** Leader peer. */
    private final Peer leader;

    /** Corresponding term to the leader. */
    private final long term;

    /** Index on the moment to formed the metadata. */
    private final long index;

    /**
     * The constructor.
     *
     * @param leader Leader.
     * @param term Leader term.
     * @param index Raft index.
     */
    public LeaderMetadata(Peer leader, long term, long index) {
        this.leader = leader;
        this.term = term;
        this.index = index;
    }

    /**
     * Get a leader peer.
     *
     * @return Leader peer.
     */
    public Peer getLeader() {
        return leader;
    }

    /**
     * Gets a term corresponding to the leader.
     *
     * @return Leader term.
     */
    public long getTerm() {
        return term;
    }

    /**
     * Gets an index on the moment to formed the metadata.
     *
     * @return Raft index.
     */
    public long getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "LeaderMetadata{" +
                "leader=" + leader +
                ", term=" + term +
                ", index=" + index +
                '}';
    }
}
