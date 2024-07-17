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
import org.jetbrains.annotations.Nullable;

/**
 * Class representing a Raft group leader and its term.
 */
public class LeaderWithTerm {
    /** The instance determines a state where the leader is undefined. */
    public static LeaderWithTerm NO_LEADER = new LeaderWithTerm(null, -1);

    @Nullable
    private final Peer leader;

    private final long term;

    public LeaderWithTerm(Peer leader, long term) {
        this.leader = leader;
        this.term = term;
    }

    /**
     * Checks if there is any useful information.
     *
     * @return True if the instance does not contain useful data, false otherwise.
     */
    public boolean isEmpty() {
        return leader == null;
    }

    public @Nullable Peer leader() {
        return leader;
    }

    public long term() {
        return term;
    }
}
