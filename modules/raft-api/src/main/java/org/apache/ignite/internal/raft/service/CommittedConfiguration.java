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

import java.util.List;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * A committed RAFT configuration with index and term at which it was committed.
 */
public class CommittedConfiguration {
    private final long index;
    private final long term;

    @IgniteToStringInclude
    private final List<String> peers;
    @IgniteToStringInclude
    private final List<String> learners;

    @Nullable
    @IgniteToStringInclude
    private final List<String> oldPeers;
    @Nullable
    @IgniteToStringInclude
    private final List<String> oldLearners;

    /**
     * Creates a new instance.
     */
    public CommittedConfiguration(long index, long term, List<String> peers, List<String> learners, @Nullable List<String> oldPeers,
            @Nullable List<String> oldLearners) {
        this.index = index;
        this.term = term;
        this.peers = List.copyOf(peers);
        this.learners = List.copyOf(learners);
        this.oldPeers = oldPeers == null ? null : List.copyOf(oldPeers);
        this.oldLearners = oldLearners == null ? null : List.copyOf(oldLearners);
    }

    /**
     * Returns RAFT index corresponding to this configuration entry. Always positive.
     *
     * @return RAFT index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns RAFT term corresponding to this configuration entry. Always positive.
     *
     * @return RAFT term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns peers of the current configuration.
     *
     * @return Peers.
     */
    public List<String> peers() {
        return peers;
    }

    /**
     * Returns learners of the current configuration.
     *
     * @return Learners.
     */
    public List<String> learners() {
        return learners;
    }

    /**
     * Returns old peers of the current configuration.
     *
     * @return Old peers.
     */
    @Nullable
    public List<String> oldPeers() {
        return oldPeers;
    }

    /**
     * Returns old learners of the current configuration.
     *
     * @return Old learners.
     */
    @Nullable
    public List<String> oldLearners() {
        return oldLearners;
    }

    @Override
    public String toString() {
        return S.toString(CommittedConfiguration.class, this);
    }
}
