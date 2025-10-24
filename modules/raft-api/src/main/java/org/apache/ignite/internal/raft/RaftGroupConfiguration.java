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

package org.apache.ignite.internal.raft;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * A POJO for a RAFT group configuration with index and term at which it was committed,
 * could be used by other modules. Not used by the RAFT module itself.
 */
public class RaftGroupConfiguration implements Serializable {
    private static final long serialVersionUID = 0;

    /** Unknown index value. */
    public static final long UNKNOWN_INDEX = -1L;

    /** Unknown term value. */
    public static final long UNKNOWN_TERM = -1L;

    private final long index;
    private final long term;
    private final long sequenceToken;
    private final long oldSequenceToken;

    @IgniteToStringInclude
    private final List<String> peers;
    @IgniteToStringInclude
    private final List<String> learners;

    @IgniteToStringInclude
    private final @Nullable List<String> oldPeers;
    @IgniteToStringInclude
    private final @Nullable List<String> oldLearners;

    /**
     * Creates a new instance.
     */
    public RaftGroupConfiguration(
            long index,
            long term,
            long sequenceToken,
            long oldSequenceToken,
            Collection<String> peers,
            Collection<String> learners,
            @Nullable Collection<String> oldPeers,
            @Nullable Collection<String> oldLearners
    ) {
        this.index = index;
        this.term = term;
        this.sequenceToken = sequenceToken;
        this.oldSequenceToken = oldSequenceToken;
        this.peers = List.copyOf(peers);
        this.learners = List.copyOf(learners);
        this.oldPeers = oldPeers == null ? null : List.copyOf(oldPeers);
        this.oldLearners = oldLearners == null ? null : List.copyOf(oldLearners);
    }

    /**
     * Returns RAFT index corresponding to this configuration entry.
     *
     * @return RAFT index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns RAFT term corresponding to this configuration entry.
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
    public @Nullable List<String> oldPeers() {
        return oldPeers;
    }

    /**
     * Returns old learners of the current configuration.
     *
     * @return Old learners.
     */
    public @Nullable List<String> oldLearners() {
        return oldLearners;
    }

    /**
     * Returns {@code true} if no information about old peers/learners is available.
     */
    public boolean isStable() {
        return oldPeers == null;
    }

    public long sequenceToken() {
        return sequenceToken;
    }

    public long oldSequenceToken() {
        return oldSequenceToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaftGroupConfiguration that = (RaftGroupConfiguration) o;
        return index == that.index && term == that.term && sequenceToken == that.sequenceToken
                && oldSequenceToken == that.oldSequenceToken
                && Objects.equals(peers, that.peers) && Objects.equals(learners, that.learners)
                && Objects.equals(oldPeers, that.oldPeers) && Objects.equals(oldLearners, that.oldLearners);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term, sequenceToken, oldSequenceToken, peers, learners, oldPeers, oldLearners);
    }

    @Override
    public String toString() {
        return S.toString(RaftGroupConfiguration.class, this);
    }
}
