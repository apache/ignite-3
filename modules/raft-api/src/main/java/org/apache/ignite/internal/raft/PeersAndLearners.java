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

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Class containing peers and learners of a Raft Group.
 */
public class PeersAndLearners {
    @IgniteToStringInclude
    private final Set<Peer> peers;

    @IgniteToStringInclude
    private final Set<Peer> learners;

    private PeersAndLearners(Collection<Peer> peers, Collection<Peer> learners) {
        this.peers = Set.copyOf(peers);
        this.learners = Set.copyOf(learners);
    }

    /**
     * Creates an instance using peers represented as their consistent IDs.
     */
    public static PeersAndLearners fromConsistentIds(Set<String> peerNames) {
        return fromConsistentIds(peerNames, Set.of());
    }

    /**
     * Creates an instance using peers and learners represented as their consistent IDs.
     *
     * <p>The main purpose of this method is to assign correct indices to Raft nodes when a peer and a learner are started on the same
     * Ignite node. In this case {@code Peer} instances will have the same consistent IDs, but the learner will have an index equal to 1
     * (having more than one peer and one learner running on the same node is currently prohibited).
     */
    public static PeersAndLearners fromConsistentIds(Set<String> peerNames, Set<String> learnerNames) {
        Set<Peer> peers = peerNames.stream().map(Peer::new).collect(toUnmodifiableSet());

        Set<Peer> learners = learnerNames.stream()
                .map(name -> {
                    // Learners can be started on the same nodes as peers. However, we can only have at most one learner and one peer on
                    // the same node.
                    int idx = peerNames.contains(name) ? 1 : 0;

                    return new Peer(name, idx);
                })
                .collect(toUnmodifiableSet());

        return new PeersAndLearners(peers, learners);
    }

    /**
     * Creates an instance using peers and learners represented as {@link Peer}s.
     */
    public static PeersAndLearners fromPeers(Collection<Peer> peers, Collection<Peer> learners) {
        assert Collections.disjoint(peers, learners);

        return new PeersAndLearners(peers, learners);
    }

    /**
     * Creates an instance using the given assignments.
     */
    public static PeersAndLearners fromAssignments(Collection<Assignment> assignments) {
        var peers = new HashSet<String>();
        var learners = new HashSet<String>();

        for (Assignment assignment : assignments) {
            if (assignment.isPeer()) {
                peers.add(assignment.consistentId());
            } else {
                learners.add(assignment.consistentId());
            }
        }

        return fromConsistentIds(peers, learners);
    }

    /**
     * Returns the set of peers.
     */
    public Set<Peer> peers() {
        return peers;
    }

    /**
     * Returns the set of learners.
     */
    public Set<Peer> learners() {
        return learners;
    }

    /**
     * Returns a peer with the given consistent ID or {@code null} if it is not present in the configuration.
     */
    public @Nullable Peer peer(String consistentId) {
        return peers.stream().filter(p -> p.consistentId().equals(consistentId)).findAny().orElse(null);
    }

    /**
     * Returns a learner with the given consistent ID or {@code null} if it is not present in the configuration.
     */
    public @Nullable Peer learner(String consistentId) {
        return learners.stream().filter(p -> p.consistentId().equals(consistentId)).findAny().orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PeersAndLearners that = (PeersAndLearners) o;

        if (!peers.equals(that.peers)) {
            return false;
        }
        return learners.equals(that.learners);
    }

    @Override
    public int hashCode() {
        int result = peers.hashCode();
        result = 31 * result + learners.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(PeersAndLearners.class, this);
    }
}
