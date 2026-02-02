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

package org.apache.ignite.internal.raft.client;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods for RAFT peer operations.
 */
final class RaftPeerUtils {
    private RaftPeerUtils() {
        // No-op.
    }

    /**
     * Parses a peer ID string to a Peer object.
     *
     * @param peerId Peer ID string in format "consistentId:idx" or just "consistentId".
     * @return Parsed Peer object, or {@code null} if parsing fails or input is null.
     */
    static @Nullable Peer parsePeer(@Nullable String peerId) {
        PeerId id = PeerId.parsePeer(peerId);

        return id == null ? null : new Peer(id.getConsistentId(), id.getIdx());
    }

    /**
     * Parse list of peers from list with string representations.
     */
    static List<Peer> parsePeerList(@Nullable Collection<String> peers) {
        if (peers == null) {
            return List.of();
        }

        List<Peer> res = new ArrayList<>(peers.size());
        for (String peer : peers) {
            res.add(parsePeer(peer));
        }
        return res;
    }

    /**
     * Converts a Peer to string ID.
     */
    static String peerId(Peer peer) {
        return PeerId.fromPeer(peer).toString();
    }

    /**
     * Converts a collection of Peers to string IDs.
     */
    static List<String> peerIds(Collection<Peer> peers) {
        return peers.stream().map(RaftPeerUtils::peerId).collect(toList());
    }
}
