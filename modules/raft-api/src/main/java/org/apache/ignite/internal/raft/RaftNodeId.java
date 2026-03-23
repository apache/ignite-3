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

import java.util.Objects;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.S;

/**
 * Raft node identifier, consists of a Raft group ID and a Peer ID.
 */
public class RaftNodeId {
    private static final String PEER_INDEX_DELIMITER = "-";

    private final ReplicationGroupId groupId;

    private final Peer peer;

    /**
     * Creates an instance.
     *
     * @param groupId Raft group name.
     * @param peer Peer running a Raft node.
     */
    public RaftNodeId(ReplicationGroupId groupId, Peer peer) {
        this.groupId = groupId;
        this.peer = Objects.requireNonNull(peer);
    }

    /**
     * Returns string which represents this Raft node ID when used in file paths and RocksDB prefixes.
     *
     * @see #fromNodeIdStringForStorage(String, String)
     */
    public String nodeIdStringForStorage() {
        return groupId().toString() + PEER_INDEX_DELIMITER + peer().idx();
    }

    /**
     * Parses string represenation of {@link RaftNodeId} and returns it as {@link StoredRaftNodeId}.
     *
     * @param nodeIdStr String to parse (it's produced by {@link #nodeIdStringForStorage()}).
     * @param localNodeConsistentId Name of the local node.
     * @see #nodeIdStringForStorage()
     */
    public static StoredRaftNodeId fromNodeIdStringForStorage(String nodeIdStr, String localNodeConsistentId) {
        int separatorIndex = nodeIdStr.lastIndexOf(PEER_INDEX_DELIMITER);

        int peerIndex = Integer.parseInt(nodeIdStr.substring(separatorIndex + PEER_INDEX_DELIMITER.length()));
        Peer peer = new Peer(localNodeConsistentId, peerIndex);

        return new StoredRaftNodeId(nodeIdStr.substring(0, separatorIndex), peer);
    }

    /**
     * Returns the ID of this Raft group.
     */
    public ReplicationGroupId groupId() {
        return groupId;
    }

    /**
     * Returns the peer running a Raft node.
     */
    public Peer peer() {
        return peer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RaftNodeId that = (RaftNodeId) o;

        if (!groupId.equals(that.groupId)) {
            return false;
        }
        return peer.equals(that.peer);
    }

    @Override
    public int hashCode() {
        int result = groupId.hashCode();
        result = 31 * result + peer.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(RaftNodeId.class, this);
    }
}
