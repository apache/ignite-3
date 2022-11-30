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
 * Class that is used to uniquely identify a locally hosted Raft group.
 */
public class RaftGroupId {
    private final ReplicationGroupId replicationGroupId;

    private final Peer serverPeer;

    /**
     * Creates an instance.
     *
     * @param replicationGroupId Raft group name.
     * @param serverPeer Local peer running a Raft node.
     */
    public RaftGroupId(ReplicationGroupId replicationGroupId, Peer serverPeer) {
        this.replicationGroupId = replicationGroupId;
        this.serverPeer = Objects.requireNonNull(serverPeer);
    }

    /**
     * Return the name of this Raft group.
     */
    public ReplicationGroupId replicationGroupId() {
        return replicationGroupId;
    }

    /**
     * Returns the local peer running a Raft node.
     */
    public Peer serverPeer() {
        return serverPeer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RaftGroupId that = (RaftGroupId) o;

        if (!replicationGroupId.equals(that.replicationGroupId)) {
            return false;
        }
        return serverPeer.equals(that.serverPeer);
    }

    @Override
    public int hashCode() {
        int result = replicationGroupId.hashCode();
        result = 31 * result + serverPeer.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(RaftGroupId.class, this);
    }
}
