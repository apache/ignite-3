/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft;

import java.io.Serializable;
import org.apache.ignite.network.NetworkMember;

/**
 * Represents a participant in a replicating group.
 */
public final class PeerId implements Serializable {
    private static final long serialVersionUID = 8083529734784884641L;

    /**
     * Cluster node for peer.
     */
    private final NetworkMember node;

    /**
     * Peer's local priority value, if node don't support priority election, this value is -1.
     */
    private final int priority;

    public PeerId(PeerId peer) {
        this.node = peer.getNode();
        this.priority = peer.getPriority();
    }

    public PeerId(NetworkMember node) {
        this(node, ElectionPriority.DISABLED);
    }

    public PeerId(final NetworkMember node, final int priority) {
        super();
        this.node = node;
        this.priority = priority;
    }

    public NetworkMember getNode() {
        return this.node;
    }

    public int getPriority() {
        return priority;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PeerId peerId = (PeerId) o;

        if (priority != peerId.priority) return false;
        if (!node.equals(peerId.node)) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = node.hashCode();
        result = 31 * result + priority;
        return result;
    }

    @Override public String toString() {
        return node.name() + ":" + priority;
    }
}
