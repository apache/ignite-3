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
import org.apache.ignite.internal.tostring.S;

/**
 * Stored Raft node ID. It holds the subset of information contained in {@link RaftNodeId} that can be
 *
 * @see RaftNodeId
 */
public class StoredRaftNodeId {
    private final String groupIdName;
    private final Peer peer;

    public StoredRaftNodeId(String groupIdName, Peer peer) {
        this.groupIdName = groupIdName;
        this.peer = peer;
    }

    public String groupIdName() {
        return groupIdName;
    }

    public Peer peer() {
        return peer;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoredRaftNodeId that = (StoredRaftNodeId) o;
        return Objects.equals(groupIdName, that.groupIdName) && Objects.equals(peer, that.peer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupIdName, peer);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
