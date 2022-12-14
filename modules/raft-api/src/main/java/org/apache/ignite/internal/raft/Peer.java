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
import org.apache.ignite.internal.tostring.S;

/**
 * A participant of a replication group.
 */
public final class Peer implements Serializable {
    private static final long serialVersionUID = 6140534113821565486L;

    /**
     * Node consistent ID.
     */
    private final String consistentId;

    /**
     * Peer index. Used when multiple Raft nodes are started on the same Ignite node.
     */
    private final int idx;

    /**
     * Constructor.
     *
     * @param consistentId Consistent ID of a node.
     */
    public Peer(String consistentId) {
        this(consistentId, 0);
    }

    /**
     * Constructor.
     *
     * @param consistentId Consistent ID of a node.
     * @param idx Peer index.
     */
    public Peer(String consistentId, int idx) {
        this.consistentId = consistentId;
        this.idx = idx;
    }

    /**
     * Returns this node's consistent ID.
     */
    public String consistentId() {
        return consistentId;
    }

    /**
     * Returns this node's index.
     */
    public int idx() {
        return idx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Peer peer = (Peer) o;

        if (idx != peer.idx) {
            return false;
        }
        return consistentId.equals(peer.consistentId);
    }

    @Override
    public int hashCode() {
        int result = consistentId.hashCode();
        result = 31 * result + idx;
        return result;
    }

    @Override
    public String toString() {
        return S.toString(Peer.class, this);
    }
}
