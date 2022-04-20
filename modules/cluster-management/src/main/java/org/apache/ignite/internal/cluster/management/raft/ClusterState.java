/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.raft;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a cluster state. It contains:
 * <ol>
 *     <li>Names of the nodes that host the Meta Storage.</li>
 *     <li>Names of the nodes that host the CMG.</li>
 * </ol>
 */
public final class ClusterState implements Serializable {
    private final Set<String> cmgNodes;

    private final Set<String> msNodes;

    public ClusterState(Collection<String> cmgNodes, Collection<String> metaStorageNodes) {
        this.cmgNodes = Set.copyOf(cmgNodes);
        this.msNodes = Set.copyOf(metaStorageNodes);
    }

    public Set<String> cmgNodes() {
        return cmgNodes;
    }

    public Set<String> metaStorageNodes() {
        return msNodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterState that = (ClusterState) o;
        return cmgNodes.equals(that.cmgNodes) && msNodes.equals(that.msNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmgNodes, msNodes);
    }

    @Override
    public String toString() {
        return "ClusterState{"
                + "cmgNodes=" + cmgNodes
                + ", msNodes=" + msNodes
                + '}';
    }
}
