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

package org.apache.ignite.internal.cluster.management.topology;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.network.ClusterNode;

/**
 * Implementation of {@link InternalLogicalTopologyService}.
 */
public class LogicalTopologyServiceImpl implements InternalLogicalTopologyService {
    /** Storage key for the logical topology. */
    private static final byte[] LOGICAL_TOPOLOGY_KEY = "logical".getBytes(UTF_8);

    private final ClusterStateStorage storage;

    public LogicalTopologyServiceImpl(ClusterStateStorage storage) {
        this.storage = storage;
    }

    @Override
    public Collection<ClusterNode> getLogicalTopology() {
        return readLogicalTopology().nodes();
    }

    private LogicalTopologySnapshot readLogicalTopology() {
        byte[] bytes = storage.get(LOGICAL_TOPOLOGY_KEY);

        return bytes == null ? LogicalTopologySnapshot.INITIAL : fromBytes(bytes);
    }

    @Override
    public void putLogicalTopologyNode(ClusterNode node) {
        replaceLogicalTopologyWith(readLogicalTopology().addNode(node));
    }

    private void replaceLogicalTopologyWith(LogicalTopologySnapshot newTopology) {
        storage.put(LOGICAL_TOPOLOGY_KEY, toBytes(newTopology));
    }

    @Override
    public void removeLogicalTopologyNodes(Set<ClusterNode> nodes) {
        replaceLogicalTopologyWith(readLogicalTopology().removeNodesByIds(nodes));
    }

    @Override
    public boolean isNodeInLogicalTopology(ClusterNode node) {
        return readLogicalTopology().containsNodeById(node);
    }
}
