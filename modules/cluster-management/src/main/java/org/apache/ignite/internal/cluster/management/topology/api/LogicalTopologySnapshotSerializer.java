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

package org.apache.ignite.internal.cluster.management.topology.api;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link LogicalTopologySnapshot} instances.
 */
public class LogicalTopologySnapshotSerializer extends VersionedSerializer<LogicalTopologySnapshot> {
    /** Serializer instance. */
    public static final LogicalTopologySnapshotSerializer INSTANCE = new LogicalTopologySnapshotSerializer();

    private final LogicalNodeSerializer logicalNodeSerializer = LogicalNodeSerializer.INSTANCE;

    @Override
    protected void writeExternalData(LogicalTopologySnapshot snapshot, IgniteDataOutput out) throws IOException {
        out.writeVarInt(snapshot.version());

        out.writeVarInt(snapshot.size());
        for (LogicalNode node : snapshot.nodes()) {
            logicalNodeSerializer.writeExternal(node, out);
        }

        out.writeUuid(snapshot.clusterId());
    }

    @Override
    protected LogicalTopologySnapshot readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        long version = in.readVarInt();

        int nodesCount = in.readVarIntAsInt();
        Set<LogicalNode> nodes = new HashSet<>(IgniteUtils.capacity(nodesCount));
        for (int i = 0; i < nodesCount; i++) {
            nodes.add(logicalNodeSerializer.readExternal(in));
        }

        UUID clusterId = in.readUuid();

        return new LogicalTopologySnapshot(version, nodes, clusterId);
    }
}
