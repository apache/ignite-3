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

package org.apache.ignite.internal.tx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link TxMeta} instances.
 */
// TODO remove support of v1
public class TxMetaSerializer extends VersionedSerializer<TxMeta> {
    /** Serializer instance. */
    public static final TxMetaSerializer INSTANCE = new TxMetaSerializer();

    @Override
    protected byte getProtocolVersion() {
        return 2;
    }

    @Override
    protected void writeExternalData(TxMeta meta, IgniteDataOutput out) throws IOException {
        boolean hasAnyZonePartitionIds = hasAnyZonePartitionIds(meta);
        if (hasAnyZonePartitionIds) {
            assert hasNoTablePartitionIds(meta) : "Both table-based and zone-based partition IDs: " + meta.enlistedPartitions();
        }

        out.writeBoolean(hasAnyZonePartitionIds);

        out.writeVarInt(meta.txState().id());

        out.writeVarInt(meta.enlistedPartitions().size());
        for (EnlistedPartitionGroup enlistedPartitionGroup : meta.enlistedPartitions()) {
            PartitionGroupId partitionGroupId = (PartitionGroupId) enlistedPartitionGroup.groupId();

            out.writeVarInt(partitionGroupId.objectId());
            out.writeVarInt(partitionGroupId.partitionId());
            writeVarIntSet(enlistedPartitionGroup.tableIds(), out);
        }

        HybridTimestamp.write(meta.commitTimestamp(), out);
    }

    private static boolean hasAnyZonePartitionIds(TxMeta meta) {
        return meta.enlistedPartitions().stream()
                .anyMatch(partition -> partition.groupId() instanceof ZonePartitionId);
    }

    private static boolean hasNoTablePartitionIds(TxMeta meta) {
        // TODO
        return true;
//        return meta.enlistedPartitions().stream()
//                .noneMatch(partition -> partition.groupId() instanceof TablePartitionId);
    }

    @Override
    protected TxMeta readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        boolean usesZonePartitionIds;
        if (protoVer >= 2) {
            usesZonePartitionIds = in.readBoolean();
        } else {
            usesZonePartitionIds = false;
        }

        TxState state = TxState.fromId(in.readVarIntAsInt());
        List<EnlistedPartitionGroup> enlistedPartitions = readEnlistedPartitions(in, protoVer, usesZonePartitionIds);
        HybridTimestamp commitTimestamp = HybridTimestamp.readNullableFrom(in);

        return new TxMeta(state, enlistedPartitions, commitTimestamp);
    }

    private static List<EnlistedPartitionGroup> readEnlistedPartitions(IgniteDataInput in, byte protoVer, boolean usesZonePartitionIds)
            throws IOException {
        int length = in.readVarIntAsInt();

        List<EnlistedPartitionGroup> enlistedPartitions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            int objectId = in.readVarIntAsInt();
            int partitionId = in.readVarIntAsInt();

            Set<Integer> tableIds;
            if (protoVer >= 2) {
                tableIds = readVarIntSet(in);
            } else {
                // In version 1 only table-based partitions were supported, so tableIds will be a singleton.
                tableIds = Set.of(objectId);
            }

            enlistedPartitions.add(
                    new EnlistedPartitionGroup(
                            (ZonePartitionId) replicationGroupId(objectId, partitionId, usesZonePartitionIds),
                            tableIds
                    ));
        }

        return enlistedPartitions;
    }

    private static ReplicationGroupId replicationGroupId(int objectId, int partitionId, boolean usesZonePartitionIds) {
        if (usesZonePartitionIds) {
            return new ZonePartitionId(objectId, partitionId);
        } else {
            return new TablePartitionId(objectId, partitionId);
        }
    }
}
