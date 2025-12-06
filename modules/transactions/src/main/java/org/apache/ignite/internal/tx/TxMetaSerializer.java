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
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link TxMeta} instances.
 */
public class TxMetaSerializer extends VersionedSerializer<TxMeta> {
    /** Serializer instance. */
    public static final TxMetaSerializer INSTANCE = new TxMetaSerializer();

    @Override
    protected byte getProtocolVersion() {
        return 2;
    }

    @Override
    protected void writeExternalData(TxMeta meta, IgniteDataOutput out) throws IOException {
        // Write this boolean flag (which is always true) due to compatibility reasons.
        boolean hasAnyZonePartitionIds = true;
        out.writeBoolean(hasAnyZonePartitionIds);

        out.writeVarInt(meta.txState().id());

        out.writeVarInt(meta.enlistedPartitions().size());
        for (EnlistedPartitionGroup enlistedPartitionGroup : meta.enlistedPartitions()) {
            ZonePartitionId partitionGroupId = enlistedPartitionGroup.groupId();

            out.writeVarInt(partitionGroupId.objectId());
            out.writeVarInt(partitionGroupId.partitionId());
            writeVarIntSet(enlistedPartitionGroup.tableIds(), out);
        }

        HybridTimestamp.write(meta.commitTimestamp(), out);
    }

    @Override
    protected TxMeta readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        // V1 version is not supported anymore.
        if (protoVer == 1) {
            throw new IllegalArgumentException("Transaction meta v1 is not supported.");
        }

        if (protoVer >= 2) {
            boolean usesZonePartitionIds = in.readBoolean();
            assert usesZonePartitionIds : "Transaction meta relies on table partition IDs.";
        }

        TxState state = TxState.fromId(in.readVarIntAsInt());
        List<EnlistedPartitionGroup> enlistedPartitions = readEnlistedPartitions(in, protoVer);
        HybridTimestamp commitTimestamp = HybridTimestamp.readNullableFrom(in);

        return new TxMeta(state, enlistedPartitions, commitTimestamp);
    }

    private static List<EnlistedPartitionGroup> readEnlistedPartitions(IgniteDataInput in, byte protoVer) throws IOException {
        assert protoVer >= 2 : "Unsupported protocol version [ver=" + protoVer + "].";

        int length = in.readVarIntAsInt();

        List<EnlistedPartitionGroup> enlistedPartitions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            int objectId = in.readVarIntAsInt();
            int partitionId = in.readVarIntAsInt();

            Set<Integer> tableIds = readVarIntSet(in);

            enlistedPartitions.add(
                    new EnlistedPartitionGroup(
                            new ZonePartitionId(objectId, partitionId),
                            tableIds
                    ));
        }

        return enlistedPartitions;
    }
}
