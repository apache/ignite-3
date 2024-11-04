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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
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
    protected void writeExternalData(TxMeta meta, IgniteDataOutput out) throws IOException {
        out.writeVarInt(meta.txState().ordinal());

        out.writeVarInt(meta.enlistedPartitions().size());
        for (TablePartitionId partitionId : meta.enlistedPartitions()) {
            out.writeVarInt(partitionId.tableId());
            out.writeVarInt(partitionId.partitionId());
        }

        HybridTimestamp.write(meta.commitTimestamp(), out);
    }

    @Override
    protected TxMeta readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        TxState state = TxState.fromOrdinal(in.readVarIntAsInt());
        List<TablePartitionId> enlistedPartitions = readEnlistedPartitions(in);
        HybridTimestamp commitTimestamp = HybridTimestamp.readNullableFrom(in);

        return new TxMeta(state, enlistedPartitions, commitTimestamp);
    }

    private static List<TablePartitionId> readEnlistedPartitions(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        List<TablePartitionId> enlistedPartitions = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            int tableId = in.readVarIntAsInt();
            int partitionId = in.readVarIntAsInt();

            enlistedPartitions.add(new TablePartitionId(tableId, partitionId));
        }

        return enlistedPartitions;
    }
}
