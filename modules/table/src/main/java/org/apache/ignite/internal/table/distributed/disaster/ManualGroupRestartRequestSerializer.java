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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestsSerialization.readVarIntSet;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestsSerialization.writeVarIntSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link ManualGroupRestartRequest} instances.
 */
class ManualGroupRestartRequestSerializer extends VersionedSerializer<ManualGroupRestartRequest> {
    /** Serializer instance. */
    static final ManualGroupRestartRequestSerializer INSTANCE = new ManualGroupRestartRequestSerializer();

    @Override
    protected void writeExternalData(ManualGroupRestartRequest request, IgniteDataOutput out) throws IOException {
        out.writeUuid(request.operationId());
        out.writeVarInt(request.zoneId());
        out.writeVarInt(request.tableId());
        writeVarIntSet(request.partitionIds(), out);

        out.writeVarInt(request.nodeNames().size());
        for (String nodeName : request.nodeNames()) {
            out.writeUTF(nodeName);
        }

        // Writing long and not a varlong as the latter requires 9 bytes for hybrid timestamps.
        out.writeLong(request.assignmentsTimestamp());
    }

    @Override
    protected ManualGroupRestartRequest readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        UUID operationId = in.readUuid();
        int zoneId = in.readVarIntAsInt();
        int tableId = in.readVarIntAsInt();
        Set<Integer> partitionIds = readVarIntSet(in);
        Set<String> nodeNames = readStringSet(in);
        long assignmentsTimestamp = in.readLong();

        return new ManualGroupRestartRequest(operationId, zoneId, tableId, partitionIds, nodeNames, assignmentsTimestamp);
    }

    private static Set<String> readStringSet(IgniteDataInput in) throws IOException {
        int size = in.readVarIntAsInt();

        Set<String> result = new HashSet<>(IgniteUtils.capacity(size));
        for (int i = 0; i < size; i++) {
            result.add(in.readUTF());
        }

        return result;
    }
}
