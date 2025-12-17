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

import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestsSerialization.readVarIntMap;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestsSerialization.writeVarIntMap;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link GroupUpdateRequest} instances.
 */
// TODO check and update
class GroupUpdateRequestSerializer extends VersionedSerializer<GroupUpdateRequest> {
    /** Serializer instance. */
    static final GroupUpdateRequestSerializer INSTANCE = new GroupUpdateRequestSerializer();

    @Override
    protected byte getProtocolVersion() {
        return 2;
    }

    @Override
    protected void writeExternalData(GroupUpdateRequest request, IgniteDataOutput out) throws IOException {
        out.writeUuid(request.operationId());
        out.writeVarInt(request.catalogVersion());
        out.writeVarInt(request.zoneId());
        writeVarIntMap(request.partitionIds(), out);
        out.writeBoolean(request.manualUpdate());
        out.writeBoolean(request.colocationEnabled());
    }

    @Override
    protected GroupUpdateRequest readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        UUID operationId = in.readUuid();
        int catalogVersion = in.readVarIntAsInt();
        int zoneId = in.readVarIntAsInt();
        Map<Integer, Set<Integer>> partitionIds = readVarIntMap(in);
        boolean manualUpdate = in.readBoolean();
        boolean usesZonePartitionIds;
        if (protoVer >= 2) {
            usesZonePartitionIds = in.readBoolean();
        } else {
            usesZonePartitionIds = false;
        }
        return GroupUpdateRequest
                .create(operationId, catalogVersion, zoneId, partitionIds, manualUpdate, usesZonePartitionIds);
    }
}
