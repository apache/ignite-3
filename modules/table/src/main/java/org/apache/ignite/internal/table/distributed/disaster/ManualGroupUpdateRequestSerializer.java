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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link ManualGroupUpdateRequest} instances.
 */
class ManualGroupUpdateRequestSerializer extends VersionedSerializer<ManualGroupUpdateRequest> {
    /** Serializer instance. */
    static final ManualGroupUpdateRequestSerializer INSTANCE = new ManualGroupUpdateRequestSerializer();

    @Override
    protected void writeExternalData(ManualGroupUpdateRequest request, IgniteDataOutput out) throws IOException {
        out.writeUuid(request.operationId());
        out.writeVarInt(request.catalogVersion());
        out.writeVarInt(request.zoneId());
        out.writeVarInt(request.tableId());
        writeVarIntSet(request.partitionIds(), out);
    }

    @Override
    protected ManualGroupUpdateRequest readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        UUID operationId = in.readUuid();
        int catalogVersion = in.readVarIntAsInt();
        int zoneId = in.readVarIntAsInt();
        int tableId = in.readVarIntAsInt();
        Set<Integer> partitionIds = readVarIntSet(in);

        return new ManualGroupUpdateRequest(operationId, catalogVersion, zoneId, tableId, partitionIds);
    }
}
