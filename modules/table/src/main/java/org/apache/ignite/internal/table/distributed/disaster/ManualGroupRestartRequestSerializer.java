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

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VersionedSerializer} for {@link ManualGroupRestartRequest} instances.
 */
class ManualGroupRestartRequestSerializer extends VersionedSerializer<ManualGroupRestartRequest> {
    // We use this constant due compatibility reasons.
    private static final int UNDEFINED_TABLE_ID = -1;

    /** Serializer instance. */
    static final ManualGroupRestartRequestSerializer INSTANCE = new ManualGroupRestartRequestSerializer();

    @Override
    protected byte getProtocolVersion() {
        return 3;
    }

    @Override
    protected void writeExternalData(ManualGroupRestartRequest request, IgniteDataOutput out) throws IOException {
        Objects.requireNonNull(request.coordinator(), "Coordinator must not be null");

        out.writeUuid(request.operationId());
        out.writeVarInt(request.zoneId());
        // Write this field due compatibility reasons.
        out.writeVarInt(UNDEFINED_TABLE_ID);
        writeVarIntSet(request.partitionIds(), out);
        writeStringSet(request.nodeNames(), out);
        hybridTimestamp(request.assignmentsTimestamp()).writeTo(out);
        out.writeBoolean(request.cleanUp()); // Write the new 'cleanUp' field introduced in protocol version 2.
        out.writeUTF(request.coordinator()); // Write the new 'coordinator' field introduced in protocol version 3.
    }

    @Override
    protected ManualGroupRestartRequest readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        UUID operationId = in.readUuid();
        int zoneId = in.readVarIntAsInt();
        // Need to read this ignored value due compatibility reasons.
        in.readVarIntAsInt();
        Set<Integer> partitionIds = readVarIntSet(in);
        Set<String> nodeNames = readStringSet(in);
        HybridTimestamp assignmentsTimestamp = HybridTimestamp.readFrom(in);

        boolean cleanUp = false;

        if (protoVer >= 2) {
            cleanUp = in.readBoolean(); // Read the new 'cleanUp' field if protocol version is 2 or greater.
        }

        String coordinator = readCoordinator(protoVer, in);

        return new ManualGroupRestartRequest(
                operationId,
                zoneId,
                partitionIds,
                nodeNames,
                assignmentsTimestamp.longValue(),
                cleanUp,
                coordinator
        );
    }

    private static @Nullable String readCoordinator(byte protoVer, IgniteDataInput in) throws IOException {
        return protoVer >= 3 ? in.readUTF() : null;
    }
}
