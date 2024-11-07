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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.internal.versioned.VersionedSerializer;

/**
 * {@link VersionedSerializer} for {@link DisasterRecoveryRequest} instances.
 */
class DisasterRecoveryRequestSerializer extends VersionedSerializer<DisasterRecoveryRequest> {
    static final DisasterRecoveryRequestSerializer INSTANCE = new DisasterRecoveryRequestSerializer();

    @Override
    protected void writeExternalData(DisasterRecoveryRequest request, IgniteDataOutput out) throws IOException {
        Operation operation = Operation.findByRequest(request);

        out.writeVarInt(operation.code);
        operation.serialize(request, out);
    }

    @Override
    protected DisasterRecoveryRequest readExternalData(byte protoVer, IgniteDataInput in) throws IOException {
        int operationCode = in.readVarIntAsInt();
        Operation operation = Operation.findByCode(operationCode);

        return operation.deserialize(in);
    }

    private enum Operation {
        GROUP_UPDATE(0, GroupUpdateRequestSerializer.INSTANCE),
        MANUAL_GROUP_RESTART(1, ManualGroupRestartRequestSerializer.INSTANCE);

        private static final Map<Integer, Operation> valuesByCode = Arrays.stream(values())
                .collect(toUnmodifiableMap(op -> op.code, identity()));

        private final int code;
        private final VersionedSerializer<DisasterRecoveryRequest> serializer;

        Operation(int code, VersionedSerializer<? extends DisasterRecoveryRequest> serializer) {
            this.code = code;
            this.serializer = (VersionedSerializer<DisasterRecoveryRequest>) serializer;
        }

        static Operation findByCode(int code) {
            Operation operation = valuesByCode.get(code);

            if (operation == null) {
                throw new IllegalArgumentException("Unknown operation code: " + code);
            }

            return operation;
        }

        static Operation findByRequest(DisasterRecoveryRequest request) {
            if (request instanceof GroupUpdateRequest) {
                return GROUP_UPDATE;
            }
            if (request instanceof ManualGroupRestartRequest) {
                return MANUAL_GROUP_RESTART;
            }

            throw new IllegalArgumentException("Unknown request type: " + request);
        }

        void serialize(DisasterRecoveryRequest request, IgniteDataOutput out) throws IOException {
            serializer.writeExternal(request, out);
        }

        <T extends DisasterRecoveryRequest> T deserialize(IgniteDataInput in) throws IOException {
            return (T) serializer.readExternal(in);
        }
    }
}
