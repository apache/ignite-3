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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.table.IgniteTables;

/**
 * Client streamer batch request.
 */
public class ClientStreamerWithReceiverBatchSendRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param tables Ignite tables.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteTables tables,
            IgniteComputeInternal compute
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            int partition = in.unpackInt();
            List<DeploymentUnit> deploymentUnits = in.unpackDeploymentUnits();
            boolean returnResults = in.unpackBoolean();

            // Payload = binary tuple of (receiverClassName, receiverArgs, items). We pass it to the job without deserialization.
            int payloadElementCount = in.unpackInt();
            int payloadSize = in.unpackBinaryHeader();

            byte[] payloadArr = new byte[payloadSize + 4];
            var payloadBuf = ByteBuffer.wrap(payloadArr).order(ByteOrder.LITTLE_ENDIAN);

            payloadBuf.putInt(payloadElementCount);
            in.readPayload(payloadBuf);

            return table.partitionManager()
                    .primaryReplicaAsync(new HashPartition(partition))
                    .thenCompose(node -> table.internalTable().runReceiverAsync(payloadArr, node, deploymentUnits))
                    .thenAccept(res -> StreamerReceiverSerializer.serializeReceiverJobResultsForClient(out, returnResults ? res : null));
        });
    }
}
