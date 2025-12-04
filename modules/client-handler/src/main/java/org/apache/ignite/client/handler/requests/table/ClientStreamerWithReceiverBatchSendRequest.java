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
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.ReceiverExecutionOptions;

/**
 * Client streamer batch request.
 */
public class ClientStreamerWithReceiverBatchSendRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param tables Ignite tables.
     * @param enableExecutionOptions Whether to read execution options.
     * @param tsTracker Hybrid timestamp tracker.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteTables tables,
            boolean enableExecutionOptions,
            HybridTimestampTracker tsTracker) {
        int tableId = in.unpackInt();
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

        ReceiverExecutionOptions options = enableExecutionOptions
                ? ReceiverExecutionOptions.builder()
                .priority(in.unpackInt())
                .maxRetries(in.unpackInt())
                .executorType(JobExecutorType.fromOrdinal(in.unpackInt()))
                .build()
                : ReceiverExecutionOptions.DEFAULT;

        return readTableAsync(tableId, tables).thenCompose(table -> {
            return table.partitionDistribution()
                    .primaryReplicaAsync(new HashPartition(partition))
                    .thenApply(ClusterNodeImpl::fromPublicClusterNode)
                    .thenCompose(node -> table.internalTable().streamerReceiverRunner()
                            .runReceiverAsync(payloadArr, node, deploymentUnits, options))
                    .thenApply(res -> {
                        byte[] resBytes = res.get1();
                        Long observableTs = res.get2();

                        assert observableTs != null : "Observable timestamp should not be null";
                        assert observableTs != NULL_HYBRID_TIMESTAMP : "Observable timestamp should not be NULL_HYBRID_TIMESTAMP";

                        tsTracker.update(observableTs);

                        return out -> StreamerReceiverSerializer.serializeReceiverResultsForClient(out, returnResults ? resBytes : null);
                    });
        });
    }
}
