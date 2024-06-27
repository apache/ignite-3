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
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.IgniteTables;
import org.jetbrains.annotations.Nullable;

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
            byte[] payload = in.readBinary();

            byte[] encodedPayload = new byte[payload.length + 4];
            encodedPayload[0] = (byte) ((payloadElementCount >> 24) & 0xFF); // Most significant byte
            encodedPayload[1] = (byte) ((payloadElementCount >> 16) & 0xFF);
            encodedPayload[2] = (byte) ((payloadElementCount >> 8) & 0xFF);
            encodedPayload[3] = (byte) (payloadElementCount & 0xFF); // Least significant byte

            // Copy the payload into the remaining part of encodedPayload
            System.arraycopy(payload, 0, encodedPayload, 4, payload.length);

            return table.partitionManager().primaryReplicaAsync(new HashPartition(partition)).thenCompose(primaryReplica -> {
                // Use Compute to execute receiver on the target node with failover, class loading, scheduling.
                JobExecution<List<Object>> jobExecution = compute.executeAsyncWithFailover(
                        Set.of(primaryReplica),
                        deploymentUnits,
                        ReceiverRunnerJob.class.getName(),
                        JobExecutionOptions.DEFAULT,
                        encodedPayload);

                return jobExecution.resultAsync()
                        .handle((res, err) -> {
                            if (err != null) {
                                if (err.getCause() instanceof ComputeException) {
                                    ComputeException computeErr = (ComputeException) err.getCause();
                                    throw new IgniteException(
                                            COMPUTE_JOB_FAILED_ERR,
                                            "Streamer receiver failed: " + computeErr.getMessage(), computeErr);
                                }

                                ExceptionUtils.sneakyThrow(err);
                            }

                            StreamerReceiverSerializer.serializeResults(out, returnResults ? res : null);
                            return null;
                        });
            });
        });
    }

    private static class ReceiverRunnerJob implements ComputeJob<byte[], List<Object>> {
        @Override
        public @Nullable CompletableFuture<List<Object>> executeAsync(JobExecutionContext context, byte[] payload) {
            // Combine the first four bytes into an integer
            int payloadElementCount = ((payload[0] & 0xFF) << 24) | ((payload[1] & 0xFF) << 16)
                    | ((payload[2] & 0xFF) << 8) | (payload[3] & 0xFF);

            byte[] remainingBytes = new byte[payload.length - 4];
            // Copy the remaining bytes to the new array
            System.arraycopy(payload, 4, remainingBytes, 0, remainingBytes.length);

            var receiverInfo = StreamerReceiverSerializer.deserialize(remainingBytes, payloadElementCount);

            ClassLoader classLoader = ((JobExecutionContextImpl) context).classLoader();
            Class<DataStreamerReceiver<Object, Object, Object>> receiverClass = ComputeUtils.receiverClass(classLoader, receiverInfo.className());
            DataStreamerReceiver<Object, Object, Object> receiver = ComputeUtils.instantiateReceiver(receiverClass);
            DataStreamerReceiverContext receiverContext = context::ignite;

            return receiver.receive(receiverInfo.items(), receiverContext, receiverInfo.args());
        }
    }
}
