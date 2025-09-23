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

package org.apache.ignite.client.handler.requests.compute;

import static org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest.packSubmitResult;
import static org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest.sendResultAndState;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTableAsync;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.readTuple;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJob;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackTaskId;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.COMPUTE_TASK_ID;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientContext;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.Job;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ExecutionContext;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.table.IgniteTables;

/**
 * Compute execute colocated request.
 */
public class ClientComputeExecuteColocatedRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param compute Compute.
     * @param tables Tables.
     * @param cluster Cluster service
     * @param notificationSender Notification sender.
     * @param clientContext Client context.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteComputeInternal compute,
            IgniteTables tables,
            ClusterService cluster,
            NotificationSender notificationSender,
            ClientContext clientContext
    ) {
        int tableId = in.unpackInt();
        int schemaId = in.unpackInt();

        BitSet noValueSet = in.unpackBitSet();
        byte[] tupleBytes = in.readBinary();

        Job job = unpackJob(in, clientContext.hasFeature(PLATFORM_COMPUTE_JOB));
        unpackTaskId(in, clientContext.hasFeature(COMPUTE_TASK_ID)); // Placeholder for a possible future usage

        return readTableAsync(tableId, tables).thenCompose(table -> readTuple(schemaId, noValueSet, tupleBytes, table, true)
                .thenCompose(keyTuple -> {
                    ComputeEventMetadataBuilder metadataBuilder = ComputeEventMetadata.builder(Type.SINGLE)
                            .eventUser(clientContext.userDetails())
                            .tableName(table.name())
                            .clientAddress(clientContext.remoteAddress().toString());

                    CompletableFuture<JobExecution<ComputeJobDataHolder>> jobExecutionFut = compute.submitColocatedInternal(
                            table,
                            keyTuple,
                            new ExecutionContext(
                                    job.options(),
                                    job.deploymentUnits(),
                                    job.jobClassName(),
                                    metadataBuilder,
                                    job.arg()
                            ),
                            null
                    );

                    sendResultAndState(jobExecutionFut, notificationSender);

                    return jobExecutionFut.thenCompose(execution ->
                            execution.idAsync().thenApply(jobId -> out -> {
                                out.packInt(table.schemaView().lastKnownSchemaVersion());

                                //noinspection DataFlowIssue
                                packSubmitResult(out, jobId, execution.node());
                            })
                    );
                }));
    }
}
