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
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJobArgumentWithoutMarshaller;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.table.IgniteTables;

/**
 * Compute execute partitioned request.
 */
public class ClientComputeExecutePartitionedRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param compute Compute.
     * @param tables Tables.
     * @param cluster Cluster service
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteComputeInternal compute,
            IgniteTables tables,
            ClusterService cluster,
            NotificationSender notificationSender
    ) {
        return readTableAsync(in, tables).thenCompose(table -> {
            out.packInt(table.schemaView().lastKnownSchemaVersion());

            int partitionId = in.unpackInt();

            // Unpack job
            List<DeploymentUnit> deploymentUnits = in.unpackDeploymentUnits();
            String jobClassName = in.unpackString();
            JobExecutionOptions options = JobExecutionOptions.builder().priority(in.unpackInt()).maxRetries(in.unpackInt()).build();
            ComputeJobDataHolder args = unpackJobArgumentWithoutMarshaller(in);

            CompletableFuture<JobExecution<ComputeJobDataHolder>> jobExecutionFut = compute.submitPartitionedInternal(
                    table,
                    partitionId,
                    deploymentUnits,
                    jobClassName,
                    options,
                    args,
                    null
            );

            sendResultAndState(jobExecutionFut, notificationSender);

            //noinspection DataFlowIssue
            return jobExecutionFut.thenCompose(execution ->
                    execution.idAsync().thenAccept(jobId -> packSubmitResult(out, jobId, execution.node()))
            );
        });
    }
}
