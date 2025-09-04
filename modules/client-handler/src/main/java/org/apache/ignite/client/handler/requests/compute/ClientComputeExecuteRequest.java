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

import static org.apache.ignite.client.handler.requests.cluster.ClientClusterGetNodesRequest.packClusterNode;
import static org.apache.ignite.client.handler.requests.compute.ClientComputeGetStateRequest.packJobState;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJob;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackTaskId;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.COMPUTE_TASK_ID;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.PLATFORM_COMPUTE_JOB;
import static org.apache.ignite.internal.hlc.HybridTimestamp.NULL_HYBRID_TIMESTAMP;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.client.handler.ClientContext;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.NodeNotFoundException;
import org.apache.ignite.internal.client.proto.ClientComputeJobPacker;
import org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.Job;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.ExecutionContext;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Compute execute request.
 */
public class ClientComputeExecuteRequest {
    private static final IgniteLogger LOG = Loggers.forClass(ClientComputeExecuteRequest.class);

    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param compute Compute.
     * @param cluster Cluster.
     * @param notificationSender Notification sender.
     * @param clientContext Client context.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteComputeInternal compute,
            ClusterService cluster,
            NotificationSender notificationSender,
            ClientContext clientContext
    ) {
        Set<InternalClusterNode> candidates = unpackCandidateNodes(in, cluster);

        Job job = unpackJob(in, clientContext.hasFeature(PLATFORM_COMPUTE_JOB));
        UUID taskId = unpackTaskId(in, clientContext.hasFeature(COMPUTE_TASK_ID));

        ComputeEventMetadataBuilder metadataBuilder = ComputeEventMetadata.builder(taskId != null ? Type.BROADCAST : Type.SINGLE)
                .eventUser(clientContext.userDetails())
                .taskId(taskId)
                .clientAddress(clientContext.remoteAddress().toString());

        CompletableFuture<JobExecution<ComputeJobDataHolder>> executionFut = compute.executeAsyncWithFailover(
                candidates, new ExecutionContext(job.options(), job.deploymentUnits(), job.jobClassName(), metadataBuilder, job.arg()), null
        );
        sendResultAndState(executionFut, notificationSender);

        //noinspection DataFlowIssue
        return executionFut.thenCompose(execution ->
                execution.idAsync().thenApply(jobId -> out -> packSubmitResult(out, jobId, execution.node()))
        );
    }

    private static Set<InternalClusterNode> unpackCandidateNodes(ClientMessageUnpacker in, ClusterService cluster) {
        int size = in.unpackInt();

        if (size < 1) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        Set<String> nodeNames = new HashSet<>(size);
        Set<InternalClusterNode> nodes = new HashSet<>(size);

        for (int i = 0; i < size; i++) {
            String nodeName = in.unpackString();
            nodeNames.add(nodeName);
            InternalClusterNode node = cluster.topologyService().getByConsistentId(nodeName);
            if (node != null) {
                nodes.add(node);
            }
        }

        if (nodes.isEmpty()) {
            throw new NodeNotFoundException(nodeNames);
        }

        return nodes;
    }

    static CompletableFuture<ComputeJobDataHolder> sendResultAndState(
            CompletableFuture<JobExecution<ComputeJobDataHolder>> executionFut,
            NotificationSender notificationSender
    ) {
        return executionFut.handle((execution, throwable) -> {
            if (throwable != null) {
                notificationSender.sendNotification(null, throwable, NULL_HYBRID_TIMESTAMP);
                return CompletableFuture.<ComputeJobDataHolder>failedFuture(throwable);
            } else {
                return execution.resultAsync().whenComplete((val, err) ->
                        execution.stateAsync().whenComplete((state, errState) -> {
                            try {
                                notificationSender.sendNotification(
                                        w -> {
                                            Marshaller<Object, byte[]> marshaller = extractMarshaller(execution);
                                            ClientComputeJobPacker.packJobResult(val, marshaller, w);
                                            packJobState(w, state);
                                        },
                                        err,
                                        hybridTimestamp(val));
                            } catch (Throwable e) {
                                LOG.error("Failed to send job result notification: " + e.getMessage(), e);
                            }
                        }));
            }
        }).thenCompose(Function.identity());
    }

    static void packSubmitResult(ClientMessagePacker out, UUID jobId, ClusterNode node) {
        out.packUuid(jobId);
        packClusterNode(node, out);
    }

    private static long hybridTimestamp(ComputeJobDataHolder holder) {
        if (holder == null) {
            return NULL_HYBRID_TIMESTAMP;
        }

        Long observableTimestamp = holder.observableTimestamp();

        return observableTimestamp == null ? NULL_HYBRID_TIMESTAMP : observableTimestamp;
    }

    private static <T> @Nullable Marshaller<T, byte[]> extractMarshaller(JobExecution<ComputeJobDataHolder> e) {
        if (e instanceof MarshallerProvider) {
            return ((MarshallerProvider<T>) e).resultMarshaller();
        }

        return null;
    }
}
