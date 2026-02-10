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

import static org.apache.ignite.client.handler.requests.compute.ClientComputeGetStateRequest.packJobState;
import static org.apache.ignite.client.handler.requests.compute.ClientComputeGetStateRequest.packTaskState;
import static org.apache.ignite.internal.client.proto.ClientComputeJobUnpacker.unpackJobArgumentWithoutMarshaller;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientContext;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.ClientComputeJobPacker;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.compute.ComputeJobDataHolder;
import org.apache.ignite.internal.compute.HybridTimestampProvider;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.MarshallerProvider;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.ComputeEventMetadataBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.marshalling.Marshaller;

/**
 * Compute MapReduce request.
 */
public class ClientComputeExecuteMapReduceRequest {
    private static final IgniteLogger LOG = Loggers.forClass(ClientComputeExecuteMapReduceRequest.class);

    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param compute Compute.
     * @param notificationSender Notification sender.
     * @param clientContext Client context.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            IgniteComputeInternal compute,
            NotificationSender notificationSender,
            ClientContext clientContext
    ) {
        List<DeploymentUnit> deploymentUnits = in.unpackDeploymentUnits();
        String taskClassName = in.unpackString();

        boolean enableObservableTs = clientContext.hasFeature(ProtocolBitmaskFeature.COMPUTE_OBSERVABLE_TS);
        ComputeJobDataHolder arg = unpackJobArgumentWithoutMarshaller(in, enableObservableTs);

        TaskDescriptor<Object, Object> taskDescriptor = TaskDescriptor.builder(taskClassName).units(deploymentUnits).build();

        ComputeEventMetadataBuilder metadataBuilder = ComputeEventMetadata.builder(Type.MAP_REDUCE)
                .eventUser(clientContext.userDetails())
                .clientAddress(clientContext.remoteAddress().toString());

        TaskExecution<Object> execution = compute.submitMapReduceInternal(taskDescriptor, metadataBuilder, arg, null);
        sendTaskResult(execution, notificationSender);

        var idsAsync = execution.idsAsync()
                .handle((ids, ex) -> {
                    // empty ids in case of split exception to properly respond with task id and failed status
                    return ex == null ? ids : Collections.<UUID>emptyList();
                });

        return execution.idAsync().thenCompose(id -> idsAsync.thenApply(ids -> out -> {
            //noinspection DataFlowIssue
            out.packUuid(id);
            packJobIds(out, ids);
        }));
    }

    private static void packJobIds(ClientMessagePacker out, List<UUID> ids) {
        out.packInt(ids.size());
        for (var uuid : ids) {
            out.packUuid(uuid);
        }
    }

    private static void sendTaskResult(TaskExecution<Object> execution, NotificationSender notificationSender) {
        execution.resultAsync().whenComplete((val, err) ->
                execution.stateAsync().whenComplete((state, errState) ->
                        execution.statesAsync().whenComplete((states, errStates) -> {
                            try {
                                notificationSender.sendNotification(
                                        w -> {
                                            Marshaller<Object, byte[]> resultMarshaller = ((MarshallerProvider<Object>) execution)
                                                    .resultMarshaller();

                                            ClientComputeJobPacker.packJobResult(val, resultMarshaller, w);
                                            packTaskState(w, state);
                                            packJobStates(w, states);
                                        },
                                        firstNotNull(err, errState, errStates),
                                        ((HybridTimestampProvider) execution).hybridTimestamp());

                            } catch (Throwable t) {
                                LOG.error("Failed to send task result notification: " + t.getMessage(), t);
                            }
                        })
                ));
    }

    private static void packJobStates(ClientMessagePacker w, List<JobState> states) {
        w.packInt(states.size());
        for (JobState state : states) {
            packJobState(w, state);
        }
    }
}
