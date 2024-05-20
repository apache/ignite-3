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

import static org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest.unpackArgs;
import static org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest.unpackDeploymentUnits;
import static org.apache.ignite.client.handler.requests.compute.ClientComputeGetStatusRequest.packJobStatus;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.NotificationSender;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.compute.IgniteComputeInternal;

/**
 * Compute MapReduce request.
 */
public class ClientComputeExecuteMapReduceRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param compute Compute.
     * @param notificationSender Notification sender.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            IgniteComputeInternal compute,
            NotificationSender notificationSender) {
        List<DeploymentUnit> deploymentUnits = unpackDeploymentUnits(in);
        String taskClassName = in.unpackString();
        Object[] args = unpackArgs(in);

        TaskExecution<Object> execution = compute.submitMapReduce(deploymentUnits, taskClassName, args);
        sendTaskResult(execution, notificationSender);

        var idsAsync = execution.idsAsync()
                .handle((ids, ex) -> {
                    // empty ids in case of split exception to properly respond with task id and failed status
                    return ex == null ? ids : Collections.<UUID>emptyList();
                });

        return execution.idAsync()
                .thenAcceptBoth(idsAsync, (id, ids) -> {
                    out.packUuid(id);
                    packJobIds(out, ids);
                });
    }

    static void packJobIds(ClientMessagePacker out, List<UUID> ids) {
        out.packInt(ids.size());
        for (var uuid : ids) {
            out.packUuid(uuid);
        }
    }

    static CompletableFuture<Object> sendTaskResult(TaskExecution<Object> execution, NotificationSender notificationSender) {
        return execution.resultAsync().whenComplete((val, err) ->
                execution.statusAsync().whenComplete((status, errStatus) ->
                        execution.statusesAsync().whenComplete((statuses, errStatuses) ->
                                notificationSender.sendNotification(w -> {
                                    w.packObjectAsBinaryTuple(val);
                                    packJobStatus(w, status);
                                    packJobStatuses(w, statuses);
                                }, firstNotNull(err, errStatus, errStatuses)))
                ));
    }

    static void packJobStatuses(ClientMessagePacker w, List<JobStatus> statuses) {
        w.packInt(statuses.size());
        for (JobStatus status : statuses) {
            packJobStatus(w, status);
        }
    }
}
