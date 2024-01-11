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

package org.apache.ignite.internal.compute.messaging;

import static org.apache.ignite.internal.compute.ComputeUtils.cancelFromJobCancelResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.jobIdFromExecuteResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.resultFromJobResultResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.statusFromJobStatusResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.toDeploymentUnit;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.compute.ComputeMessagesFactory;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelRequest;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobResultRequest;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStatusRequest;
import org.apache.ignite.internal.compute.message.JobStatusResponse;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Compute API internal messaging service.
 */
public class ComputeMessaging {
    private static final long NETWORK_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    private final ComputeComponentImpl computeComponent;

    private final MessagingService messagingService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    public ComputeMessaging(ComputeComponentImpl computeComponent, MessagingService messagingService) {
        this.computeComponent = computeComponent;
        this.messagingService = messagingService;
    }

    /**
     * Start messaging service.
     */
    public void start() {
        messagingService.addMessageHandler(ComputeMessageTypes.class, (message, senderConsistentId, correlationId) -> {
            assert correlationId != null;

            if (!busyLock.enterBusy()) {
                sendException(
                        message,
                        senderConsistentId,
                        correlationId,
                        new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException())
                );
                return;
            }

            try {
                processRequest(message, senderConsistentId, correlationId);
            } finally {
                busyLock.leaveBusy();
            }
        });
    }

    private void sendException(
            NetworkMessage message,
            String senderConsistentId,
            long correlationId,
            IgniteInternalException ex
    ) {
        if (message instanceof ExecuteRequest) {
            sendExecuteResponse(null, ex, senderConsistentId, correlationId);
        } else if (message instanceof JobResultRequest) {
            sendJobResultResponse(null, ex, senderConsistentId, correlationId);
        } else if (message instanceof JobStatusRequest) {
            sendJobStatusResponse(null, ex, senderConsistentId, correlationId);
        } else if (message instanceof JobCancelRequest) {
            sendJobCancelResponse(ex, senderConsistentId, correlationId);
        }
    }

    private void processRequest(
            NetworkMessage message,
            String senderConsistentId,
            long correlationId
    ) {
        if (message instanceof ExecuteRequest) {
            processExecuteRequest((ExecuteRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobResultRequest) {
            processJobResultRequest((JobResultRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobStatusRequest) {
            processJobStatusRequest((JobStatusRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobCancelRequest) {
            processJobCancelRequest((JobCancelRequest) message, senderConsistentId, correlationId);
        }
    }

    /**
     * Stop messaging service. After stop this service is not usable anymore.
     */
    public void stop() {
        busyLock.block();
    }

    /**
     * Submit Compute job to execution on remote node.
     *
     * @param options Job execution options.
     * @param remoteNode The job will be executed on this node.
     * @param units Deployment units. Can be empty.
     * @param jobClassName Name of the job class to execute.
     * @param args Arguments of the job.
     * @return Job id future that will be completed when the job is submitted on the remote node.
     */
    public CompletableFuture<UUID> remoteExecuteRequestAsync(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        List<DeploymentUnitMsg> deploymentUnitMsgs = units.stream()
                .map(ComputeUtils::toDeploymentUnitMsg)
                .collect(Collectors.toList());

        ExecuteRequest executeRequest = messagesFactory.executeRequest()
                .executeOptions(options)
                .deploymentUnits(deploymentUnitMsgs)
                .jobClassName(jobClassName)
                .args(args)
                .build();

        return messagingService.invoke(remoteNode, executeRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> jobIdFromExecuteResponse((ExecuteResponse) networkMessage));
    }

    private void processExecuteRequest(
            ExecuteRequest request,
            String senderConsistentId,
            long correlationId
    ) {
        List<DeploymentUnit> units = toDeploymentUnit(request.deploymentUnits());

        JobExecution<Object> execution = computeComponent.executeLocally(
                request.executeOptions(),
                units,
                request.jobClassName(),
                request.args()
        );
        execution.idAsync().whenComplete((jobId, err) -> sendExecuteResponse(jobId, err, senderConsistentId, correlationId));
    }

    private void sendExecuteResponse(@Nullable UUID jobId, @Nullable Throwable ex, String senderConsistentId, Long correlationId) {
        ExecuteResponse executeResponse = messagesFactory.executeResponse()
                .jobId(jobId)
                .throwable(ex)
                .build();

        messagingService.respond(senderConsistentId, executeResponse, correlationId);
    }

    /**
     * Gets compute job execution result from the remote node.
     *
     * @param remoteNode The job will be executed on this node.
     * @param jobId Job id.
     * @param <R> Job result type
     * @return Job result.
     */
    public <R> CompletableFuture<R> remoteJobResultRequestAsync(ClusterNode remoteNode, UUID jobId) {
        JobResultRequest jobResultRequest = messagesFactory.jobResultRequest()
                .jobId(jobId)
                .build();

        return messagingService.invoke(remoteNode, jobResultRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> resultFromJobResultResponse((JobResultResponse) networkMessage));
    }

    private void processJobResultRequest(
            JobResultRequest request,
            String senderConsistentId,
            long correlationId
    ) {
        computeComponent.resultAsync(request.jobId())
                .whenComplete((result, err) -> sendJobResultResponse(result, err, senderConsistentId, correlationId));
    }

    private void sendJobResultResponse(
            @Nullable Object result,
            @Nullable Throwable ex,
            String senderConsistentId,
            long correlationId
    ) {
        JobResultResponse jobResultResponse = messagesFactory.jobResultResponse()
                .result(result)
                .throwable(ex)
                .build();

        messagingService.respond(senderConsistentId, jobResultResponse, correlationId);
    }

    /**
     * Gets compute job status from the remote node.
     *
     * @param remoteNode The job will be executed on this node.
     * @param jobId Compute job id.
     * @return Job status future.
     */
    CompletableFuture<JobStatus> remoteStatusAsync(ClusterNode remoteNode, UUID jobId) {
        JobStatusRequest jobStatusRequest = messagesFactory.jobStatusRequest()
                .jobId(jobId)
                .build();

        return messagingService.invoke(remoteNode, jobStatusRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> statusFromJobStatusResponse((JobStatusResponse) networkMessage));
    }

    private void processJobStatusRequest(
            JobStatusRequest request,
            String senderConsistentId,
            long correlationId
    ) {
        computeComponent.statusAsync(request.jobId())
                .whenComplete((status, throwable) -> sendJobStatusResponse(status, throwable, senderConsistentId, correlationId));
    }

    private void sendJobStatusResponse(
            @Nullable JobStatus status,
            @Nullable Throwable throwable,
            String senderConsistentId,
            Long correlationId
    ) {
        JobStatusResponse jobStatusResponse = messagesFactory.jobStatusResponse()
                .status(status)
                .throwable(throwable)
                .build();

        messagingService.respond(senderConsistentId, jobStatusResponse, correlationId);
    }

    /**
     * Cancels compute job on the remote node.
     *
     * @param remoteNode The job will be canceled on this node.
     * @param jobId Compute job id.
     * @return Job cancel future (will be completed when cancel request is processed).
     */
    CompletableFuture<Void> remoteCancelAsync(ClusterNode remoteNode, UUID jobId) {
        JobCancelRequest jobCancelRequest = messagesFactory.jobCancelRequest()
                .jobId(jobId)
                .build();

        return messagingService.invoke(remoteNode, jobCancelRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> cancelFromJobCancelResponse((JobCancelResponse) networkMessage));
    }

    private void processJobCancelRequest(JobCancelRequest request, String senderConsistentId, long correlationId) {
        computeComponent.cancelAsync(request.jobId())
                .whenComplete((result, err) -> sendJobCancelResponse(err, senderConsistentId, correlationId));
    }

    private void sendJobCancelResponse(
            @Nullable Throwable throwable,
            String senderConsistentId,
            Long correlationId
    ) {
        JobCancelResponse jobCancelResponse = messagesFactory.jobCancelResponse()
                .throwable(throwable)
                .build();

        messagingService.respond(senderConsistentId, jobCancelResponse, correlationId);
    }
}
