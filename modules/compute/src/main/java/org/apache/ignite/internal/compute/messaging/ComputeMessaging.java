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
import static org.apache.ignite.internal.compute.ComputeUtils.changePriorityFromJobChangePriorityResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.jobIdFromExecuteResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.resultFromJobResultResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.statusFromJobStatusResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.toDeploymentUnit;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.compute.ComputeMessagesFactory;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.JobStarter;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelRequest;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobChangePriorityRequest;
import org.apache.ignite.internal.compute.message.JobChangePriorityResponse;
import org.apache.ignite.internal.compute.message.JobResultRequest;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStatusRequest;
import org.apache.ignite.internal.compute.message.JobStatusResponse;
import org.apache.ignite.internal.compute.queue.CancellingException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ErrorGroups.Compute;
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

    private final MessagingService messagingService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    //TODO https://issues.apache.org/jira/browse/IGNITE-21168
    private final Map<UUID, JobExecution<Object>> executions = new ConcurrentHashMap<>();

    public ComputeMessaging(MessagingService messagingService) {
        this.messagingService = messagingService;
    }

    /**
     * Start messaging service.
     *
     * @param starter Compute job starter.
     */
    public void start(JobStarter starter, Function<UUID, JobStatus> jobStatus) {
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
                processRequest(message, senderConsistentId, correlationId, starter, jobStatus);
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
        } else if (message instanceof JobChangePriorityRequest) {
            sendJobChangePriorityResponse(ex, senderConsistentId, correlationId);
        }
    }

    private void processRequest(
            NetworkMessage message,
            String senderConsistentId,
            long correlationId,
            JobStarter starter,
            Function<UUID, JobStatus> jobStatus
    ) {
        if (message instanceof ExecuteRequest) {
            processExecuteRequest(starter, (ExecuteRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobResultRequest) {
            processJobResultRequest((JobResultRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobStatusRequest) {
            processJobStatusRequest(jobStatus, (JobStatusRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobCancelRequest) {
            processJobCancelRequest((JobCancelRequest) message, senderConsistentId, correlationId);
        } else if (message instanceof JobChangePriorityRequest) {
            processJobChangePriorityRequest((JobChangePriorityRequest) message, senderConsistentId, correlationId);
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

    private void processExecuteRequest(JobStarter starter, ExecuteRequest request, String senderConsistentId, long correlationId) {
        List<DeploymentUnit> units = toDeploymentUnit(request.deploymentUnits());

        JobExecution<Object> execution = starter.start(request.executeOptions(), units, request.jobClassName(), request.args());
        execution.idAsync().whenComplete((jobId, err) -> {
            if (jobId != null) {
                executions.put(jobId, execution);
            }
            sendExecuteResponse(jobId, err, senderConsistentId, correlationId);
        });
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

    private void processJobResultRequest(JobResultRequest request, String senderConsistentId, long correlationId) {
        UUID jobId = request.jobId();
        JobExecution<Object> execution = executions.get(jobId);
        if (execution != null) {
            execution.resultAsync()
                    .whenComplete((result, err) -> sendJobResultResponse(result, err, senderConsistentId, correlationId)
                            .whenComplete((unused, throwable) -> executions.remove(jobId)));
        } else {
            ComputeException ex = new ComputeException(Compute.RESULT_NOT_FOUND_ERR, "Job result not found for the job id " + jobId);
            sendJobResultResponse(null, ex, senderConsistentId, correlationId);
        }
    }

    private CompletableFuture<Void> sendJobResultResponse(
            @Nullable Object result,
            @Nullable Throwable ex,
            String senderConsistentId,
            long correlationId
    ) {
        JobResultResponse jobResultResponse = messagesFactory.jobResultResponse()
                .result(result)
                .throwable(ex)
                .build();

        return messagingService.respond(senderConsistentId, jobResultResponse, correlationId);
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
            Function<UUID, JobStatus> jobStatus,
            JobStatusRequest request,
            String senderConsistentId,
            long correlationId
    ) {
        sendJobStatusResponse(jobStatus.apply(request.jobId()), null, senderConsistentId, correlationId);
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
        UUID jobId = request.jobId();
        JobExecution<Object> execution = executions.get(jobId);
        if (execution != null) {
            execution.cancelAsync()
                    .whenComplete((result, err) -> sendJobCancelResponse(err, senderConsistentId, correlationId));
        } else {
            sendJobCancelResponse(new CancellingException(jobId), senderConsistentId, correlationId);
        }
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

    /**
     * Changes compute job priority on the remote node.
     *
     * @param remoteNode The priority of the job will be changed on this node.
     * @param jobId Compute job id.
     * @param newPriority new job priority.
     *
     * @return Job change priority future (will be completed when change priority request is processed).
     */
    CompletableFuture<Void> remoteChangePriorityAsync(ClusterNode remoteNode, UUID jobId, int newPriority) {
        JobChangePriorityRequest jobChangePriorityRequest = messagesFactory.jobChangePriorityRequest()
                .jobId(jobId)
                .priority(newPriority)
                .build();

        return messagingService.invoke(remoteNode, jobChangePriorityRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> changePriorityFromJobChangePriorityResponse((JobChangePriorityResponse) networkMessage));
    }

    private void processJobChangePriorityRequest(JobChangePriorityRequest request, String senderConsistentId, long correlationId) {
        UUID jobId = request.jobId();
        JobExecution<Object> execution = executions.get(jobId);
        if (execution != null) {
            execution.changePriorityAsync(request.priority())
                    .whenComplete((result, err) -> sendJobChangePriorityResponse(err, senderConsistentId, correlationId));
        } else {
            ComputeException ex = new ComputeException(Compute.CHANGE_JOB_PRIORITY_NO_JOB_ERR, "Can not change job priority,"
                    + " job not found for the job id " + jobId);
            sendJobChangePriorityResponse(ex, senderConsistentId, correlationId);
        }
    }

    private void sendJobChangePriorityResponse(
            @Nullable Throwable throwable,
            String senderConsistentId,
            Long correlationId
    ) {
        JobChangePriorityResponse jobChangePriorityResponse = messagesFactory.jobChangePriorityResponse()
                .throwable(throwable)
                .build();

        messagingService.respond(senderConsistentId, jobChangePriorityResponse, correlationId);
    }
}
