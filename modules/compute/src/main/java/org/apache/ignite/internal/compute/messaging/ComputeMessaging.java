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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.compute.ComputeUtils.cancelFromJobCancelResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.changePriorityFromJobChangePriorityResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.jobIdFromExecuteResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.resultFromJobResultResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.stateFromJobStateResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.statesFromJobStatesResponse;
import static org.apache.ignite.internal.compute.ComputeUtils.toDeploymentUnit;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.CANCELLING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.CHANGE_JOB_PRIORITY_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.FAIL_TO_GET_JOB_STATE_ERR;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.compute.ComputeMessagesFactory;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionManager;
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
import org.apache.ignite.internal.compute.message.JobStateRequest;
import org.apache.ignite.internal.compute.message.JobStateResponse;
import org.apache.ignite.internal.compute.message.JobStatesRequest;
import org.apache.ignite.internal.compute.message.JobStatesResponse;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Compute API internal messaging service.
 */
public class ComputeMessaging {
    private static final long NETWORK_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    private final ExecutionManager executionManager;

    private final MessagingService messagingService;

    private final TopologyService topologyService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Constructor.
     *
     * @param executionManager Execution manager.
     * @param messagingService Messaging service.
     * @param topologyService Topology service.
     */
    public ComputeMessaging(ExecutionManager executionManager, MessagingService messagingService, TopologyService topologyService) {
        this.executionManager = executionManager;
        this.messagingService = messagingService;
        this.topologyService = topologyService;
    }

    /**
     * Start messaging service.
     */
    public void start(JobStarter starter) {
        messagingService.addMessageHandler(ComputeMessageTypes.class, (message, sender, correlationId) -> {
            assert correlationId != null;

            if (!busyLock.enterBusy()) {
                sendException(
                        message,
                        sender,
                        requireNonNull(correlationId, "correlationId is null"),
                        new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException())
                );
                return;
            }

            try {
                processRequest(message, sender, requireNonNull(correlationId), starter);
            } finally {
                busyLock.leaveBusy();
            }
        });
    }

    private void sendException(NetworkMessage message, ClusterNode sender, long correlationId, IgniteInternalException ex) {
        if (message instanceof ExecuteRequest) {
            sendExecuteResponse(null, ex, sender, correlationId);
        } else if (message instanceof JobResultRequest) {
            sendJobResultResponse(null, ex, sender, correlationId);
        } else if (message instanceof JobStatesRequest) {
            sendJobStatesResponse(null, ex, sender, correlationId);
        } else if (message instanceof JobStateRequest) {
            sendJobStateResponse(null, ex, sender, correlationId);
        } else if (message instanceof JobCancelRequest) {
            sendJobCancelResponse(null, ex, sender, correlationId);
        } else if (message instanceof JobChangePriorityRequest) {
            sendJobChangePriorityResponse(null, ex, sender, correlationId);
        }
    }

    private void processRequest(NetworkMessage message, ClusterNode sender, long correlationId, JobStarter starter) {
        if (message instanceof ExecuteRequest) {
            processExecuteRequest(starter, (ExecuteRequest) message, sender, correlationId);
        } else if (message instanceof JobResultRequest) {
            processJobResultRequest((JobResultRequest) message, sender, correlationId);
        } else if (message instanceof JobStatesRequest) {
            processJobStatesRequest((JobStatesRequest) message, sender, correlationId);
        } else if (message instanceof JobStateRequest) {
            processJobStateRequest((JobStateRequest) message, sender, correlationId);
        } else if (message instanceof JobCancelRequest) {
            processJobCancelRequest((JobCancelRequest) message, sender, correlationId);
        } else if (message instanceof JobChangePriorityRequest) {
            processJobChangePriorityRequest((JobChangePriorityRequest) message, sender, correlationId);
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
     * @param input Arguments of the job.
     * @return Job id future that will be completed when the job is submitted on the remote node.
     */
    public <T> CompletableFuture<UUID> remoteExecuteRequestAsync(
            ExecutionOptions options,
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            T input
    ) {
        List<DeploymentUnitMsg> deploymentUnitMsgs = units.stream()
                .map(ComputeUtils::toDeploymentUnitMsg)
                .collect(toList());

        ExecuteRequest executeRequest = messagesFactory.executeRequest()
                .executeOptions(options)
                .deploymentUnits(deploymentUnitMsgs)
                .jobClassName(jobClassName)
                .input(input)
                .build();

        return messagingService.invoke(remoteNode, executeRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> jobIdFromExecuteResponse((ExecuteResponse) networkMessage));
    }

    private void processExecuteRequest(JobStarter starter, ExecuteRequest request, ClusterNode sender, long correlationId) {
        List<DeploymentUnit> units = toDeploymentUnit(request.deploymentUnits());

        JobExecution<Object> execution = starter.start(
                request.executeOptions(), units, request.jobClassName(), request.input()
        );
        execution.idAsync().whenComplete((jobId, err) -> sendExecuteResponse(jobId, err, sender, correlationId));
    }

    private void sendExecuteResponse(@Nullable UUID jobId, @Nullable Throwable ex, ClusterNode sender, Long correlationId) {
        ExecuteResponse executeResponse = messagesFactory.executeResponse()
                .jobId(jobId)
                .throwable(ex)
                .build();

        messagingService.respond(sender, executeResponse, correlationId);
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

    private void processJobResultRequest(JobResultRequest request, ClusterNode sender, long correlationId) {
        executionManager.resultAsync(request.jobId())
                .whenComplete((result, err) -> sendJobResultResponse(result, err, sender, correlationId));
    }

    private void sendJobResultResponse(@Nullable Object result, @Nullable Throwable ex, ClusterNode sender, long correlationId) {
        JobResultResponse jobResultResponse = messagesFactory.jobResultResponse()
                .result(result)
                .throwable(ex)
                .build();

        messagingService.respond(sender, jobResultResponse, correlationId);
    }

    CompletableFuture<Collection<JobState>> remoteStatesAsync(ClusterNode remoteNode) {
        JobStatesRequest jobStatesRequest = messagesFactory.jobStatesRequest()
                .build();

        return messagingService.invoke(remoteNode, jobStatesRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> statesFromJobStatesResponse((JobStatesResponse) networkMessage));
    }

    private void processJobStatesRequest(JobStatesRequest message, ClusterNode sender, long correlationId) {
        executionManager.localStatesAsync()
                .whenComplete((states, throwable) -> sendJobStatesResponse(states, throwable, sender, correlationId));
    }

    private void sendJobStatesResponse(
            @Nullable Collection<JobState> states,
            @Nullable Throwable throwable,
            ClusterNode sender,
            Long correlationId
    ) {
        JobStatesResponse jobStatesResponse = messagesFactory.jobStatesResponse()
                .states(states)
                .throwable(throwable)
                .build();

        messagingService.respond(sender, jobStatesResponse, correlationId);
    }

    /**
     * Gets compute job state from the remote node.
     *
     * @param remoteNode The job will be executed on this node.
     * @param jobId Compute job id.
     * @return The current state of the job, or {@code null} if there's no job with the specified id.
     */
    CompletableFuture<@Nullable JobState> remoteStateAsync(ClusterNode remoteNode, UUID jobId) {
        JobStateRequest jobStateRequest = messagesFactory.jobStateRequest()
                .jobId(jobId)
                .build();

        return messagingService.invoke(remoteNode, jobStateRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> stateFromJobStateResponse((JobStateResponse) networkMessage));
    }

    private void processJobStateRequest(JobStateRequest request, ClusterNode sender, long correlationId) {
        executionManager.stateAsync(request.jobId())
                .whenComplete((state, throwable) -> sendJobStateResponse(state, throwable, sender, correlationId));
    }

    private void sendJobStateResponse(@Nullable JobState state, @Nullable Throwable throwable, ClusterNode sender, Long correlationId) {
        JobStateResponse jobStateResponse = messagesFactory.jobStateResponse()
                .state(state)
                .throwable(throwable)
                .build();

        messagingService.respond(sender, jobStateResponse, correlationId);
    }

    /**
     * Cancels compute job on the remote node.
     *
     * @param remoteNode The job will be canceled on this node.
     * @param jobId Compute job id.
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (either it's not yet started, or it's already completed), or {@code null} if there's no job with the specified id.
     */
    CompletableFuture<@Nullable Boolean> remoteCancelAsync(ClusterNode remoteNode, UUID jobId) {
        JobCancelRequest jobCancelRequest = messagesFactory.jobCancelRequest()
                .jobId(jobId)
                .build();

        return messagingService.invoke(remoteNode, jobCancelRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> cancelFromJobCancelResponse((JobCancelResponse) networkMessage));
    }

    private void processJobCancelRequest(JobCancelRequest request, ClusterNode sender, long correlationId) {
        executionManager.cancelAsync(request.jobId())
                .whenComplete((result, err) -> sendJobCancelResponse(result, err, sender, correlationId));
    }

    private void sendJobCancelResponse(@Nullable Boolean result, @Nullable Throwable throwable, ClusterNode sender, Long correlationId) {
        JobCancelResponse jobCancelResponse = messagesFactory.jobCancelResponse()
                .result(result)
                .throwable(throwable)
                .build();

        messagingService.respond(sender, jobCancelResponse, correlationId);
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
    CompletableFuture<@Nullable Boolean> remoteChangePriorityAsync(ClusterNode remoteNode, UUID jobId, int newPriority) {
        JobChangePriorityRequest jobChangePriorityRequest = messagesFactory.jobChangePriorityRequest()
                .jobId(jobId)
                .priority(newPriority)
                .build();

        return messagingService.invoke(remoteNode, jobChangePriorityRequest, NETWORK_TIMEOUT_MILLIS)
                .thenCompose(networkMessage -> changePriorityFromJobChangePriorityResponse((JobChangePriorityResponse) networkMessage));
    }

    private void processJobChangePriorityRequest(JobChangePriorityRequest request, ClusterNode sender, long correlationId) {
        executionManager.changePriorityAsync(request.jobId(), request.priority())
                .whenComplete((result, err) -> sendJobChangePriorityResponse(result, err, sender, correlationId));
    }

    private void sendJobChangePriorityResponse(
            @Nullable Boolean result,
            @Nullable Throwable throwable,
            ClusterNode sender,
            Long correlationId
    ) {
        JobChangePriorityResponse jobChangePriorityResponse = messagesFactory.jobChangePriorityResponse()
                .throwable(throwable)
                .result(result)
                .build();

        messagingService.respond(sender, jobChangePriorityResponse, correlationId);
    }

    /**
     * Broadcasts job states request to all nodes in the cluster.
     *
     * @return The future which will be completed with the collection of states from all nodes.
     */
    public CompletableFuture<Collection<JobState>> broadcastStatesAsync() {
        return broadcastAsyncAndCollect(
                this::remoteStatesAsync,
                throwable -> new ComputeException(
                        FAIL_TO_GET_JOB_STATE_ERR,
                        "Failed to retrieve states",
                        throwable
                )
        ).thenApply(states -> states.stream()
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .collect(toList()));
    }

    /**
     * Broadcasts job state request to all nodes in the cluster.
     *
     * @param jobId Job id.
     * @return The current state of the job, or {@code null} if the job state no longer exists due to exceeding the retention time limit.
     */
    public CompletableFuture<@Nullable JobState> broadcastStateAsync(UUID jobId) {
        return broadcastAsync(
                node -> remoteStateAsync(node, jobId),
                throwable -> new ComputeException(
                        FAIL_TO_GET_JOB_STATE_ERR,
                        "Failed to retrieve state of the job with ID: " + jobId,
                        throwable
                ));
    }

    /**
     * Broadcasts job cancel request to all nodes in the cluster.
     *
     * @param jobId Job id.
     * @return The future which will be completed with {@code true} when the job is cancelled, {@code false} when the job couldn't be
     *         cancelled (either it's not yet started, or it's already completed), or {@code null} if there's no job with the specified id.
     */
    public CompletableFuture<@Nullable Boolean> broadcastCancelAsync(UUID jobId) {
        return broadcastAsync(
                node -> remoteCancelAsync(node, jobId),
                throwable -> new ComputeException(
                        CANCELLING_ERR,
                        "Failed to cancel job with ID: " + jobId,
                        throwable
                ));
    }

    /**
     * Broadcasts job priority change request to all nodes in the cluster.
     *
     * @param jobId Job id.
     * @param newPriority New priority.
     * @return The future which will be completed with {@code true} when the priority is changed, {@code false} when the priority couldn't
     *         be changed (it's already executing or completed), or {@code null} if there's no job with the specified id.
     */
    public CompletableFuture<@Nullable Boolean> broadcastChangePriorityAsync(UUID jobId, int newPriority) {
        return broadcastAsync(
                node -> remoteChangePriorityAsync(node, jobId, newPriority),
                throwable -> new ComputeException(
                        CHANGE_JOB_PRIORITY_ERR,
                        "Failed to change priority for job with ID: " + jobId,
                        throwable
                ));
    }

    /**
     * Broadcasts a request to all nodes in the cluster.
     *
     * @param request Function which maps a node to the request future.
     * @param error Function which creates a specific error from the exception thrown from the request.
     * @return The future which will be completed when request is processed.
     */
    private <R> CompletableFuture<@Nullable R> broadcastAsync(
            Function<ClusterNode, CompletableFuture<@Nullable R>> request,
            Function<Throwable, Throwable> error
    ) {
        CompletableFuture<@Nullable R> result = new CompletableFuture<>();

        ClusterNode localMember = topologyService.localMember();
        CompletableFuture<?>[] futures = topologyService.allMembers()
                .stream()
                .filter(node -> !node.equals(localMember))
                .map(node -> request.apply(node)
                        .thenAccept(response -> {
                            if (response != null) {
                                result.complete(response);
                            }
                        }))
                .toArray(CompletableFuture[]::new);

        allOf(futures).whenComplete((unused, throwable) -> {
            // If none of the nodes returned non-null state it means that either we couldn't find the state
            // or the node which had the state thrown an exception. If any of the futures completed exceptionally
            // but the result is non-null, then ignore the exceptions from other futures.
            if (!result.isDone()) {
                // allOf will complete exceptionally if any of the futures failed, so this condition means that we
                // successfully couldn't find a state.
                if (throwable == null) {
                    result.complete(null);
                    return;
                }
                result.completeExceptionally(error.apply(throwable));
            }
        });

        return result;
    }

    private <R> CompletableFuture<List<R>> broadcastAsyncAndCollect(
            Function<ClusterNode, CompletableFuture<@Nullable R>> request,
            Function<Throwable, RuntimeException> error
    ) {
        CompletableFuture<R>[] futures = topologyService.allMembers()
                .stream()
                .map(request::apply)
                .toArray(CompletableFuture[]::new);

        return allOfToList(futures).exceptionally(throwable -> {
            throw error.apply(throwable);
        });
    }
}
