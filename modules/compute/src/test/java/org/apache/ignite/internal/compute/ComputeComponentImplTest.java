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

package org.apache.ignite.internal.compute;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.QUEUED;
import static org.apache.ignite.internal.compute.ExecutionOptions.DEFAULT;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStateMatcher.jobStateWithStatus;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutor;
import org.apache.ignite.internal.compute.executor.ComputeExecutorImpl;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
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
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
@Timeout(10)
class ComputeComponentImplTest extends BaseIgniteAbstractTest {
    private static final String INSTANCE_NAME = "Ignite-0";

    @Mock
    private Ignite ignite;

    @Mock
    private MessagingService messagingService;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private TopologyService topologyService;

    @Mock
    private LogicalTopologyService logicalTopologyService;

    @InjectConfiguration("mock{threadPoolSize=1, threadPoolStopTimeoutMillis=100}")
    private ComputeConfiguration computeConfiguration;

    @Mock
    private JobContextManager jobContextManager;

    private ComputeComponent computeComponent;

    private final ClusterNode testNode = new ClusterNodeImpl(randomUUID(), "test", new NetworkAddress("test-host", 1));
    private final ClusterNode remoteNode = new ClusterNodeImpl(randomUUID(), "remote", new NetworkAddress("remote-host", 1));

    private final AtomicReference<NetworkMessageHandler> computeMessageHandlerRef = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        lenient().when(ignite.name()).thenReturn(INSTANCE_NAME);
        lenient().when(topologyService.localMember().name()).thenReturn(INSTANCE_NAME);

        JobClassLoader classLoader = new JobClassLoader(List.of(), new URL[0], getClass().getClassLoader());
        JobContext jobContext = new JobContext(classLoader, ignored -> {});
        lenient().when(jobContextManager.acquireClassLoader(anyList()))
                .thenReturn(completedFuture(jobContext));

        doAnswer(invocation -> {
            computeMessageHandlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(ComputeMessageTypes.class), any());

        InMemoryComputeStateMachine stateMachine = new InMemoryComputeStateMachine(computeConfiguration, INSTANCE_NAME);
        ComputeExecutor computeExecutor = new ComputeExecutorImpl(ignite, stateMachine, computeConfiguration);

        computeComponent = new ComputeComponentImpl(
                INSTANCE_NAME,
                messagingService,
                topologyService,
                logicalTopologyService,
                jobContextManager,
                computeExecutor,
                computeConfiguration
        );

        assertThat(computeComponent.startAsync(new ComponentContext()), willCompleteSuccessfully());
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));
    }

    @AfterEach
    void cleanup() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void executesLocally() {
        JobExecution<String> execution = computeComponent.executeLocally(List.of(), SimpleJob.class.getName(), "");

        assertThat(execution.resultAsync(), willBe("jobResponse"));
        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));
        assertThat(execution.changePriorityAsync(1), willBe(false));

        assertThatNoRequestsWereSent();
    }

    @Test
    void getsStateAndCancelsLocally() {
        JobExecution<String> execution = computeComponent.executeLocally(List.of(), LongJob.class.getName(), null);

        await().until(execution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        assertThat(execution.cancelAsync(), willBe(true));

        await().until(execution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        assertThatNoRequestsWereSent();
    }

    @Test
    void stateCancelAndChangePriorityTriesLocalNodeFirst() {
        JobExecution<String> runningExecution = computeComponent.executeLocally(List.of(), LongJob.class.getName(), null);
        await().until(runningExecution::stateAsync, willBe(jobStateWithStatus(EXECUTING)));

        JobExecution<String> queuedExecution = computeComponent.executeLocally(List.of(), LongJob.class.getName(), null);
        await().until(queuedExecution::stateAsync, willBe(jobStateWithStatus(QUEUED)));

        UUID jobId = queuedExecution.idAsync().join();

        assertThat(computeComponent.stateAsync(jobId), willBe(jobStateWithStatus(QUEUED)));
        assertThat(computeComponent.changePriorityAsync(jobId, 1), willBe(true));
        assertThat(computeComponent.cancelAsync(jobId), willBe(true));

        await().until(queuedExecution::stateAsync, willBe(jobStateWithStatus(CANCELED)));

        assertThatNoRequestsWereSent();
    }

    private void assertThatNoRequestsWereSent() {
        verify(messagingService, never()).invoke(anyString(), any(), anyLong());
    }

    @Test
    void executesLocallyWithException() {
        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> executeLocally(FailingJob.class.getName()).get()
        );

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesRemotelyUsingNetworkCommunication() {
        UUID jobId = randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);
        respondWithJobStateResponseWhenJobStateRequestIsSent(jobId, COMPLETED);
        respondWithJobCancelResponseWhenJobCancelRequestIsSent(jobId, false);

        JobExecution<String> execution = computeComponent.executeRemotely(
                remoteNode, List.of(), SimpleJob.class.getName(), "a"
        );
        assertThat(execution.resultAsync(), willBe("remoteResponse"));

        // Verify that second invocation of resultAsync will not result in the network communication (i.e. the result is cached locally)
        assertThat(execution.resultAsync(), willBe("remoteResponse"));

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(COMPLETED)));
        assertThat(execution.cancelAsync(), willBe(false));

        assertThatExecuteRequestWasSent(SimpleJob.class.getName(), "a");
        assertThatJobResultRequestWasSent(jobId);
        assertThatJobStateRequestWasSent(jobId);
        assertThatJobCancelRequestWasSent(jobId);
    }

    @Test
    void getsStateAndCancelsRemotelyUsingNetworkCommunication() {
        UUID jobId = randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);
        respondWithJobStateResponseWhenJobStateRequestIsSent(jobId, EXECUTING);
        respondWithJobCancelResponseWhenJobCancelRequestIsSent(jobId, true);

        JobExecution<String> execution = computeComponent.executeRemotely(remoteNode, List.of(), LongJob.class.getName(), null);

        assertThat(execution.stateAsync(), willBe(jobStateWithStatus(EXECUTING)));
        assertThat(execution.resultAsync(), willBe("remoteResponse"));
        assertThat(execution.cancelAsync(), willBe(true));

        assertThatExecuteRequestWasSent(LongJob.class.getName(), null);
        assertThatJobResultRequestWasSent(jobId);
        assertThatJobStateRequestWasSent(jobId);
        assertThatJobCancelRequestWasSent(jobId);
    }

    @Test
    void changePriorityRemotelyUsingNetworkCommunication() {
        UUID jobId = randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobChangePriorityResponseWhenJobChangePriorityRequestIsSent(jobId);

        JobExecution<String> execution = computeComponent.executeRemotely(remoteNode, List.of(), LongJob.class.getName(), null);

        assertThat(execution.changePriorityAsync(1), willBe(true));

        assertThatJobChangePriorityRequestWasSent(jobId);
    }

    private void respondWithExecuteResponseWhenExecuteRequestIsSent(UUID jobId) {
        ExecuteResponse executeResponse = new ComputeMessagesFactory().executeResponse()
                .jobId(jobId)
                .build();
        when(messagingService.invoke(anyString(), any(ExecuteRequest.class), anyLong()))
                .thenReturn(completedFuture(executeResponse));
    }

    private void respondWithJobResultResponseWhenJobResultRequestIsSent(UUID jobId) {
        JobResultResponse jobResultResponse = new ComputeMessagesFactory().jobResultResponse()
                .result("remoteResponse")
                .build();
        when(messagingService.invoke(anyString(), argThat(msg -> jobResultRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobResultResponse));
    }

    private void respondWithJobStateResponseWhenJobStateRequestIsSent(UUID jobId, JobStatus status) {
        JobStateResponse jobStateResponse = new ComputeMessagesFactory().jobStateResponse()
                .state(JobStateImpl.builder().id(jobId).status(status).createTime(Instant.now()).build())
                .build();
        when(messagingService.invoke(anyString(), argThat(msg -> jobStateRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobStateResponse));
    }

    private void respondWithJobCancelResponseWhenJobCancelRequestIsSent(UUID jobId, boolean result) {
        JobCancelResponse jobCancelResponse = new ComputeMessagesFactory().jobCancelResponse()
                .result(result)
                .build();
        when(messagingService.invoke(anyString(), argThat(msg -> jobCancelRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobCancelResponse));
    }

    private void respondWithJobChangePriorityResponseWhenJobChangePriorityRequestIsSent(UUID jobId) {
        JobChangePriorityResponse jobChangePriorityResponse = new ComputeMessagesFactory().jobChangePriorityResponse()
                .result(true)
                .build();
        when(messagingService.invoke(anyString(), argThat(msg -> jobChangePriorityRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobChangePriorityResponse));
    }

    private static boolean jobResultRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobResultRequest) {
            JobResultRequest jobResultRequest = (JobResultRequest) argument;
            return jobResultRequest.jobId() == jobId;
        }
        return false;
    }

    private static boolean jobStateRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobStateRequest) {
            JobStateRequest jobStateRequest = (JobStateRequest) argument;
            return jobStateRequest.jobId() == jobId;
        }
        return false;
    }

    private static boolean jobCancelRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobCancelRequest) {
            JobCancelRequest jobCancelRequest = (JobCancelRequest) argument;
            return jobCancelRequest.jobId() == jobId;
        }
        return false;
    }

    private static boolean jobChangePriorityRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobChangePriorityRequest) {
            JobChangePriorityRequest jobChangePriorityRequest = (JobChangePriorityRequest) argument;
            return jobChangePriorityRequest.jobId() == jobId;
        }
        return false;
    }

    private void assertThatExecuteRequestWasSent(String jobClassName, String args) {
        ExecuteRequest capturedRequest = invokeAndCaptureRequest(ExecuteRequest.class);

        assertThat(capturedRequest.jobClassName(), is(jobClassName));
        assertThat(capturedRequest.input(), is(equalTo(args)));
    }

    private void assertThatJobResultRequestWasSent(UUID jobId) {
        JobResultRequest capturedRequest = invokeAndCaptureRequest(JobResultRequest.class);

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobStateRequestWasSent(UUID jobId) {
        JobStateRequest capturedRequest = invokeAndCaptureRequest(JobStateRequest.class);

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobCancelRequestWasSent(UUID jobId) {
        JobCancelRequest capturedRequest = invokeAndCaptureRequest(JobCancelRequest.class);

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobChangePriorityRequestWasSent(UUID jobId) {
        JobChangePriorityRequest capturedRequest = invokeAndCaptureRequest(JobChangePriorityRequest.class);

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    @Test
    void executesRemotelyWithException() {
        UUID jobId = randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        JobResultResponse jobResultResponse = new ComputeMessagesFactory().jobResultResponse()
                .throwable(new JobException("Oops", new Exception()))
                .build();
        when(messagingService.invoke(anyString(), argThat(msg -> jobResultRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobResultResponse));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> executeRemotely(FailingJob.class.getName()).get()
        );

        assertThatExecuteRequestWasSent(FailingJob.class.getName(), null);

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesJobAndRespondsWhenGetsExecuteRequest() {
        ExecuteRequest executeRequest = new ComputeMessagesFactory().executeRequest()
                .executeOptions(DEFAULT)
                .deploymentUnits(List.of())
                .jobClassName(SimpleJob.class.getName())
                .input("")
                .build();
        ExecuteResponse executeResponse = sendRequestAndCaptureResponse(executeRequest, testNode, 123L);

        UUID jobId = executeResponse.jobId();
        assertThat(jobId, is(notNullValue()));
        assertThat(executeResponse.throwable(), is(nullValue()));

        JobResultRequest jobResultRequest = new ComputeMessagesFactory().jobResultRequest()
                .jobId(jobId)
                .build();
        JobResultResponse jobResultResponse = sendRequestAndCaptureResponse(jobResultRequest, testNode, 456L);

        assertThat(jobResultResponse.result(), is("jobResponse"));
        assertThat(jobResultResponse.throwable(), is(nullValue()));
    }

    @Test
    void stoppedComponentReturnsExceptionOnLocalExecutionAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<String> result = executeLocally(SimpleJob.class.getName());

        assertThat(result, willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }

    @Test
    void localExecutionReleasesStopLock() throws Exception {
        executeLocally(SimpleJob.class.getName()).get();

        assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () -> assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully())
        );
    }

    @Test
    void stoppedComponentReturnsExceptionOnRemoteExecutionAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        CompletableFuture<String> result = executeRemotely(SimpleJob.class.getName());

        assertThat(result, willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }

    @Test
    void remoteExecutionReleasesStopLock() throws Exception {
        UUID jobId = randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);

        executeRemotely(SimpleJob.class.getName()).get();

        assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () -> assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully())
        );
    }

    @Test
    void stoppedComponentReturnsExceptionOnExecuteRequestAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        ExecuteRequest request = new ComputeMessagesFactory().executeRequest()
                .executeOptions(DEFAULT)
                .deploymentUnits(List.of())
                .jobClassName(SimpleJob.class.getName())
                .input(new Object[]{"a", 42})
                .build();
        ExecuteResponse response = sendRequestAndCaptureResponse(request, testNode, 123L);

        assertThat(response.jobId(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobResultRequestAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        JobResultRequest jobResultRequest = new ComputeMessagesFactory().jobResultRequest()
                .jobId(randomUUID())
                .build();
        JobResultResponse response = sendRequestAndCaptureResponse(jobResultRequest, testNode, 123L);

        assertThat(response.result(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobStateRequestAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        JobStateRequest jobStateRequest = new ComputeMessagesFactory().jobStateRequest()
                .jobId(randomUUID())
                .build();
        JobStateResponse response = sendRequestAndCaptureResponse(jobStateRequest, testNode, 123L);

        assertThat(response.state(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobCancelRequestAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        JobCancelRequest jobCancelRequest = new ComputeMessagesFactory().jobCancelRequest()
                .jobId(randomUUID())
                .build();
        JobCancelResponse response = sendRequestAndCaptureResponse(jobCancelRequest, testNode, 123L);

        assertThat(response.result(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobChangePriorityRequestAttempt() {
        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        JobChangePriorityRequest jobChangePriorityRequest = new ComputeMessagesFactory().jobChangePriorityRequest()
                .jobId(randomUUID())
                .priority(1)
                .build();
        JobChangePriorityResponse response = sendRequestAndCaptureResponse(jobChangePriorityRequest, testNode, 123L);

        assertThat(response.result(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void executorThreadsAreNamedAccordingly() {
        assertThat(
                executeLocally(GetThreadNameJob.class.getName()),
                willBe(startsWith(NamedThreadFactory.threadPrefix(INSTANCE_NAME, "compute")))
        );
    }

    @Test
    void stopCausesCancellationExceptionOnLocalExecution() {
        // take the only executor thread
        executeLocally(LongJob.class.getName());

        // the corresponding task goes to work queue
        CompletableFuture<String> resultFuture = executeLocally(SimpleJob.class.getName());

        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        // now work queue is dropped to the floor, so the future should be resolved with a cancellation
        assertThat(resultFuture, willThrow(CancellationException.class));
    }

    @Test
    void stopCausesCancellationExceptionOnRemoteExecution() {
        respondWithExecuteResponseWhenExecuteRequestIsSent(randomUUID());
        respondWithIncompleteFutureWhenJobResultRequestIsSent();

        CompletableFuture<String> resultFuture = executeRemotely(SimpleJob.class.getName());

        assertThat(computeComponent.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(resultFuture, willThrow(CancellationException.class));
    }

    private void respondWithIncompleteFutureWhenJobResultRequestIsSent() {
        when(messagingService.invoke(anyString(), any(JobResultRequest.class), anyLong()))
                .thenReturn(new CompletableFuture<>());
    }

    @Test
    void executionOfJobOfNonExistentClassResultsInException() {
        assertThat(
                executeLocally("no-such-class"),
                willThrow(Exception.class, "Cannot load job class by name 'no-such-class'")
        );
    }

    @Test
    void executionOfNonJobClassResultsInException() {
        assertThat(
                executeLocally(Object.class.getName()),
                willThrow(Exception.class, "'java.lang.Object' does not implement ComputeJob interface")
        );
    }

    @Test
    void executionOfNotExistingDeployedUnit() {
        List<DeploymentUnit> units = List.of(new DeploymentUnit("unit", "1.0.0"));
        doReturn(CompletableFuture.failedFuture(new DeploymentUnitNotFoundException("unit", Version.parseVersion("1.0.0"))))
                .when(jobContextManager).acquireClassLoader(units);

        assertThat(
                executeLocally(units, "com.example.Maim", null),
                willThrow(ClassNotFoundException.class)
        );
    }

    @Test
    void executionOfNotAvailableDeployedUnit() {
        List<DeploymentUnit> units = List.of(new DeploymentUnit("unit", "1.0.0"));
        DeploymentUnitUnavailableException toBeThrown = new DeploymentUnitUnavailableException(
                "unit",
                Version.parseVersion("1.0.0"),
                DeploymentStatus.OBSOLETE,
                DeploymentStatus.REMOVING
        );
        doReturn(CompletableFuture.failedFuture(toBeThrown))
                .when(jobContextManager).acquireClassLoader(units);

        assertThat(
                executeLocally(units, "com.example.Maim", null),
                willThrow(ClassNotFoundException.class)
        );
    }

    private <T extends NetworkMessage> T invokeAndCaptureRequest(Class<T> clazz) {
        ArgumentCaptor<T> requestCaptor = ArgumentCaptor.forClass(clazz);
        verify(messagingService).invoke(eq(remoteNode.name()), requestCaptor.capture(), anyLong());
        return requestCaptor.getValue();
    }

    private <T extends NetworkMessage> T sendRequestAndCaptureResponse(NetworkMessage request, ClusterNode sender, long correlationId) {
        AtomicBoolean responseSent = new AtomicBoolean(false);

        when(messagingService.respond(eq(sender.name()), any(), eq(correlationId)))
                .thenAnswer(invocation -> {
                    responseSent.set(true);
                    return nullCompletedFuture();
                });

        computeMessageHandlerRef.get().onReceived(request, testNode, correlationId);

        await().until(responseSent::get, is(true));

        ArgumentCaptor<T> responseCaptor = ArgumentCaptor.captor();
        verify(messagingService).respond(eq(sender.name()), responseCaptor.capture(), eq(correlationId));
        return responseCaptor.getValue();
    }

    private CompletableFuture<String> executeLocally(String jobClassName, String args) {
        return executeLocally(List.of(), jobClassName, args);
    }

    private CompletableFuture<String> executeLocally(String jobClassName) {
        return executeLocally(jobClassName, null);
    }

    private CompletableFuture<String> executeLocally(List<DeploymentUnit> units, String jobClassName, String args) {
        return computeComponent.<String, String>executeLocally(units, jobClassName, args).resultAsync();
    }

    private CompletableFuture<String> executeRemotely(
            String jobClassName,
            String args
    ) {
        return computeComponent.<String, String>executeRemotely(remoteNode, List.of(), jobClassName, args).resultAsync();
    }

    private CompletableFuture<String> executeRemotely(
            String jobClassName
    ) {
        return executeRemotely(jobClassName, null);
    }

    private static class SimpleJob implements ComputeJob<String, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String args) {
            return completedFuture("jobResponse");
        }
    }

    private static class FailingJob implements ComputeJob<String, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String args) {
            throw new JobException("Oops", new Exception());
        }
    }

    private static class JobException extends RuntimeException {
        public JobException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class GetThreadNameJob implements ComputeJob<String, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, String args) {
            return completedFuture(Thread.currentThread().getName());
        }
    }

    private static class LongJob implements ComputeJob<Void, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<String> executeAsync(JobExecutionContext context, Void args) {
            try {
                Thread.sleep(1_000_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return null;
        }
    }
}
