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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.internal.compute.ExecutionOptions.DEFAULT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.JobStatusMatcher.jobStatusWithState;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.version.Version;
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
import org.apache.ignite.internal.compute.message.JobStatusRequest;
import org.apache.ignite.internal.compute.message.JobStatusResponse;
import org.apache.ignite.internal.compute.queue.CancellingException;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
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

    @InjectConfiguration
    private ComputeConfiguration computeConfiguration;

    @Mock
    private JobContextManager jobContextManager;

    @InjectMocks
    private ComputeComponentImpl computeComponent;

    private ComputeExecutor computeExecutor;

    @Captor
    private ArgumentCaptor<ExecuteRequest> executeRequestCaptor;

    @Captor
    private ArgumentCaptor<ExecuteResponse> executeResponseCaptor;

    @Captor
    private ArgumentCaptor<JobResultRequest> jobResultRequestCaptor;

    @Captor
    private ArgumentCaptor<JobResultResponse> jobResultResponseCaptor;

    @Captor
    private ArgumentCaptor<JobStatusRequest> jobStatusRequestCaptor;

    @Captor
    private ArgumentCaptor<JobStatusResponse> jobStatusResponseCaptor;

    @Captor
    private ArgumentCaptor<JobCancelRequest> jobCancelRequestCaptor;

    @Captor
    private ArgumentCaptor<JobCancelResponse> jobCancelResponseCaptor;

    @Captor
    private ArgumentCaptor<JobChangePriorityRequest> jobChangePriorityRequestCaptor;

    @Captor
    private ArgumentCaptor<JobChangePriorityResponse> jobChangePriorityResponseCaptor;

    private final ClusterNode remoteNode = new ClusterNodeImpl("remote", "remote", new NetworkAddress("remote-host", 1));

    private final AtomicReference<NetworkMessageHandler> computeMessageHandlerRef = new AtomicReference<>();

    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        lenient().when(ignite.name()).thenReturn(INSTANCE_NAME);

        JobClassLoader classLoader = new JobClassLoader(List.of(), new URL[0], getClass().getClassLoader());
        JobContext jobContext = new JobContext(classLoader, ignored -> {});
        lenient().when(jobContextManager.acquireClassLoader(anyList()))
                .thenReturn(completedFuture(jobContext));

        doAnswer(invocation -> {
            computeMessageHandlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(ComputeMessageTypes.class), any());

        assertThat(computeConfiguration.change(computeChange ->
                        computeChange.changeThreadPoolStopTimeoutMillis(10_000L).changeThreadPoolSize(8)),
                willCompleteSuccessfully()
        );

        computeExecutor = new ComputeExecutorImpl(ignite, new InMemoryComputeStateMachine(computeConfiguration), computeConfiguration);

        computeComponent = new ComputeComponentImpl(messagingService, jobContextManager, computeExecutor);

        computeComponent.start();
    }

    @AfterEach
    void cleanup() throws Exception {
        computeComponent.stop();
    }

    @Test
    void executesLocally() {
        JobExecution<String> execution = computeComponent.executeLocally(List.of(), SimpleJob.class.getName(), "a", 42);

        assertThat(execution.resultAsync(), willBe("jobResponse"));
        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willThrow(CancellingException.class));
        assertThat(execution.changePriorityAsync(1), willThrow(ComputeException.class));

        assertThatNoRequestsWereSent();
    }

    @Test
    void getsStatusAndCancelsLocally() {
        JobExecution<String> execution = computeComponent.executeLocally(List.of(), LongJob.class.getName());

        await().until(execution::statusAsync, willBe(jobStatusWithState(EXECUTING)));

        assertThat(execution.cancelAsync(), willCompleteSuccessfully());

        await().until(execution::statusAsync, willBe(jobStatusWithState(CANCELED)));

        assertThatNoRequestsWereSent();
    }

    private void assertThatNoRequestsWereSent() {
        verify(messagingService, never()).invoke(any(ClusterNode.class), any(), anyLong());
    }

    @Test
    void executesLocallyWithException() {
        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> executeLocally(List.of(), FailingJob.class.getName()).get()
        );

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesRemotelyUsingNetworkCommunication() {
        UUID jobId = UUID.randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);
        respondWithJobStatusResponseWhenJobStatusRequestIsSent(jobId, COMPLETED);
        respondWithCancellingExceptionWhenJobCancelRequestIsSent(jobId);

        JobExecution<String> execution = computeComponent.executeRemotely(remoteNode, List.of(), SimpleJob.class.getName(), "a", 42);
        assertThat(execution.resultAsync(), willBe("remoteResponse"));

        // Verify that second invocation of resultAsync will not result in the network communication (i.e. the result is cached locally)
        assertThat(execution.resultAsync(), willBe("remoteResponse"));

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(COMPLETED)));
        assertThat(execution.cancelAsync(), willThrow(CancellingException.class));

        assertThatExecuteRequestWasSent(SimpleJob.class.getName(), "a", 42);
        assertThatJobResultRequestWasSent(jobId);
    }

    @Test
    void getsStatusAndCancelsRemotelyUsingNetworkCommunication() {
        UUID jobId = UUID.randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);
        respondWithJobStatusResponseWhenJobStatusRequestIsSent(jobId, EXECUTING);
        respondWithJobCancelResponseWhenJobCancelRequestIsSent(jobId);

        JobExecution<String> execution = computeComponent.executeRemotely(remoteNode, List.of(), LongJob.class.getName());

        assertThat(execution.statusAsync(), willBe(jobStatusWithState(EXECUTING)));
        assertThat(execution.resultAsync(), willBe("remoteResponse"));
        assertThat(execution.cancelAsync(), willCompleteSuccessfully());

        assertThatExecuteRequestWasSent(LongJob.class.getName());
        assertThatJobResultRequestWasSent(jobId);
        assertThatJobStatusRequestWasSent(jobId);
        assertThatJobCancelRequestWasSent(jobId);
    }

    @Test
    void changePriorityRemotelyUsingNetworkCommunication() {
        UUID jobId = UUID.randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobChangePriorityResponseWhenJobChangePriorityRequestIsSent(jobId);

        JobExecution<String> execution = computeComponent.executeRemotely(remoteNode, List.of(), LongJob.class.getName());

        assertThat(execution.changePriorityAsync(1), willCompleteSuccessfully());

        assertThatJobChangePriorityRequestWasSent(jobId);
    }

    private void respondWithExecuteResponseWhenExecuteRequestIsSent(UUID jobId) {
        ExecuteResponse executeResponse = new ComputeMessagesFactory().executeResponse()
                .jobId(jobId)
                .build();
        when(messagingService.invoke(any(ClusterNode.class), any(ExecuteRequest.class), anyLong()))
                .thenReturn(completedFuture(executeResponse));
    }

    private void respondWithJobResultResponseWhenJobResultRequestIsSent(UUID jobId) {
        JobResultResponse jobResultResponse = new ComputeMessagesFactory().jobResultResponse()
                .result("remoteResponse")
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobResultRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobResultResponse));
    }

    private void respondWithJobStatusResponseWhenJobStatusRequestIsSent(UUID jobId, JobState jobState) {
        JobStatusResponse jobStatusResponse = new ComputeMessagesFactory().jobStatusResponse()
                .status(JobStatus.builder().id(jobId).state(jobState).createTime(Instant.now()).build())
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobStatusRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobStatusResponse));
    }

    private void respondWithJobCancelResponseWhenJobCancelRequestIsSent(UUID jobId) {
        JobCancelResponse jobCancelResponse = new ComputeMessagesFactory().jobCancelResponse()
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobCancelRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobCancelResponse));
    }

    private void respondWithCancellingExceptionWhenJobCancelRequestIsSent(UUID jobId) {
        JobCancelResponse jobCancelResponse = new ComputeMessagesFactory().jobCancelResponse()
                .throwable(new CancellingException(jobId))
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobCancelRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobCancelResponse));
    }

    private void respondWithJobChangePriorityResponseWhenJobChangePriorityRequestIsSent(UUID jobId) {
        JobChangePriorityResponse jobChangePriorityResponse = new ComputeMessagesFactory().jobChangePriorityResponse()
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobChangePriorityRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobChangePriorityResponse));
    }

    private static boolean jobResultRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobResultRequest) {
            JobResultRequest jobResultRequest = (JobResultRequest) argument;
            return jobResultRequest.jobId() == jobId;
        }
        return false;
    }

    private static boolean jobStatusRequestWithJobId(NetworkMessage argument, UUID jobId) {
        if (argument instanceof JobStatusRequest) {
            JobStatusRequest jobStatusRequest = (JobStatusRequest) argument;
            return jobStatusRequest.jobId() == jobId;
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

    private void assertThatExecuteRequestWasSent(String jobClassName, Object... args) {
        verify(messagingService).invoke(eq(remoteNode), executeRequestCaptor.capture(), anyLong());

        ExecuteRequest capturedRequest = executeRequestCaptor.getValue();

        assertThat(capturedRequest.jobClassName(), is(jobClassName));
        assertThat(capturedRequest.args(), is(equalTo(args)));
    }

    private void assertThatJobResultRequestWasSent(UUID jobId) {
        verify(messagingService).invoke(eq(remoteNode), jobResultRequestCaptor.capture(), anyLong());

        JobResultRequest capturedRequest = jobResultRequestCaptor.getValue();

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobStatusRequestWasSent(UUID jobId) {
        verify(messagingService).invoke(eq(remoteNode), jobStatusRequestCaptor.capture(), anyLong());

        JobStatusRequest capturedRequest = jobStatusRequestCaptor.getValue();

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobCancelRequestWasSent(UUID jobId) {
        verify(messagingService).invoke(eq(remoteNode), jobCancelRequestCaptor.capture(), anyLong());

        JobCancelRequest capturedRequest = jobCancelRequestCaptor.getValue();

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    private void assertThatJobChangePriorityRequestWasSent(UUID jobId) {
        verify(messagingService).invoke(eq(remoteNode), jobChangePriorityRequestCaptor.capture(), anyLong());

        JobChangePriorityRequest capturedRequest = jobChangePriorityRequestCaptor.getValue();

        assertThat(capturedRequest.jobId(), is(jobId));
    }

    @Test
    void executesRemotelyWithException() {
        UUID jobId = UUID.randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        JobResultResponse jobResultResponse = new ComputeMessagesFactory().jobResultResponse()
                .throwable(new JobException("Oops", new Exception()))
                .build();
        when(messagingService.invoke(any(ClusterNode.class), argThat(msg -> jobResultRequestWithJobId(msg, jobId)), anyLong()))
                .thenReturn(completedFuture(jobResultResponse));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> executeRemotely(remoteNode, List.of(), FailingJob.class.getName()).get()
        );

        assertThatExecuteRequestWasSent(FailingJob.class.getName());

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesJobAndRespondsWhenGetsExecuteRequest() throws Exception {
        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        ExecuteRequest executeRequest = new ComputeMessagesFactory().executeRequest()
                .executeOptions(DEFAULT)
                .deploymentUnits(List.of())
                .jobClassName(SimpleJob.class.getName())
                .args(new Object[]{"a", 42})
                .build();
        computeMessageHandlerRef.get().onReceived(executeRequest, sender, 123L);

        UUID jobId = assertThatExecuteResponseIsSentTo(sender);
        responseSent.set(false);
        markResponseSentOnResponseSend();

        JobResultRequest jobResultRequest = new ComputeMessagesFactory().jobResultRequest()
                .jobId(jobId)
                .build();
        computeMessageHandlerRef.get().onReceived(jobResultRequest, sender, 456L);

        assertThatJobResultResponseIsSentTo(sender);
    }

    private void markResponseSentOnResponseSend() {
        when(messagingService.respond(anyString(), any(), anyLong()))
                .thenAnswer(invocation -> {
                    responseSent.set(true);
                    return nullCompletedFuture();
                });
    }

    private UUID assertThatExecuteResponseIsSentTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), executeResponseCaptor.capture(), eq(123L));
        ExecuteResponse response = executeResponseCaptor.getValue();

        UUID jobId = response.jobId();
        assertThat(jobId, is(notNullValue()));
        assertThat(response.throwable(), is(nullValue()));
        return jobId;
    }

    private void assertThatJobResultResponseIsSentTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), jobResultResponseCaptor.capture(), eq(456L));
        JobResultResponse response = jobResultResponseCaptor.getValue();

        assertThat(response.result(), is("jobResponse"));
        assertThat(response.throwable(), is(nullValue()));
    }

    @Test
    void stoppedComponentReturnsExceptionOnLocalExecutionAttempt() throws Exception {
        computeComponent.stop();

        CompletableFuture<String> result = executeLocally(List.of(), SimpleJob.class.getName());

        assertThat(result, willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }

    @Test
    void localExecutionReleasesStopLock() throws Exception {
        executeLocally(List.of(), SimpleJob.class.getName()).get();

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> computeComponent.stop());
    }

    @Test
    void stoppedComponentReturnsExceptionOnRemoteExecutionAttempt() throws Exception {
        computeComponent.stop();

        CompletableFuture<String> result = executeRemotely(remoteNode, List.of(), SimpleJob.class.getName());

        assertThat(result, willThrowWithCauseOrSuppressed(NodeStoppingException.class));
    }

    @Test
    void remoteExecutionReleasesStopLock() throws Exception {
        UUID jobId = UUID.randomUUID();
        respondWithExecuteResponseWhenExecuteRequestIsSent(jobId);
        respondWithJobResultResponseWhenJobResultRequestIsSent(jobId);

        executeRemotely(remoteNode, List.of(), SimpleJob.class.getName()).get();

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> computeComponent.stop());
    }

    @Test
    void stoppedComponentReturnsExceptionOnExecuteRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        ExecuteRequest request = new ComputeMessagesFactory().executeRequest()
                .executeOptions(DEFAULT)
                .deploymentUnits(List.of())
                .jobClassName(SimpleJob.class.getName())
                .args(new Object[]{"a", 42})
                .build();
        computeMessageHandlerRef.get().onReceived(request, sender, 123L);

        assertThatExecuteRequestSendsNodeStoppingExceptionTo(sender);
    }

    private void assertThatExecuteRequestSendsNodeStoppingExceptionTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), executeResponseCaptor.capture(), eq(123L));
        ExecuteResponse response = executeResponseCaptor.getValue();

        assertThat(response.jobId(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobResultRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        JobResultRequest jobResultRequest = new ComputeMessagesFactory().jobResultRequest()
                .jobId(UUID.randomUUID())
                .build();
        computeMessageHandlerRef.get().onReceived(jobResultRequest, sender, 123L);

        assertThatJobResultRequestSendsNodeStoppingExceptionTo(sender);
    }

    private void assertThatJobResultRequestSendsNodeStoppingExceptionTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), jobResultResponseCaptor.capture(), eq(123L));
        JobResultResponse response = jobResultResponseCaptor.getValue();

        assertThat(response.result(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobStatusRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        JobStatusRequest jobStatusRequest = new ComputeMessagesFactory().jobStatusRequest()
                .jobId(UUID.randomUUID())
                .build();
        computeMessageHandlerRef.get().onReceived(jobStatusRequest, sender, 123L);

        assertThatJobStatusRequestSendsNodeStoppingExceptionTo(sender);
    }

    private void assertThatJobStatusRequestSendsNodeStoppingExceptionTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), jobStatusResponseCaptor.capture(), eq(123L));
        JobStatusResponse response = jobStatusResponseCaptor.getValue();

        assertThat(response.status(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobCancelRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        JobCancelRequest jobCancelRequest = new ComputeMessagesFactory().jobCancelRequest()
                .jobId(UUID.randomUUID())
                .build();
        computeMessageHandlerRef.get().onReceived(jobCancelRequest, sender, 456L);

        assertThatJobCancelRequestSendsNodeStoppingExceptionTo(sender);
    }

    private void assertThatJobCancelRequestSendsNodeStoppingExceptionTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), jobCancelResponseCaptor.capture(), eq(456L));
        JobCancelResponse response = jobCancelResponseCaptor.getValue();

        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void stoppedComponentReturnsExceptionOnJobChangePriorityRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        JobChangePriorityRequest jobChangePriorityRequest = new ComputeMessagesFactory().jobChangePriorityRequest()
                .jobId(UUID.randomUUID())
                .priority(1)
                .build();
        computeMessageHandlerRef.get().onReceived(jobChangePriorityRequest, sender, 456L);

        assertThatJobChangePriorityRequestSendsNodeStoppingExceptionTo(sender);
    }

    private void assertThatJobChangePriorityRequestSendsNodeStoppingExceptionTo(String sender) throws InterruptedException {
        assertTrue(waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), jobChangePriorityResponseCaptor.capture(), eq(456L));
        JobChangePriorityResponse response = jobChangePriorityResponseCaptor.getValue();

        assertThat(response.throwable(), is(instanceOf(IgniteInternalException.class)));
        assertThat(response.throwable().getCause(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void executorThreadsAreNamedAccordingly() {
        assertThat(
                executeLocally(List.of(), GetThreadNameJob.class.getName()),
                willBe(startsWith(NamedThreadFactory.threadPrefix(INSTANCE_NAME, "compute")))
        );
    }

    private void restrictPoolSizeTo1() {
        assertThat(computeConfiguration.change(computeChange -> computeChange.changeThreadPoolSize(1)), willCompleteSuccessfully());
    }

    @Test
    void stopCausesCancellationExceptionOnLocalExecution() throws Exception {
        restrictPoolSizeTo1();

        assertThat(computeConfiguration.change(computeChange -> computeChange.changeThreadPoolStopTimeoutMillis(100)),
                willCompleteSuccessfully());

        computeComponent = new ComputeComponentImpl(
                messagingService,
                jobContextManager,
                computeExecutor
        );
        computeComponent.start();

        // take the only executor thread
        executeLocally(List.of(), LongJob.class.getName());

        // the corresponding task goes to work queue
        CompletableFuture<String> resultFuture = executeLocally(List.of(), SimpleJob.class.getName());

        computeComponent.stop();

        // now work queue is dropped to the floor, so the future should be resolved with a cancellation

        assertThat(resultFuture, willThrow(CancellationException.class));
    }

    @Test
    void stopCausesCancellationExceptionOnRemoteExecution() throws Exception {
        respondWithExecuteResponseWhenExecuteRequestIsSent(UUID.randomUUID());
        respondWithIncompleteFutureWhenJobResultRequestIsSent();

        CompletableFuture<String> resultFuture = executeRemotely(remoteNode, List.of(), SimpleJob.class.getName());

        computeComponent.stop();

        assertThat(resultFuture, willThrow(CancellationException.class, 3, TimeUnit.SECONDS));
    }

    private void respondWithIncompleteFutureWhenJobResultRequestIsSent() {
        when(messagingService.invoke(any(ClusterNode.class), any(JobResultRequest.class), anyLong()))
                .thenReturn(new CompletableFuture<>());
    }

    @Test
    void executionOfJobOfNonExistentClassResultsInException() {
        assertThat(
                executeLocally(List.of(), "no-such-class"),
                willThrow(Exception.class, "Cannot load job class by name 'no-such-class'")
        );
    }

    @Test
    void executionOfNonJobClassResultsInException() {
        assertThat(
                executeLocally(List.of(), Object.class.getName()),
                willThrow(Exception.class, "'java.lang.Object' does not implement ComputeJob interface")
        );
    }

    @Test
    void executionOfNotExistingDeployedUnit() {
        List<DeploymentUnit> units = List.of(new DeploymentUnit("unit", "1.0.0"));
        doReturn(CompletableFuture.failedFuture(new DeploymentUnitNotFoundException("unit", Version.parseVersion("1.0.0"))))
                .when(jobContextManager).acquireClassLoader(units);

        assertThat(
                executeLocally(units, "com.example.Maim"),
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
                executeLocally(units, "com.example.Maim"),
                willThrow(ClassNotFoundException.class)
        );
    }

    private CompletableFuture<String> executeLocally(List<DeploymentUnit> units, String jobClassName, Object... args) {
        return computeComponent.<String>executeLocally(units, jobClassName, args).resultAsync();
    }

    private CompletableFuture<String> executeRemotely(
            ClusterNode remoteNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        return computeComponent.<String>executeRemotely(remoteNode, units, jobClassName, args).resultAsync();
    }

    private static class SimpleJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return "jobResponse";
        }
    }

    private static class FailingJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            throw new JobException("Oops", new Exception());
        }
    }

    private static class JobException extends RuntimeException {
        public JobException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class GetThreadNameJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return Thread.currentThread().getName();
        }
    }

    private static class LongJob implements ComputeJob<String> {
        /** {@inheritDoc} */
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            try {
                Thread.sleep(1_000_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return null;
        }
    }
}
