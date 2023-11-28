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

import static org.apache.ignite.internal.compute.ExecutionOptions.DEFAULT;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.queue.ComputeExecutor;
import org.apache.ignite.internal.compute.queue.ComputeExecutorImpl;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
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

    private final ClusterNode remoteNode = new ClusterNodeImpl("remote", "remote", new NetworkAddress("remote-host", 1));

    private final AtomicReference<NetworkMessageHandler> computeMessageHandlerRef = new AtomicReference<>();

    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        lenient().when(ignite.name()).thenReturn(INSTANCE_NAME);

        JobClassLoader classLoader = new JobClassLoader(List.of(), new URL[0], getClass().getClassLoader());
        JobContext jobContext = new JobContext(classLoader, ignored -> {});
        lenient().when(jobContextManager.acquireClassLoader(anyList()))
                .thenReturn(CompletableFuture.completedFuture(jobContext));

        doAnswer(invocation -> {
            computeMessageHandlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(ComputeMessageTypes.class), any());

        assertThat(computeConfiguration.change(computeChange ->
                        computeChange.changeThreadPoolStopTimeoutMillis(10_000L).changeThreadPoolSize(8)),
                willCompleteSuccessfully()
        );


        computeExecutor = new ComputeExecutorImpl(ignite, computeConfiguration);
        computeComponent = new ComputeComponentImpl(messagingService, jobContextManager, computeExecutor);

        computeComponent.start();
    }

    @AfterEach
    void cleanup() throws Exception {
        computeComponent.stop();
    }

    @Test
    void executesLocally() throws Exception {
        String result = computeComponent.<String>executeLocally(List.of(), SimpleJob.class.getName(), "a", 42).get();

        assertThat(result, is("jobResponse"));

        assertThatExecuteRequestWasNotSent();
    }

    private void assertThatExecuteRequestWasNotSent() {
        verify(messagingService, never()).invoke(any(ClusterNode.class), any(), anyLong());
    }

    @Test
    void executesLocallyWithException() {
        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> computeComponent.executeLocally(List.of(), FailingJob.class.getName()).get()
        );

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesRemotelyUsingNetworkCommunication() throws Exception {
        respondWithExecuteResponseWhenExecuteRequestIsSent();

        String result = computeComponent.<String>executeRemotely(remoteNode, List.of(), SimpleJob.class.getName(), "a", 42).get();

        assertThat(result, is("remoteResponse"));

        assertThatExecuteRequestWasSent();
    }

    private void respondWithExecuteResponseWhenExecuteRequestIsSent() {
        ExecuteResponse executeResponse = new ComputeMessagesFactory().executeResponse()
                .result("remoteResponse")
                .build();
        when(messagingService.invoke(any(ClusterNode.class), any(ExecuteRequest.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(executeResponse));
    }

    private void assertThatExecuteRequestWasSent() {
        verify(messagingService).invoke(eq(remoteNode), executeRequestCaptor.capture(), anyLong());

        ExecuteRequest capturedRequest = executeRequestCaptor.getValue();

        assertThat(capturedRequest.jobClassName(), is(SimpleJob.class.getName()));
        assertThat(capturedRequest.args(), is(equalTo(new Object[]{"a", 42})));
    }

    @Test
    void executesRemotelyWithException() {
        ExecuteResponse executeResponse = new ComputeMessagesFactory().executeResponse()
                .throwable(new JobException("Oops", new Exception()))
                .build();
        when(messagingService.invoke(any(ClusterNode.class), any(ExecuteRequest.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(executeResponse));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> computeComponent.executeRemotely(remoteNode, List.of(), FailingJob.class.getName()).get()
        );

        assertThat(ex.getCause(), is(instanceOf(JobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesJobAndRespondsWhenGetsExecuteRequest() throws Exception {
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

        assertThatExecuteResponseIsSentTo(sender);
    }

    private void markResponseSentOnResponseSend() {
        when(messagingService.respond(anyString(), any(), anyLong()))
                .thenAnswer(invocation -> {
                    responseSent.set(true);
                    return null;
                });
    }

    private void assertThatExecuteResponseIsSentTo(String sender) throws InterruptedException {
        assertTrue(IgniteTestUtils.waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), executeResponseCaptor.capture(), eq(123L));
        ExecuteResponse response = executeResponseCaptor.getValue();

        assertThat(response.result(), is("jobResponse"));
        assertThat(response.throwable(), is(nullValue()));
    }

    @Test
    void stoppedComponentReturnsExceptionOnLocalExecutionAttempt() throws Exception {
        computeComponent.stop();

        Object result = computeComponent.executeLocally(List.of(), SimpleJob.class.getName())
                .handle((s, ex) -> ex != null ? ex : s)
                .get();

        assertThat(result, is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void localExecutionReleasesStopLock() throws Exception {
        computeComponent.executeLocally(List.of(), SimpleJob.class.getName()).get();

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> computeComponent.stop());
    }

    @Test
    void stoppedComponentReturnsExceptionOnRemoteExecutionAttempt() throws Exception {
        computeComponent.stop();

        Object result = computeComponent.executeRemotely(remoteNode, List.of(), SimpleJob.class.getName())
                .handle((s, ex) -> ex != null ? ex : s)
                .get();

        assertThat(result, is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void remoteExecutionReleasesStopLock() throws Exception {
        respondWithExecuteResponseWhenExecuteRequestIsSent();

        computeComponent.executeRemotely(remoteNode, List.of(), SimpleJob.class.getName()).get();

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

        assertThatNodeStoppingExceptionIsSentTo(sender);
    }

    private void assertThatNodeStoppingExceptionIsSentTo(String sender) throws InterruptedException {
        assertTrue(IgniteTestUtils.waitForCondition(responseSent::get, 1000), "No response sent");

        verify(messagingService).respond(eq(sender), executeResponseCaptor.capture(), eq(123L));
        ExecuteResponse response = executeResponseCaptor.getValue();

        assertThat(response.result(), is(nullValue()));
        assertThat(response.throwable(), is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void executorThreadsAreNamedAccordingly() throws Exception {
        String threadName = computeComponent.<String>executeLocally(List.of(), GetThreadNameJob.class.getName()).get();

        assertThat(threadName, startsWith(NamedThreadFactory.threadPrefix(INSTANCE_NAME, "compute")));
    }

    private void restrictPoolSizeTo1() {
        assertThat(computeConfiguration.change(computeChange -> computeChange.changeThreadPoolSize(1)), willCompleteSuccessfully());
    }

    @Test
    void stopCausesCancellationExceptionOnLocalExecution() throws Exception {
        restrictPoolSizeTo1();

        assertThat(computeConfiguration.change(computeChange -> computeChange.changeThreadPoolStopTimeoutMillis(100)),
                willCompleteSuccessfully());

        computeComponent = new ComputeComponentImpl(messagingService, jobContextManager, computeExecutor);
        computeComponent.start();

        // take the only executor thread
        computeComponent.executeLocally(List.of(), LongJob.class.getName());

        // the corresponding task goes to work queue
        CompletableFuture<Object> resultFuture = computeComponent.executeLocally(List.of(), SimpleJob.class.getName())
                .handle((res, ex) -> ex != null ? ex : res);

        computeComponent.stop();

        // now work queue is dropped to the floor, so the future should be resolved with a cancellation

        Exception result = (Exception) resultFuture.get(3, TimeUnit.SECONDS);

        assertThat(result.getCause(), is(instanceOf(CancellationException.class)));
    }

    @Test
    void stopCausesCancellationExceptionOnRemoteExecution() throws Exception {
        respondWithIncompleteFutureWhenExecuteRequestIsSent();

        CompletableFuture<Object> resultFuture = computeComponent.executeRemotely(remoteNode, List.of(), SimpleJob.class.getName())
                .handle((res, ex) -> ex != null ? ex : res);

        computeComponent.stop();

        Object result = resultFuture.get(3, TimeUnit.SECONDS);

        assertThat(result, is(instanceOf(CancellationException.class)));
    }

    private void respondWithIncompleteFutureWhenExecuteRequestIsSent() {
        when(messagingService.invoke(any(ClusterNode.class), any(ExecuteRequest.class), anyLong()))
                .thenReturn(new CompletableFuture<>());
    }

    @Test
    void executionOfJobOfNonExistentClassResultsInException() throws Exception {
        Object result = computeComponent.executeLocally(List.of(), "no-such-class")
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result, is(instanceOf(Exception.class)));
        assertThat(((Exception) result).getMessage(), containsString("Cannot load job class by name 'no-such-class'"));
    }

    @Test
    void executionOfNonJobClassResultsInException() throws Exception {
        Object result = computeComponent.executeLocally(List.of(), Object.class.getName())
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result, is(instanceOf(Exception.class)));
        assertThat(((Exception) result).getMessage(), containsString("'java.lang.Object' does not implement ComputeJob interface"));
    }

    @Test
    void executionOfNotExistingDeployedUnit() {
        List<DeploymentUnit> units = List.of(new DeploymentUnit("unit", "1.0.0"));
        doReturn(CompletableFuture.failedFuture(new DeploymentUnitNotFoundException("unit", Version.parseVersion("1.0.0"))))
                .when(jobContextManager).acquireClassLoader(units);

        assertThat(
                computeComponent.executeLocally(units, "com.example.Maim"),
                CompletableFutureExceptionMatcher.willThrow(ClassNotFoundException.class)
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
                computeComponent.executeLocally(units, "com.example.Maim"),
                CompletableFutureExceptionMatcher.willThrow(ClassNotFoundException.class)
        );
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
