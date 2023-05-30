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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Timeout(10)
class ComputeComponentImplTest {
    private static final String INSTANCE_NAME = "Ignite-0";

    private final Path units = Path.of(JobClassLoaderFactory.class.getClassLoader().getResource("units").getPath());

    @Mock
    private Ignite ignite;

    @Mock
    private MessagingService messagingService;

    @Mock
    private ComputeConfiguration computeConfiguration;

    @Mock
    private ConfigurationValue<Integer> threadPoolSizeValue;

    @Mock
    private ConfigurationValue<Long> threadPoolStopTimeoutMillisValue;

    private ComputeComponentImpl computeComponent;

    @Captor
    private ArgumentCaptor<ExecuteRequest> executeRequestCaptor;
    @Captor
    private ArgumentCaptor<ExecuteResponse> executeResponseCaptor;

    private final JobClassLoaderFactory jobClassLoaderFactory = new JobClassLoaderFactory(units, name -> {
        throw new UnsupportedOperationException("LATEST version is not supported");
    });

    private final ClusterNode remoteNode = new ClusterNode("remote", "remote", new NetworkAddress("remote-host", 1));

    private final AtomicReference<NetworkMessageHandler> computeMessageHandlerRef = new AtomicReference<>();

    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    private final List<DeploymentUnit> testDeploymentUnits = List.of(
            new DeploymentUnit("ignite-job", Version.parseVersion("1.0.0"))
    );

    private final List<DeploymentUnitMsg> testDeploymentUnitMsgs = testDeploymentUnits.stream()
            .map(it -> DeploymentUnitMsg.fromDeploymentUnit(messagesFactory, it))
            .collect(Collectors.toList());

    @BeforeEach
    void setUp() {
        lenient().when(computeConfiguration.threadPoolSize()).thenReturn(threadPoolSizeValue);
        lenient().when(threadPoolSizeValue.value()).thenReturn(8);
        lenient().when(computeConfiguration.threadPoolStopTimeoutMillis()).thenReturn(threadPoolStopTimeoutMillisValue);
        lenient().when(threadPoolStopTimeoutMillisValue.value()).thenReturn(10_000L);

        lenient().when(ignite.name()).thenReturn(INSTANCE_NAME);

        doAnswer(invocation -> {
            computeMessageHandlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(ComputeMessageTypes.class), any());

        computeComponent = new ComputeComponentImpl(ignite, messagingService, computeConfiguration, jobClassLoaderFactory);
        computeComponent.start();
    }

    @AfterEach
    void cleanup() throws Exception {
        computeComponent.stop();
    }

    @Test
    void executesLocally() throws Exception {
        String result = computeComponent.<String>executeLocally(
                testDeploymentUnits,
                "org.example.SimpleJob",
                "a", 42
        ).get();

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
                () -> computeComponent.executeLocally(testDeploymentUnits, "org.example.FailingJob").get()
        );

        assertThat(ex.getCause().getClass().getName(), is("org.example.JobException"));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    @Test
    void executesRemotelyUsingNetworkCommunication() throws Exception {
        respondWithExecuteResponseWhenExecuteRequestIsSent();

        String result = computeComponent.<String>executeRemotely(
                remoteNode,
                testDeploymentUnits,
                "org.example.SimpleJob",
                "a", 42
        ).get();

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

    @Test
    void executesRemotelyWithException() {
        ExecuteResponse executeResponse = new ComputeMessagesFactory().executeResponse()
                .throwable(new RemoteJobException("Oops", new Exception()))
                .build();
        when(messagingService.invoke(any(ClusterNode.class), any(ExecuteRequest.class), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(executeResponse));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> computeComponent.executeRemotely(remoteNode, testDeploymentUnits, "org.example.FailingJob").get()
        );

        assertThat(ex.getCause(), is(instanceOf(RemoteJobException.class)));
        assertThat(ex.getCause().getMessage(), is("Oops"));
        assertThat(ex.getCause().getCause(), is(notNullValue()));
    }

    private void assertThatExecuteRequestWasSent() {
        verify(messagingService).invoke(eq(remoteNode), executeRequestCaptor.capture(), anyLong());

        ExecuteRequest capturedRequest = executeRequestCaptor.getValue();

        assertThat(capturedRequest.deploymentUnits(), is(equalTo(testDeploymentUnitMsgs)));
        assertThat(capturedRequest.jobClassName(), is("org.example.SimpleJob"));
        assertThat(capturedRequest.args(), is(equalTo(new Object[]{"a", 42})));
    }

    @Test
    void executesJobAndRespondsWhenGetsExecuteRequest() throws Exception {
        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        List<DeploymentUnitMsg> deploymentUnitMsgs = testDeploymentUnits.stream()
                .map(it -> DeploymentUnitMsg.fromDeploymentUnit(messagesFactory, it))
                .collect(Collectors.toList());
        ExecuteRequest request = messagesFactory.executeRequest()
                .deploymentUnits(deploymentUnitMsgs)
                .jobClassName("org.example.SimpleJob")
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

        Object result = computeComponent.executeLocally(testDeploymentUnits, "org.example.SimpleJob")
                .handle((s, ex) -> ex != null ? ex : s)
                .get();

        assertThat(result, is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void localExecutionReleasesStopLock() throws Exception {
        computeComponent.executeLocally(testDeploymentUnits, "org.example.SimpleJob").get();

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> computeComponent.stop());
    }

    @Test
    void stoppedComponentReturnsExceptionOnRemoteExecutionAttempt() throws Exception {
        computeComponent.stop();

        Object result = computeComponent.executeRemotely(remoteNode, testDeploymentUnits, "org.example.SimpleJob")
                .handle((s, ex) -> ex != null ? ex : s)
                .get();

        assertThat(result, is(instanceOf(NodeStoppingException.class)));
    }

    @Test
    void remoteExecutionReleasesStopLock() throws Exception {
        respondWithExecuteResponseWhenExecuteRequestIsSent();

        computeComponent.executeRemotely(remoteNode, testDeploymentUnits, "org.example.SimpleJob").get();

        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> computeComponent.stop());
    }

    public static class RemoteJobException extends RuntimeException {
        public RemoteJobException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Test
    void stoppedComponentReturnsExceptionOnExecuteRequestAttempt() throws Exception {
        computeComponent.stop();

        markResponseSentOnResponseSend();
        assertThat(computeMessageHandlerRef.get(), is(notNullValue()));

        String sender = "test";

        ExecuteRequest request = new ComputeMessagesFactory().executeRequest()
                .deploymentUnits(testDeploymentUnitMsgs)
                .jobClassName("org.example.SimpleJob")
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
        String threadName = computeComponent.<String>executeLocally(testDeploymentUnits, "org.example.GetThreadNameJob").get();

        assertThat(threadName, startsWith(NamedThreadFactory.threadPrefix(INSTANCE_NAME, "compute")));
    }

    @Test
    void executionRejectionCausesExceptionToBeReturnedViaFuture() throws Exception {
        restrictPoolSizeTo1();

        computeComponent = new ComputeComponentImpl(ignite, messagingService, computeConfiguration, jobClassLoaderFactory) {
            @Override
            BlockingQueue<Runnable> newExecutorServiceTaskQueue() {
                return new SynchronousQueue<>();
            }

            @Override
            long stopTimeoutMillis() {
                return 100;
            }
        };
        computeComponent.start();

        // take the only executor thread
        computeComponent.executeLocally(testDeploymentUnits, "org.example.LongJob");

        Exception result = (Exception) computeComponent.executeLocally(testDeploymentUnits, "org.example.SimpleJob")
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result.getCause(), is(instanceOf(RejectedExecutionException.class)));
    }

    private void restrictPoolSizeTo1() {
        when(threadPoolSizeValue.value()).thenReturn(1);
    }

    @Test
    void stopCausesCancellationExceptionOnLocalExecution() throws Exception {
        restrictPoolSizeTo1();

        computeComponent = new ComputeComponentImpl(ignite, messagingService, computeConfiguration, jobClassLoaderFactory) {
            @Override
            long stopTimeoutMillis() {
                return 100;
            }
        };
        computeComponent.start();

        // take the only executor thread
        computeComponent.executeLocally(testDeploymentUnits, "org.example.LongJob");

        // the corresponding task goes to work queue
        CompletableFuture<Object> resultFuture = computeComponent.executeLocally(testDeploymentUnits, "org.example.SimpleJob")
                .handle((res, ex) -> ex != null ? ex : res);

        computeComponent.stop();

        // now work queue is dropped to the floor, so the future should be resolved with a cancellation

        Exception result = (Exception) resultFuture.get(3, TimeUnit.SECONDS);

        assertThat(result.getCause(), is(instanceOf(CancellationException.class)));
    }

    @Test
    void stopCausesCancellationExceptionOnRemoteExecution() throws Exception {
        respondWithIncompleteFutureWhenExecuteRequestIsSent();

        CompletableFuture<Object> resultFuture = computeComponent.executeRemotely(remoteNode, testDeploymentUnits, "org.example.SimpleJob")
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
        Object result = computeComponent.executeLocally(testDeploymentUnits, "no-such-class")
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result, is(instanceOf(Exception.class)));
        assertThat(((Exception) result).getMessage(), containsString("Cannot load job class by name 'no-such-class'"));
    }

    @Test
    void executionOfNonJobClassResultsInException() throws Exception {
        Object result = computeComponent.executeLocally(testDeploymentUnits, Object.class.getName())
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result, is(instanceOf(Exception.class)));
        assertThat(((Exception) result).getMessage(), containsString("'java.lang.Object' does not implement ComputeJob interface"));
    }
}
