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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URL;
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
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Timeout(10)
class ComputeComponentImplTest {
    private static final String INSTANCE_NAME = "Ignite-0";

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
    @Mock
    private JobClassLoaderFactory jobClassLoaderFactory;

    @InjectMocks
    private ComputeComponentImpl computeComponent;

    @Captor
    private ArgumentCaptor<ExecuteRequest> executeRequestCaptor;
    @Captor
    private ArgumentCaptor<ExecuteResponse> executeResponseCaptor;

    private final ClusterNode remoteNode = new ClusterNode("remote", "remote", new NetworkAddress("remote-host", 1));

    private final AtomicReference<NetworkMessageHandler> computeMessageHandlerRef = new AtomicReference<>();

    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    @BeforeEach
    void setUp() {
        lenient().when(computeConfiguration.threadPoolSize()).thenReturn(threadPoolSizeValue);
        lenient().when(threadPoolSizeValue.value()).thenReturn(8);
        lenient().when(computeConfiguration.threadPoolStopTimeoutMillis()).thenReturn(threadPoolStopTimeoutMillisValue);
        lenient().when(threadPoolStopTimeoutMillisValue.value()).thenReturn(10_000L);

        lenient().when(ignite.name()).thenReturn(INSTANCE_NAME);

        lenient().when(jobClassLoaderFactory.createClassLoader(anyList()))
                .thenReturn(new JobClassLoader(new URL[0], getClass().getClassLoader()));

        doAnswer(invocation -> {
            computeMessageHandlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(ComputeMessageTypes.class), any());

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
        computeComponent.executeLocally(List.of(), LongJob.class.getName());

        Object result = computeComponent.executeLocally(List.of(), SimpleJob.class.getName())
                .handle((res, ex) -> ex != null ? ex : res)
                .get();

        assertThat(result, is(instanceOf(RejectedExecutionException.class)));
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
        computeComponent.executeLocally(List.of(), LongJob.class.getName());

        // the corresponding task goes to work queue
        CompletableFuture<Object> resultFuture = computeComponent.executeLocally(List.of(), SimpleJob.class.getName())
                .handle((res, ex) -> ex != null ? ex : res);

        computeComponent.stop();

        // now work queue is dropped to the floor, so the future should be resolved with a cancellation

        Object result = resultFuture.get(3, TimeUnit.SECONDS);

        assertThat(result, is(instanceOf(CancellationException.class)));
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
