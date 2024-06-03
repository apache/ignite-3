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

package org.apache.ignite.internal.network.wrapper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.network.BaseNetworkTest;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JumpToExecutorByConsistentIdAfterSendTest extends BaseNetworkTest {
    private static final String SENDER_CONSISTENT_ID = "sender";
    private static final String RECIPIENT_CONSISTENT_ID = "recipient";

    @Mock
    private MessagingService messagingService;

    @Mock
    private NetworkMessage message;

    private final ClusterNode sender = new ClusterNodeImpl("senderId", SENDER_CONSISTENT_ID, new NetworkAddress("sender-host", 3000));
    private final ClusterNode recipient = new ClusterNodeImpl(
            "recipientId",
            RECIPIENT_CONSISTENT_ID,
            new NetworkAddress("recipient-host", 3000)
    );

    private final ExecutorService specificPool = Executors.newSingleThreadExecutor(SpecificThread::new);

    private MessagingService wrapper;

    @BeforeEach
    void createWrapper() {
        wrapper = new JumpToExecutorByConsistentIdAfterSend(messagingService, SENDER_CONSISTENT_ID, request -> specificPool);
    }

    @AfterEach
    void cleanup() {
        IgniteUtils.shutdownAndAwaitTermination(specificPool, 10, SECONDS);
    }

    @Test
    void switchesResponseHandlingToPoolAfterSendToAnotherMember() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.send(recipient, ChannelType.DEFAULT, message)).thenReturn(sendFuture),
                () -> wrapper.send(recipient, message)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterSendToAnotherMemberWithChannel() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.send(recipient, ChannelType.DEFAULT, message)).thenReturn(sendFuture),
                () -> wrapper.send(recipient, ChannelType.DEFAULT, message)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterSendToAnotherMemberByConstantId() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.send(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message)).thenReturn(sendFuture),
                () -> wrapper.send(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterRespondToAnotherMember() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.respond(recipient, ChannelType.DEFAULT, message, 0)).thenReturn(sendFuture),
                () -> wrapper.respond(recipient, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterRespondToAnotherMemberWithChannel() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.respond(recipient, ChannelType.DEFAULT, message, 0)).thenReturn(sendFuture),
                () -> wrapper.respond(recipient, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterRespondToAnotherMemberByConsistentId() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.respond(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0))
                        .thenReturn(sendFuture),
                () -> wrapper.respond(RECIPIENT_CONSISTENT_ID, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterRespondToAnotherMemberByConsistentIdWithChannel() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.respond(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0))
                        .thenReturn(sendFuture),
                () -> wrapper.respond(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterInvokeToAnotherMember() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.invoke(recipient, ChannelType.DEFAULT, message, 0)).thenReturn(sendFuture),
                () -> wrapper.invoke(recipient, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterInvokeToAnotherMemberWithChannel() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.invoke(recipient, ChannelType.DEFAULT, message, 0)).thenReturn(sendFuture),
                () -> wrapper.invoke(recipient, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterInvokeToAnotherMemberByConsistentId() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.invoke(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0))
                        .thenReturn(sendFuture),
                () -> wrapper.invoke(RECIPIENT_CONSISTENT_ID, message, 0)
        );
    }

    @Test
    void switchesResponseHandlingToPoolAfterInvokeToAnotherMemberByConsistentIdWithChannel() {
        testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
                sendFuture -> when(messagingService.invoke(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0))
                        .thenReturn(sendFuture),
                () -> wrapper.invoke(RECIPIENT_CONSISTENT_ID, ChannelType.DEFAULT, message, 0)
        );
    }

    private static <T> void testSwitchesResponseHandlingToPoolAfterSendToAnotherMember(
            Consumer<CompletableFuture<T>> configureMocks,
            Supplier<CompletableFuture<T>> execute
    ) {
        Thread executionThread = doSendAndGetExecutionThread(configureMocks, execute);

        assertThat(executionThread, is(instanceOf(SpecificThread.class)));
    }

    private static <T> Thread doSendAndGetExecutionThread(
            Consumer<CompletableFuture<T>> configureMocks,
            Supplier<CompletableFuture<T>> execute
    ) {
        CompletableFuture<T> sendFuture = new CompletableFuture<>();

        configureMocks.accept(sendFuture);

        AtomicReference<Thread> threadRef = new AtomicReference<>();

        CompletableFuture<T> finalFuture = execute.get().whenComplete((res, ex) -> {
            threadRef.set(Thread.currentThread());
        });

        sendFuture.complete(null);

        assertThat(finalFuture, willCompleteSuccessfully());

        return threadRef.get();
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterSendToAnotherMember() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.send(any(ClusterNode.class), any(), any())).thenReturn(sendFuture),
                () -> wrapper.send(sender, message)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterSendToAnotherMemberWithChannel() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.send(any(ClusterNode.class), any(), any())).thenReturn(sendFuture),
                () -> wrapper.send(sender, ChannelType.DEFAULT, message)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterSendToAnotherMemberByConstantId() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.send(anyString(), any(), any())).thenReturn(sendFuture),
                () -> wrapper.send(SENDER_CONSISTENT_ID, ChannelType.DEFAULT, message)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterRespondToAnotherMember() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.respond(any(ClusterNode.class), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.respond(sender, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterRespondToAnotherMemberWithChannel() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.respond(any(ClusterNode.class), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.respond(sender, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterRespondToAnotherMemberByConsistentId() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.respond(anyString(), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.respond(SENDER_CONSISTENT_ID, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterRespondToAnotherMemberByConsistentIdWithChannel() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.respond(anyString(), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.respond(SENDER_CONSISTENT_ID, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterInvokeToAnotherMember() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.invoke(any(ClusterNode.class), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.invoke(sender, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterInvokeToAnotherMemberWithChannel() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.invoke(any(ClusterNode.class), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.invoke(sender, ChannelType.DEFAULT, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterInvokeToAnotherMemberByConsistentId() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.invoke(anyString(), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.invoke(SENDER_CONSISTENT_ID, message, 0)
        );
    }

    @Test
    void doesNotSwitchResponseHandlingToPoolAfterInvokeToAnotherMemberByConsistentIdWithChannel() {
        testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
                sendFuture -> when(messagingService.invoke(anyString(), any(), any(), anyLong())).thenReturn(sendFuture),
                () -> wrapper.invoke(SENDER_CONSISTENT_ID, ChannelType.DEFAULT, message, 0)
        );
    }

    private static <T> void testDoesNotSwitchResponseHandlingToPoolAfterSendToItself(
            Consumer<CompletableFuture<T>> configureMocks,
            Supplier<CompletableFuture<T>> execute
    ) {
        Thread executionThread = doSendAndGetExecutionThread(configureMocks, execute);

        assertThat(executionThread, is(Thread.currentThread()));
    }

    @Test
    void delegatesAddMessageHandler(@Mock NetworkMessageHandler messageHandler) {
        wrapper.addMessageHandler(Void.class, messageHandler);

        verify(messagingService).addMessageHandler(Void.class, messageHandler);
    }

    @Test
    void delegatesAddMessageHandlerWithChooser(@Mock NetworkMessageHandler messageHandler) {
        ExecutorChooser<NetworkMessage> executorChooser = message -> mock(Executor.class);

        wrapper.addMessageHandler(Void.class, executorChooser, messageHandler);

        verify(messagingService).addMessageHandler(Void.class, executorChooser, messageHandler);
    }

    @SuppressWarnings("ClassExplicitlyExtendsThread")
    private static class SpecificThread extends Thread {
        private SpecificThread(Runnable target) {
            super(target);
        }
    }
}
