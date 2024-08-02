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

package org.apache.ignite.internal.cluster.management.network;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CmgMessageHandlerTest extends BaseIgniteAbstractTest {
    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    private final ComponentContext componentContext = new ComponentContext();

    private ClusterService clusterService;

    private CmgMessageHandler handler;

    @Mock
    private CmgMessageCallback callback;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = ClusterServiceTestUtils.clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        assertThat(clusterService.startAsync(componentContext), willCompleteSuccessfully());

        handler = new CmgMessageHandler(
                new IgniteSpinBusyLock(),
                cmgMessagesFactory,
                clusterService,
                callback
        );

        clusterService.messagingService().addMessageHandler(CmgMessageGroup.class, handler);
    }

    @AfterEach
    void tearDown() {
        clusterService.beforeNodeStop();

        assertThat(clusterService.stopAsync(componentContext), willCompleteSuccessfully());
    }

    @Test
    void handlerBuffersMessages() {
        MessagingService messagingService = clusterService.messagingService();

        String nodeName = clusterService.nodeName();

        CmgInitMessage message1 = cmgMessagesFactory.cmgInitMessage()
                .clusterName("foo")
                .cmgNodes(Set.of("foo"))
                .metaStorageNodes(Set.of("foo"))
                .initialClusterConfiguration("")
                .build();

        CompletableFuture<Void> send1Future = messagingService.send(nodeName, ChannelType.DEFAULT, message1);

        ClusterState clusterState = cmgMessagesFactory.clusterState()
                .clusterTag(cmgMessagesFactory.clusterTag().clusterId(UUID.randomUUID()).clusterName("foo").build())
                .cmgNodes(Set.of("foo"))
                .metaStorageNodes(Set.of("foo"))
                .version("foo")
                .build();

        ClusterStateMessage message2 = cmgMessagesFactory.clusterStateMessage()
                .clusterState(clusterState)
                .build();

        CompletableFuture<Void> send2Future = messagingService.send(nodeName, ChannelType.DEFAULT, message2);

        CancelInitMessage message3 = cmgMessagesFactory.cancelInitMessage()
                .reason("foo")
                .build();

        CompletableFuture<Void> send3Future = messagingService.send(nodeName, ChannelType.DEFAULT, message3);

        // Check that no messages have been handled before the recovery is complete.
        verifyNoInteractions(callback);

        handler.onRecoveryComplete();

        assertThat(allOf(send1Future, send2Future, send3Future), willCompleteSuccessfully());

        // Verify that all buffered messages have been processed in correct order.
        InOrder inOrder = inOrder(callback);

        inOrder.verify(callback).onCmgInitMessageReceived(any(CmgInitMessage.class), any(), any());
        inOrder.verify(callback).onClusterStateMessageReceived(any(ClusterStateMessage.class), any(), any());
        inOrder.verify(callback).onCancelInitMessageReceived(any(CancelInitMessage.class), any(), any());

        // Verify that new messages get handled immediately.
        CompletableFuture<Void> send4Future = messagingService.send(nodeName, ChannelType.DEFAULT, message2);

        assertThat(send4Future, willCompleteSuccessfully());

        inOrder.verify(callback).onClusterStateMessageReceived(any(ClusterStateMessage.class), any(), any());
    }
}
