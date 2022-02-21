/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.join;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.join.messages.CancelInitMessage;
import org.apache.ignite.internal.join.messages.CmgInitMessage;
import org.apache.ignite.internal.join.messages.InitMessagesFactory;
import org.apache.ignite.internal.join.messages.MetastorageInitMessage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for {@link ClusterInitializer}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ClusterInitializerTest {
    @Mock
    private MessagingService messagingService;

    @Mock
    private TopologyService topologyService;

    private ClusterInitializer clusterInitializer;

    private final InitMessagesFactory msgFactory = new InitMessagesFactory();

    @BeforeEach
    void setUp(@Mock ClusterService clusterService) {
        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        clusterInitializer = new ClusterInitializer(clusterService);
    }

    /**
     * Tests the happy-case scenario of cluster initialization.
     */
    @Test
    void testNormalInit() {
        ClusterNode metastorageNode = new ClusterNode("metastore", "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNode("cmg", "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(any(ClusterNode.class), any(MetastorageInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    MetastorageInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.metastorageNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.invoke(any(ClusterNode.class), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    CmgInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.cmgNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        // check that leaders are different in case different node IDs are provided
        CompletableFuture<Leaders> leaders = clusterInitializer.initCluster(List.of(metastorageNode.name()), List.of(cmgNode.name()));

        verify(messagingService).invoke(eq(metastorageNode), any(MetastorageInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(cmgNode), any(MetastorageInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(metastorageNode), any(CmgInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong());

        var expectedLeaders = new Leaders(metastorageNode.name(), cmgNode.name());

        assertThat(leaders, willBe(equalTo(expectedLeaders)));
    }

    /**
     * Tests the happy-case scenario of cluster initialization when only Meta Storage are provided.
     */
    @Test
    void testNormalInitSingleNodeList() {
        ClusterNode metastorageNode = new ClusterNode("metastore", "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNode("cmg", "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(any(ClusterNode.class), any(MetastorageInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    MetastorageInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.metastorageNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.invoke(any(ClusterNode.class), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    CmgInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.cmgNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        // check that leaders are the same in case CMG node list is empty
        CompletableFuture<Leaders> leaders = clusterInitializer.initCluster(List.of(metastorageNode.name()), List.of());

        verify(messagingService).invoke(eq(metastorageNode), any(MetastorageInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(cmgNode), any(MetastorageInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(metastorageNode), any(CmgInitMessage.class), anyLong());
        verify(messagingService).invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong());

        var expectedLeaders = new Leaders(metastorageNode.name(), metastorageNode.name());

        assertThat(leaders, willBe(equalTo(expectedLeaders)));
    }

    /**
     * Tests a situation when one of the nodes fail during initialization.
     */
    @Test
    void testInitCancel() {
        ClusterNode metastorageNode = new ClusterNode("metastore", "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNode("cmg", "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(any(ClusterNode.class), any(MetastorageInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    MetastorageInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.metastorageNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.invoke(eq(metastorageNode), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    CmgInitMessage message = invocation.getArgument(1);

                    NetworkMessage response = msgFactory.leaderElectedMessage()
                            .leaderName(message.cmgNodes()[0])
                            .build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    NetworkMessage response = msgFactory.initErrorMessage().cause("foobar").build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.send(any(ClusterNode.class), any(CancelInitMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Leaders> leaders = clusterInitializer.initCluster(List.of(metastorageNode.name()), List.of(cmgNode.name()));

        InitException e = assertFutureThrows(InitException.class, leaders);

        assertThat(e.getMessage(), containsString(String.format("Got error response from node \"%s\": foobar", cmgNode.name())));

        verify(messagingService).send(eq(metastorageNode), any(CancelInitMessage.class));
        verify(messagingService).send(eq(cmgNode), any(CancelInitMessage.class));
    }

    /**
     * Tests that providing no nodes for the initialization throws an error.
     */
    @Test
    void testEmptyInit() {
        CompletableFuture<Leaders> leaders = clusterInitializer.initCluster(List.of(), List.of());

        assertFutureThrows(IllegalArgumentException.class, leaders);
    }

    /**
     * Tests that if some nodes are not present in the topology, an error is thrown.
     */
    @Test
    void testUnresolvableNode() {
        CompletableFuture<Leaders> leaders = clusterInitializer.initCluster(List.of("foo"), List.of("bar"));

        InitException e = assertFutureThrows(InitException.class, leaders);

        assertThat(e.getMessage(), containsString("Node \"foo\" is not present in the physical topology"));
    }

    private static <T extends Throwable> T assertFutureThrows(Class<T> expected, CompletableFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), isA(expected));

        return expected.cast(e.getCause());
    }
}
