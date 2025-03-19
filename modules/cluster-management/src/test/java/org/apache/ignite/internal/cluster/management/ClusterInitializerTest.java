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

package org.apache.ignite.internal.cluster.management;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for {@link ClusterInitializer}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ClusterInitializerTest extends BaseIgniteAbstractTest {
    @Mock
    private MessagingService messagingService;

    @Mock
    private TopologyService topologyService;

    private ClusterInitializer clusterInitializer;

    private final CmgMessagesFactory msgFactory = new CmgMessagesFactory();

    @BeforeEach
    void setUp(@Mock ClusterService clusterService) {
        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        clusterInitializer = new ClusterInitializer(
                clusterService,
                hocon -> hocon,
                new TestConfigurationValidator()
        );
    }

    /**
     * Tests the happy-case scenario of cluster initialization.
     */
    @Test
    void testNormalInit() {
        ClusterNode metastorageNode = new ClusterNodeImpl(randomUUID(), "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNodeImpl(randomUUID(), "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(any(ClusterNode.class), any(CmgInitMessage.class), anyLong()))
                .thenReturn(initCompleteMessage());

        // check that leaders are different in case different node IDs are provided
        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(
                List.of(metastorageNode.name()),
                List.of(cmgNode.name()),
                "cluster"
        );

        verify(messagingService).invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong());
        verify(messagingService, never()).invoke(eq(metastorageNode), any(CmgInitMessage.class), anyLong());

        assertThat(initFuture, willBe(nullValue(Void.class)));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 9})  // Runs the test with 1 to 10 nodes
    void testInitEmptyMsCmgNodes(int numNodes) {
        // Create a list of nodes dynamically
        List<ClusterNode> allNodes = IntStream.rangeClosed(1, numNodes)
                .mapToObj(i -> (ClusterNode) new ClusterNodeImpl(
                        randomUUID(),
                        "node" + i,
                        new NetworkAddress("foo" + i, 1230 + i)))
                .collect(Collectors.toList());

        // Mock topology service behavior
        for (ClusterNode node : allNodes) {
            when(topologyService.getByConsistentId(node.name())).thenReturn(node);
        }
        when(topologyService.allMembers()).thenReturn(allNodes);

        when(messagingService.invoke(any(ClusterNode.class), any(CmgInitMessage.class), anyLong()))
                .thenReturn(initCompleteMessage());

        // Initialize cluster
        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(
                List.of(),
                List.of(),
                "cluster"
        );

        // Convert node names to a set for validation.
        // See initCluster(...) Javadoc for details.
        Set<String> cmgNodeNameSet = allNodes.stream().map(ClusterNode::name).sorted()
                .limit(numNodes < 5 ? 3 : 5)
                .collect(Collectors.toSet());

        // Verify messaging service calls
        for (int i = 1; i <= allNodes.size(); i++) {
            ClusterNode node = allNodes.get(i - 1);

            boolean shouldBeCmg = i <= 3 || (numNodes >= 5 && i <= 5);

            if (shouldBeCmg) {
                verify(messagingService).invoke(eq(node), assertArg((CmgInitMessage msg) -> {
                    assertThat(msg.metaStorageNodes(), equalTo(cmgNodeNameSet));
                    assertThat(msg.cmgNodes(), equalTo(cmgNodeNameSet));
                }), anyLong());
            } else {
                verify(messagingService, never()).invoke(eq(node), any(CmgInitMessage.class), anyLong());
            }
        }

        // Assert that initialization completed successfully
        assertThat(initFuture, willBe(nullValue(Void.class)));
    }


    /**
     * Tests the happy-case scenario of cluster initialization when only Meta Storage are provided.
     */
    @Test
    void testNormalInitSingleNodeList() {
        ClusterNode metastorageNode = new ClusterNodeImpl(randomUUID(), "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNodeImpl(randomUUID(), "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(any(ClusterNode.class), any(CmgInitMessage.class), anyLong()))
                .thenReturn(initCompleteMessage());

        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(
                List.of(metastorageNode.name()),
                List.of(),
                "cluster"
        );

        verify(messagingService).invoke(eq(metastorageNode), any(CmgInitMessage.class), anyLong());
        verify(messagingService, never()).invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong());

        assertThat(initFuture, willBe(nullValue(Void.class)));
    }

    /**
     * Tests a situation when one of the nodes fail during initialization.
     */
    @Test
    void testInitCancel() {
        ClusterNode metastorageNode = new ClusterNodeImpl(randomUUID(), "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNodeImpl(randomUUID(), "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    NetworkMessage response = msgFactory.initErrorMessage().cause("foobar").shouldCancel(true).build();

                    return CompletableFuture.completedFuture(response);
                });

        when(messagingService.send(any(ClusterNode.class), any(CancelInitMessage.class)))
                .thenReturn(nullCompletedFuture());

        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(
                List.of(metastorageNode.name()),
                List.of(cmgNode.name()),
                "cluster"
        );

        InternalInitException e = assertFutureThrows(InternalInitException.class, initFuture);

        assertThat(e.getMessage(), containsString(String.format("Got error response from node \"%s\": foobar", cmgNode.name())));

        verify(messagingService).send(eq(cmgNode), any(CancelInitMessage.class));
        verify(messagingService, never()).send(eq(metastorageNode), any(CancelInitMessage.class));
    }

    /**
     * Tests a situation when the init command fails non-critically, so that initialization is not cancelled.
     */
    @Test
    void testInitNoCancel() {
        ClusterNode metastorageNode = new ClusterNodeImpl(randomUUID(), "metastore", new NetworkAddress("foo", 123));
        ClusterNode cmgNode = new ClusterNodeImpl(randomUUID(), "cmg", new NetworkAddress("bar", 456));

        when(topologyService.getByConsistentId(metastorageNode.name())).thenReturn(metastorageNode);
        when(topologyService.getByConsistentId(cmgNode.name())).thenReturn(cmgNode);
        when(topologyService.allMembers()).thenReturn(List.of(metastorageNode, cmgNode));

        when(messagingService.invoke(eq(cmgNode), any(CmgInitMessage.class), anyLong()))
                .thenAnswer(invocation -> {
                    NetworkMessage response = msgFactory.initErrorMessage().cause("foobar").shouldCancel(false).build();

                    return CompletableFuture.completedFuture(response);
                });

        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(
                List.of(metastorageNode.name()),
                List.of(cmgNode.name()),
                "cluster"
        );

        InternalInitException e = assertFutureThrows(InternalInitException.class, initFuture);

        assertThat(e.getMessage(), containsString(String.format("Got error response from node \"%s\": foobar", cmgNode.name())));

        verify(messagingService, never()).send(eq(metastorageNode), any(CancelInitMessage.class));
        verify(messagingService, never()).send(eq(metastorageNode), any(CancelInitMessage.class));
    }

    private CompletableFuture<NetworkMessage> initCompleteMessage() {
        NetworkMessage msg = msgFactory.initCompleteMessage().build();

        return CompletableFuture.completedFuture(msg);
    }

    /**
     * Tests that providing no nodes for the initialization throws an error.
     */
    @Test
    void testInitIllegalArguments() {
        assertThrows(IllegalArgumentException.class, () -> clusterInitializer.initCluster(List.of(" "), List.of("bar"), "cluster"));

        assertThrows(IllegalArgumentException.class, () -> clusterInitializer.initCluster(List.of("foo"), List.of(" "), "cluster"));

        assertThrows(IllegalArgumentException.class, () -> clusterInitializer.initCluster(List.of("foo"), List.of("bar"), " "));
    }

    /**
     * Tests that if some nodes are not present in the topology, an error is thrown.
     */
    @Test
    void testUnresolvableNode() {
        CompletableFuture<Void> initFuture = clusterInitializer.initCluster(List.of("foo"), List.of("bar"), "cluster");

        IllegalArgumentException e = assertFutureThrows(IllegalArgumentException.class, initFuture);

        assertThat(e.getMessage(), containsString("Node \"foo\" is not present in the physical topology"));
    }

    private static <T extends Throwable> T assertFutureThrows(Class<T> expected, CompletableFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));

        assertThat(e.getCause(), isA(expected));

        return expected.cast(e.getCause());
    }
}
