/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.metastorage;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.configuration.schemas.metastorage.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.message.MetastorageMessagesFactory;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link MetaStorageManager}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MetaStorageManagerTest {
    /** */
    @Mock
    private MessagingService messagingService;

    /** */
    private final MetastorageMessagesFactory msgFactory = new MetastorageMessagesFactory();

    /** Holder class. */
    private static class Node {
        /** */
        final MetaStorageManager manager;

        /** */
        final ClusterNode clusterNode;

        /** */
        Node(ClusterNode clusterNode, MetaStorageManager manager) {
            this.manager = manager;
            this.clusterNode = clusterNode;
        }
    }

    /**
     * Creates a "cluster" consisting of nodes of the given names, where every component is mocked, except for the
     * {@link MetaStorageManager}.
     */
    private List<Node> createCluster(String... names) {
        List<ClusterNode> nodes = Arrays.stream(names)
            .map(MetaStorageManagerTest::createNode)
            .collect(Collectors.toUnmodifiableList());

        // Create the Raft component.
        var raftManager = mock(Loza.class);

        when(raftManager.prepareRaftGroup(anyString(), anyList(), any()))
            .thenReturn(CompletableFuture.completedFuture(mock(RaftGroupService.class)));

        // Create the Network component.
        var topologyService = mock(TopologyService.class);

        when(topologyService.allMembers()).thenReturn(nodes);

        var clusterService = mock(ClusterService.class);

        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        // Create a mock Meta Storage config with a poll interval of 1 ms.
        var pollIntervalMillis = mock(ConfigurationValue.class);

        when(pollIntervalMillis.value()).thenReturn(1L);

        var config = mock(MetaStorageConfiguration.class);

        when(config.startupPollIntervalMillis()).thenReturn(pollIntervalMillis);

        return nodes.stream()
            .map(node -> {
                VaultManager vaultManager = mock(VaultManager.class);

                when(vaultManager.name()).thenReturn(CompletableFuture.completedFuture(node.name()));

                var metaStorageManager = new MetaStorageManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    new SimpleInMemoryKeyValueStorage(),
                    msgFactory,
                    config
                );

                return new Node(node, metaStorageManager);
            })
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Creates a {@code ClusterNode} on a random port.
     */
    private static ClusterNode createNode(String name) {
        int port = ThreadLocalRandom.current().nextInt(0, 10000);

        return new ClusterNode(name, name, new NetworkAddress("localhost", port));
    }

    /**
     * Tests the startup of a single node that hosts the Meta Storage.
     * It should simply block until the "init" command arrives.
     */
    @Test
    void testSingleNodeHostStart() {
        Node node = createCluster("foobar").get(0);

        node.manager.start();

        verify(messagingService, never()).send(any(NetworkAddress.class), any(), anyString());

        CompletableFuture<Boolean> hasMetastorage = supplyAsync(node.manager::hasMetastorageLocally);

        node.manager.init(List.of(node.clusterNode.name()));

        assertThat(hasMetastorage, willBe(equalTo(true)));
    }

    /**
     * Tests the startup of a single node that does not host the Meta Storage.
     * It should simply block until the "init" command arrives.
     */
    @Test
    void testSingleNodeNotHostStart() {
        Node node = createCluster("foobar").get(0);

        node.manager.start();

        verify(messagingService, never()).send(any(NetworkAddress.class), any(), anyString());

        CompletableFuture<Boolean> hasMetastorage = supplyAsync(node.manager::hasMetastorageLocally);

        node.manager.init(List.of("barbaz"));

        assertThat(hasMetastorage, willBe(equalTo(false)));
    }

    /**
     * Tests a scenario when two nodes start and receive a broadcast "init" command.
     */
    @Test
    void testZombieStateStart() {
        NetworkMessage response = msgFactory.metastorageNotReadyResponse().build();

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(response));

        List<Node> nodes = createCluster("foo", "bar");

        nodes.forEach(node -> node.manager.start());

        CompletableFuture<Boolean> hasMetastorageNode0 = supplyAsync(nodes.get(0).manager::hasMetastorageLocally);
        CompletableFuture<Boolean> hasMetastorageNode1 = supplyAsync(nodes.get(1).manager::hasMetastorageLocally);

        var metaStorageNodes = List.of(nodes.get(0).clusterNode.name());

        nodes.forEach(node -> node.manager.init(metaStorageNodes));

        assertThat(hasMetastorageNode0, willBe(equalTo(true)));
        assertThat(hasMetastorageNode1, willBe(equalTo(false)));
    }

    /**
     * Tests a scenario when a single node receives the "init" command, while the second node obtains the
     * Meta Storage nodes information through the join protocol.
     */
    @Test
    void testNodeJoin() {
        List<Node> nodes = createCluster("foo", "bar");

        Node node0 = nodes.get(0);
        Node node1 = nodes.get(1);

        var metaStorageNodes = List.of(node1.clusterNode.name());

        when(messagingService.invoke(eq(node0.clusterNode), any(), anyLong()))
            .thenAnswer(invocation -> {
                NetworkMessage response = msgFactory.metastorageNodesResponse()
                    .metastorageNodes(metaStorageNodes)
                    .build();

                return CompletableFuture.completedFuture(response);
            });

        when(messagingService.invoke(eq(node1.clusterNode), any(), anyLong()))
            .thenAnswer(invocation -> {
                NetworkMessage response = msgFactory.metastorageNotReadyResponse().build();

                return CompletableFuture.completedFuture(response);
            });

        node0.manager.start();
        node0.manager.init(metaStorageNodes);

        node1.manager.start();

        CompletableFuture<Boolean> hasMetastorageNode0 = supplyAsync(node0.manager::hasMetastorageLocally);
        CompletableFuture<Boolean> hasMetastorageNode1 = supplyAsync(node1.manager::hasMetastorageLocally);

        assertThat(hasMetastorageNode0, willBe(equalTo(false)));
        assertThat(hasMetastorageNode1, willBe(equalTo(true)));
    }

    /**
     * Tests stopping two nodes before they receive the "init" command.
     * Two nodes are started here to test two different stop scenarios: one node is simply blocked, because it started
     * first, while the second node is continuously polling the first one.
     */
    @Test
    void testStop() {
        List<Node> nodes = createCluster("foo", "bar");

        Node node0 = nodes.get(0);
        Node node1 = nodes.get(1);

        NetworkMessage response = msgFactory.metastorageNotReadyResponse().build();

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(response));

        node0.manager.start();
        node1.manager.start();

        node0.manager.beforeNodeStop();
        node0.manager.stop();

        node1.manager.beforeNodeStop();
        node1.manager.stop();
    }

    /**
     * Similar to {@link #testStop} but stops the nodes after having initiated the "init" command.
     */
    @Test
    void testStopAfterInit() {
        List<Node> nodes = createCluster("foo", "bar");

        Node node0 = nodes.get(0);
        Node node1 = nodes.get(1);

        NetworkMessage response = msgFactory.metastorageNotReadyResponse().build();

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(response));

        node0.manager.start();
        node1.manager.start();

        node0.manager.init(List.of(nodes.get(0).clusterNode.name()));
        node1.manager.init(List.of(nodes.get(0).clusterNode.name()));

        node0.manager.beforeNodeStop();
        node0.manager.stop();

        node1.manager.beforeNodeStop();
        node1.manager.stop();
    }
}
