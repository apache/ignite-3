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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.SyncTimeCommand;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for idempotency of {@link org.apache.ignite.internal.metastorage.command.IdempotentCommand}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItIdempotentCommandCacheTest extends IgniteAbstractTest {
    private static final MetaStorageCommandsFactory CMD_FACTORY = new MetaStorageCommandsFactory();

    private static final int NODES_COUNT = 2;

    private List<Node> nodes;

    private static class Node implements AutoCloseable {
        ClusterService clusterService;

        Loza raftManager;

        KeyValueStorage storage;

        MetaStorageManagerImpl metaStorageManager;

        ClusterManagementGroupManager cmgManager;

        Node(
                TestInfo testInfo,
                RaftConfiguration raftConfiguration,
                MetaStorageConfiguration metaStorageConfiguration,
                Path workDir,
                int index
        ) {
            List<NetworkAddress> addrs = new ArrayList<>();

            for (int i = 0; i < NODES_COUNT; i++) {
                addrs.add(new NetworkAddress("localhost", 10_000 + i));
            }

            var localAddr = new NetworkAddress("localhost", 10_000 + index);

            clusterService = clusterService(testInfo, localAddr.port(), new StaticNodeFinder(addrs));

            HybridClock clock = new HybridClockImpl();

            var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

            Path lozaDir = workDir.resolve("loza" + index);

            raftManager = new Loza(clusterService, new NoOpMetricManager(), raftConfiguration, lozaDir, clock,
                    raftGroupEventsClientListener);

            var logicalTopologyService = mock(LogicalTopologyService.class);

            var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                    clusterService,
                    logicalTopologyService,
                    Loza.FACTORY,
                    raftGroupEventsClientListener
            );

            cmgManager = mock(ClusterManagementGroupManager.class);

            Path metaStorageDir = workDir.resolve("metastorage" + index);

            storage = new RocksDbKeyValueStorage(
                    clusterService.nodeName(),
                    metaStorageDir,
                    new NoOpFailureProcessor(clusterService.nodeName()));

            metaStorageManager = new MetaStorageManagerImpl(
                    clusterService,
                    cmgManager,
                    logicalTopologyService,
                    raftManager,
                    storage,
                    clock,
                    topologyAwareRaftGroupServiceFactory,
                    metaStorageConfiguration
            );
        }

        void start(CompletableFuture<Set<String>> metaStorageNodesFut) {
            when(cmgManager.metaStorageNodes()).thenReturn(metaStorageNodesFut);

            assertThat(startAsync(clusterService, raftManager, metaStorageManager), willCompleteSuccessfully());
        }

        void deployWatches() {
            assertThat(metaStorageManager.deployWatches(), willCompleteSuccessfully());
        }

        void stop() throws Exception {
            List<IgniteComponent> components = List.of(metaStorageManager, raftManager, clusterService);

            closeAll(Stream.concat(
                    components.stream().map(c -> c::beforeNodeStop),
                    Stream.of(() -> assertThat(stopAsync(components), willCompleteSuccessfully()))
            ));
        }

        @Override
        public void close() throws Exception {
            stop();
        }

        void dropMessages(BiPredicate<String, NetworkMessage> predicate) {
            ((DefaultMessagingService) clusterService.messagingService()).dropMessages(predicate);
        }

        void stopDroppingMessages() {
            ((DefaultMessagingService) clusterService.messagingService()).dropMessages(null);
        }

        boolean checkValueInStorage(byte[] testKey, byte[] testValueExpected) {
            Entry e = storage.get(testKey);

            return e != null && !e.empty() && !e.tombstone() && Arrays.equals(e.value(), testValueExpected);
        }
    }

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration("mock.responseTimeout = 100") RaftConfiguration raftConfiguration,
            @InjectConfiguration("mock.idleSyncTimeInterval = 100") MetaStorageConfiguration metaStorageConfiguration
    ) {
        nodes = new ArrayList<>();

        for (int i = 0; i < NODES_COUNT; i++) {
            Node node = new Node(testInfo, raftConfiguration, metaStorageConfiguration, workDir, i);
            nodes.add(node);
        }

        Set<String> nodeNames = nodes.stream().map(n -> n.clusterService.nodeName()).collect(toSet());
        CompletableFuture<Set<String>> metaStorageNodesFut = new CompletableFuture<>();

        nodes.forEach(n -> n.start(metaStorageNodesFut));

        metaStorageNodesFut.complete(nodeNames);

        nodes.forEach(Node::deployWatches);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(nodes.stream());
    }

    @Test
    public void testIdempotentInvoke() throws InterruptedException {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        byte[] testValue = "value".getBytes(StandardCharsets.UTF_8);
        byte[] anotherValue = "another".getBytes(StandardCharsets.UTF_8);

        AtomicInteger writeActionReqCount = new AtomicInteger();
        CompletableFuture<Void> retryBlockingFuture = new CompletableFuture<>();

        log.info("Test: blocking messages.");

        Node leader = leader(raftClient());

        leader.dropMessages((n, msg) -> {
            // Dropping the first response, this will cause timeout on first response, and then retry.
            if (msg instanceof ActionResponse && ((ActionResponse) msg).result() != null && writeActionReqCount.get() == 1) {
                log.info("Test: dropping ActionResponse: " + msg);

                return true;
            }

            if (msg instanceof WriteActionRequest) {
                WriteActionRequest request = (WriteActionRequest) msg;

                if (!(request.deserializedCommand() instanceof SyncTimeCommand)) {
                    writeActionReqCount.incrementAndGet();
                    log.info("Test: WriteActionRequest intercepted, count=" + writeActionReqCount.get());

                    // Second request: retry.
                    if (writeActionReqCount.get() == 2) {
                        log.info("Test: retry blocked.");

                        retryBlockingFuture.orTimeout(10, TimeUnit.SECONDS).join();

                        log.info("Test: retry unblocked.");
                    }
                }
            }

            return false;
        });

        MetaStorageManager metaStorageManager = leader.metaStorageManager;

        CompletableFuture<Boolean> fut = metaStorageManager.invoke(notExists(testKey), put(testKey, testValue), put(testKey, anotherValue));

        assertTrue(waitForCondition(() -> leader.checkValueInStorage(testKey.bytes(), testValue), 10_000));

        log.info("Test: value appeared in storage.");

        assertTrue(retryBlockingFuture.complete(null));

        assertTrue(waitForCondition(() -> writeActionReqCount.get() == 2, 10_000));

        leader.stopDroppingMessages();

        assertThat(fut, willCompleteSuccessfully());
        log.info("Test: invoke complete.");

        assertTrue(fut.join());
        assertTrue(leader.checkValueInStorage(testKey.bytes(), testValue));
    }

    @Test
    public void testIdempotentInvokeAfterLeaderChange() {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        byte[] testValue = "value".getBytes(StandardCharsets.UTF_8);
        byte[] anotherValue = "another".getBytes(StandardCharsets.UTF_8);

        RaftGroupService raftClient = raftClient();

        HybridClock clock = new HybridClockImpl();
        CommandIdGenerator commandIdGenerator = new CommandIdGenerator(() -> UUID.randomUUID().toString());

        InvokeCommand invokeCommand = CMD_FACTORY.invokeCommand()
                .condition(notExists(testKey))
                .success(List.of(put(testKey, testValue)))
                .failure(List.of(put(testKey, anotherValue)))
                .initiatorTimeLong(clock.nowLong())
                .id(commandIdGenerator.newId())
                .build();

        CompletableFuture<Boolean> fut = raftClient.run(invokeCommand);

        Node currentLeader = leader(raftClient);

        assertThat(fut, willCompleteSuccessfully());
        assertTrue(fut.join());

        assertTrue(currentLeader.checkValueInStorage(testKey.bytes(), testValue));

        Node newLeader = nodes.stream()
                .filter(n -> !n.clusterService.nodeName().equals(currentLeader.clusterService.nodeName()))
                .findAny()
                .orElseThrow();

        CompletableFuture<Void> transferLeadershipFut = raftClient.transferLeadership(new Peer(newLeader.clusterService.nodeName()));
        assertThat(transferLeadershipFut, willCompleteSuccessfully());

        CompletableFuture<Boolean> futAfterLeaderChange = raftClient.run(invokeCommand);

        assertThat(futAfterLeaderChange, willCompleteSuccessfully());
        assertTrue(futAfterLeaderChange.join());

        assertTrue(currentLeader.checkValueInStorage(testKey.bytes(), testValue));
        assertTrue(newLeader.checkValueInStorage(testKey.bytes(), testValue));
    }

    private Node leader(RaftGroupService raftClient) {
        CompletableFuture<Void> refreshLeaderFut = raftClient.refreshLeader();

        assertThat(refreshLeaderFut, willCompleteSuccessfully());

        String currentLeader = raftClient.leader().consistentId();

        return nodes.stream().filter(n -> n.clusterService.nodeName().equals(currentLeader)).findAny().orElseThrow();
    }

    private RaftGroupService raftClient() {
        Node node = nodes.get(0);

        PeersAndLearners configuration = PeersAndLearners
                .fromConsistentIds(nodes.stream().map(n -> n.clusterService.nodeName()).collect(toSet()));

        try {
            CompletableFuture<RaftGroupService> raftServiceFuture = node.raftManager
                    .startRaftGroupService(MetastorageGroupId.INSTANCE, configuration);

            assertThat(raftServiceFuture, willCompleteSuccessfully());

            return raftServiceFuture.join();
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }
    }
}
