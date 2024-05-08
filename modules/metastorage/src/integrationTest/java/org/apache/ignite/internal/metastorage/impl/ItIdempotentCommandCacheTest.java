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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.SyncTimeCommand;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.rpc.ActionResponse;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for idempotency of {@link org.apache.ignite.internal.metastorage.command.IdempotentCommand}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItIdempotentCommandCacheTest extends IgniteAbstractTest {
    private ClusterService clusterService;

    private Loza raftManager;

    private KeyValueStorage storage;

    private MetaStorageManagerImpl metaStorageManager;

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration("mock.responseTimeout = 100") RaftConfiguration raftConfiguration,
            @InjectConfiguration("mock.idleSyncTimeInterval = 100") MetaStorageConfiguration metaStorageConfiguration
    ) {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        HybridClock clock = new HybridClockImpl();

        var raftGroupEventsClientListener = new RaftGroupEventsClientListener();

        raftManager = new Loza(clusterService, new NoOpMetricManager(), raftConfiguration, workDir.resolve("loza"), clock,
                raftGroupEventsClientListener);

        var logicalTopologyService = mock(LogicalTopologyService.class);

        var topologyAwareRaftGroupServiceFactory = new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(clusterService.nodeName())));

        storage = new RocksDbKeyValueStorage(
                clusterService.nodeName(),
                workDir.resolve("metastorage"),
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

        assertThat(
                startAsync(clusterService, raftManager, metaStorageManager)
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        List<IgniteComponent> components = List.of(metaStorageManager, raftManager, clusterService);

        closeAll(Stream.concat(
                components.stream().map(c -> c::beforeNodeStop),
                Stream.of(() -> assertThat(stopAsync(components), willCompleteSuccessfully()))
        ));
    }

    @RepeatedTest(500)
    public void testIdempotentInvoke() throws InterruptedException {
        ByteArray testKey = new ByteArray("key".getBytes(StandardCharsets.UTF_8));
        byte[] testValue = "value".getBytes(StandardCharsets.UTF_8);

        AtomicInteger writeActionReqCount = new AtomicInteger();
        CompletableFuture<Void> retryBlockingFuture = new CompletableFuture<>();

        log.info("Test: blocking messages.");

        dropMessages((n, msg) -> {
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

        CompletableFuture<Boolean> fut = metaStorageManager.invoke(notExists(testKey), put(testKey, testValue), noop());

        assertTrue(waitForCondition(() -> checkValueInStorage(testKey.bytes(), testValue), 10_000));

        log.info("Test: value appeared in storage.");

        assertTrue(retryBlockingFuture.complete(null));

        assertTrue(waitForCondition(() -> writeActionReqCount.get() == 2, 10_000));

        stopDroppingMessages();

        assertThat(fut, willCompleteSuccessfully());
        log.info("Test: invoke complete.");

        assertTrue(fut.join());
        assertTrue(checkValueInStorage(testKey.bytes(), testValue));
    }

    private void dropMessages(BiPredicate<String, NetworkMessage> predicate) {
        ((DefaultMessagingService) clusterService.messagingService()).dropMessages(predicate);
    }

    private void stopDroppingMessages() {
        ((DefaultMessagingService) clusterService.messagingService()).dropMessages(null);
    }

    private boolean checkValueInStorage(byte[] testKey, byte[] testValueExpected) {
        Entry e = storage.get(testKey);

        return e != null && !e.empty() && !e.tombstone() && Arrays.equals(e.value(), testValueExpected);
    }
}
