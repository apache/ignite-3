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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for Meta Storage Watches.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItMetaStorageWatchTest {
    private static class Node {
        private final ClusterService clusterService;

        private final RaftManager raftManager;

        private final MetaStorageManager metaStorageManager;

        private final CompletableFuture<Set<String>> metaStorageNodesFuture = new CompletableFuture<>();

        Node(ClusterService clusterService, RaftConfiguration raftConfiguration, Path dataPath) {
            this.clusterService = clusterService;

            Path basePath = dataPath.resolve(name());

            this.raftManager = new Loza(
                    clusterService,
                    raftConfiguration,
                    basePath.resolve("raft"),
                    new HybridClockImpl()
            );

            var vaultManager = mock(VaultManager.class);

            when(vaultManager.get(any())).thenReturn(CompletableFuture.completedFuture(null));
            when(vaultManager.put(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
            when(vaultManager.putAll(any())).thenReturn(CompletableFuture.completedFuture(null));

            var cmgManager = mock(ClusterManagementGroupManager.class);

            when(cmgManager.metaStorageNodes()).thenReturn(metaStorageNodesFuture);

            this.metaStorageManager = new MetaStorageManagerImpl(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    raftManager,
                    new RocksDbKeyValueStorage(name(), basePath.resolve("storage"))
            );
        }

        void start(Set<String> metaStorageNodes) {
            clusterService.start();
            raftManager.start();
            metaStorageManager.start();

            metaStorageNodesFuture.complete(metaStorageNodes);
        }

        String name() {
            return clusterService.localConfiguration().getName();
        }

        void stop() throws Exception {
            Stream<AutoCloseable> beforeNodeStop = Stream.of(metaStorageManager, raftManager, clusterService).map(c -> c::beforeNodeStop);

            Stream<AutoCloseable> nodeStop = Stream.of(metaStorageManager, raftManager, clusterService).map(c -> c::stop);

            IgniteUtils.closeAll(Stream.concat(beforeNodeStop, nodeStop));
        }
    }

    private TestInfo testInfo;

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    private final List<Node> nodes = new ArrayList<>();

    @BeforeEach
    public void beforeTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    private void startNodes(int amount) {
        List<NetworkAddress> localAddresses = findLocalAddresses(10_000, 10_000 + nodes.size() + amount);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
                .map(addr -> ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder))
                .forEach(clusterService -> nodes.add(new Node(clusterService, raftConfiguration, workDir)));

        nodes.parallelStream().forEach(node -> node.start(Set.of(nodes.get(0).name())));
    }

    @Test
    void testExactWatch() throws Exception {
        testWatches((node, latch) -> node.metaStorageManager.registerExactWatch(new ByteArray("foo"), new WatchListener() {
            @Override
            public void onUpdate(WatchEvent event) {
                assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                fail();
            }
        }));
    }

    @Test
    void testPrefixWatch() throws Exception {
        testWatches((node, latch) -> node.metaStorageManager.registerPrefixWatch(new ByteArray("fo"), new WatchListener() {
            @Override
            public void onUpdate(WatchEvent event) {
                assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                fail();
            }
        }));
    }

    @Test
    void testRangeWatch() throws Exception {
        testWatches((node, latch) -> {
            var startRange = new ByteArray("fo" + ('o' - 1));
            var endRange = new ByteArray("foz");

            node.metaStorageManager.registerRangeWatch(startRange, endRange, new WatchListener() {
                @Override
                public void onUpdate(WatchEvent event) {
                    assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                    assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                    latch.countDown();
                }

                @Override
                public void onError(Throwable e) {
                    fail();
                }
            });
        });
    }

    private void testWatches(BiConsumer<Node, CountDownLatch> registerWatchAction) throws Exception {
        int numNodes = 3;

        startNodes(numNodes);

        var latch = new CountDownLatch(numNodes);

        for (Node node : nodes) {
            registerWatchAction.accept(node, latch);

            node.metaStorageManager.deployWatches();
        }

        var key = new ByteArray("foo");

        CompletableFuture<Boolean> invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.notExists(key),
                Operations.put(key, "bar".getBytes(StandardCharsets.UTF_8)),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Tests that metastorage missed metastorage events are replayed after deploying watches.
     */
    @Test
    void testReplayUpdates() throws InterruptedException {
        int numNodes = 3;

        startNodes(numNodes);

        var exactLatch = new CountDownLatch(numNodes);
        var prefixLatch = new CountDownLatch(numNodes);

        for (Node node : nodes) {
            node.metaStorageManager.registerExactWatch(new ByteArray("foo"), new WatchListener() {
                @Override
                public void onUpdate(WatchEvent event) {
                    assertThat(event.entryEvent().newEntry().key(), is("foo".getBytes(StandardCharsets.UTF_8)));
                    assertThat(event.entryEvent().newEntry().value(), is("bar".getBytes(StandardCharsets.UTF_8)));

                    exactLatch.countDown();
                }

                @Override
                public void onError(Throwable e) {
                    fail();
                }
            });

            node.metaStorageManager.registerPrefixWatch(new ByteArray("ba"), new WatchListener() {
                @Override
                public void onUpdate(WatchEvent event) {
                    List<String> keys = event.entryEvents().stream()
                            .map(e -> new String(e.newEntry().key(), StandardCharsets.UTF_8))
                            .collect(Collectors.toList());

                    List<String> values = event.entryEvents().stream()
                            .map(e -> new String(e.newEntry().value(), StandardCharsets.UTF_8))
                            .collect(Collectors.toList());

                    assertThat(keys, containsInAnyOrder("bar", "baz"));
                    assertThat(values, containsInAnyOrder("one", "two"));

                    prefixLatch.countDown();
                }

                @Override
                public void onError(Throwable e) {
                    fail();
                }
            });
        }

        CompletableFuture<Boolean> invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.notExists(new ByteArray("foo")),
                Operations.put(new ByteArray("foo"), "bar".getBytes(StandardCharsets.UTF_8)),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));

        invokeFuture = nodes.get(0).metaStorageManager.invoke(
                Conditions.exists(new ByteArray("foo")),
                List.of(
                        Operations.put(new ByteArray("bar"), "one".getBytes(StandardCharsets.UTF_8)),
                        Operations.put(new ByteArray("baz"), "two".getBytes(StandardCharsets.UTF_8))
                ),
                List.of()
        );

        assertThat(invokeFuture, willBe(true));

        nodes.forEach(node -> {
            try {
                node.metaStorageManager.deployWatches();
            } catch (NodeStoppingException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(exactLatch.await(10, TimeUnit.SECONDS));
        assertTrue(prefixLatch.await(10, TimeUnit.SECONDS));
    }
}
