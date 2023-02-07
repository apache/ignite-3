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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBeCancelledFast;
import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for {@link MetaStorageManagerImpl}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItMetaStorageManagerImplTest {
    private VaultManager vaultManager;

    private ClusterService clusterService;

    private Loza raftManager;

    private KeyValueStorage storage;

    private MetaStorageManagerImpl metaStorageManager;

    @BeforeEach
    void setUp(TestInfo testInfo, @WorkDirectory Path workDir, @InjectConfiguration RaftConfiguration raftConfiguration)
            throws NodeStoppingException {
        var addr = new NetworkAddress("localhost", 10_000);

        clusterService = clusterService(testInfo, addr.port(), new StaticNodeFinder(List.of(addr)));

        raftManager = new Loza(clusterService, raftConfiguration, workDir.resolve("loza"), new HybridClockImpl());

        vaultManager = new VaultManager(new InMemoryVaultService());

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.metaStorageNodes())
                .thenReturn(completedFuture(Set.of(clusterService.localConfiguration().getName())));

        storage = new RocksDbKeyValueStorage(clusterService.localConfiguration().getName(), workDir.resolve("metastorage"));

        metaStorageManager = new MetaStorageManagerImpl(
                vaultManager,
                clusterService,
                cmgManager,
                raftManager,
                storage
        );

        vaultManager.start();
        clusterService.start();
        raftManager.start();
        metaStorageManager.start();

        metaStorageManager.deployWatches();
    }

    @AfterEach
    void tearDown() throws Exception {
        List<IgniteComponent> components = List.of(metaStorageManager, raftManager, clusterService, vaultManager);

        IgniteUtils.closeAll(Stream.concat(
                components.stream().map(c -> c::beforeNodeStop),
                components.stream().map(c -> c::stop)
        ));
    }

    /**
     * Tests a corner case when a prefix request contains a max unsigned byte value.
     */
    @Test
    void testPrefixOverflow() throws NodeStoppingException {
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var key1 = new ByteArray(new byte[]{1, (byte) 0xFF, 0});
        var key2 = new ByteArray(new byte[]{1, (byte) 0xFF, 1});
        var key3 = new ByteArray(new byte[]{1, (byte) 0xFF, (byte) 0xFF});
        // Contains the next lexicographical prefix
        var key4 = new ByteArray(new byte[]{1, 0, 1});
        // Contains the previous lexicographical prefix
        var key5 = new ByteArray(new byte[]{1, (byte) 0xFE, 1});

        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.notExists(new ByteArray("foo")),
                List.of(
                        Operations.put(key1, value),
                        Operations.put(key2, value),
                        Operations.put(key3, value),
                        Operations.put(key4, value),
                        Operations.put(key5, value)
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));

        try (Cursor<Entry> cursor = metaStorageManager.prefix(new ByteArray(new byte[]{1, (byte) 0xFF}))) {
            List<byte[]> keys = cursor.stream().map(Entry::key).collect(toList());

            assertThat(keys, contains(key1.bytes(), key2.bytes(), key3.bytes()));
        }
    }

    /**
     * Tests that "watched" Meta Storage keys get persisted in the Vault.
     */
    @Test
    void testWatchEventsPersistence() throws InterruptedException {
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var key1 = new ByteArray("foo");
        var key2 = new ByteArray("bar");

        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.notExists(new ByteArray("foo")),
                List.of(
                        Operations.put(key1, value),
                        Operations.put(key2, value)
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));

        // No data should be persisted until any watches are registered.
        assertThat(vaultManager.get(key1), willBe(nullValue()));
        assertThat(vaultManager.get(key2), willBe(nullValue()));

        metaStorageManager.registerExactWatch(key1, noOpListener());

        invokeFuture = metaStorageManager.invoke(
                Conditions.exists(new ByteArray("foo")),
                List.of(
                        Operations.put(key1, value),
                        Operations.put(key2, value)
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() == 2, 10_000));

        // Expect that only the watched key is persisted.
        assertThat(vaultManager.get(key1).thenApply(VaultEntry::value), willBe(value));
        assertThat(vaultManager.get(key2), willBe(nullValue()));

        metaStorageManager.registerExactWatch(key2, noOpListener());

        byte[] newValue = "newValue".getBytes(StandardCharsets.UTF_8);

        invokeFuture = metaStorageManager.invoke(
                Conditions.exists(new ByteArray("foo")),
                List.of(
                        Operations.put(key1, newValue),
                        Operations.put(key2, newValue)
                ),
                List.of(Operations.noop())
        );

        assertThat(invokeFuture, willBe(true));

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() == 3, 10_000));

        assertThat(vaultManager.get(key1).thenApply(VaultEntry::value), willBe(newValue));
        assertThat(vaultManager.get(key2).thenApply(VaultEntry::value), willBe(newValue));
    }

    private static WatchListener noOpListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent event) {}

            @Override
            public void onError(Throwable e) {}
        };
    }

    @Test
    void testMetaStorageStopClosesRaftService() throws Exception {
        MetaStorageServiceImpl svc = metaStorageManager.metaStorageServiceFuture().join();

        metaStorageManager.stop();

        CompletableFuture<Entry> fut = svc.get(ByteArray.fromString("ignored"));

        assertThat(fut, willBeCancelledFast());
    }

    @Test
    void testMetaStorageStopBeforeRaftServiceStarted() throws Exception {
        metaStorageManager.stop(); // Close MetaStorage that is created in setUp.

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        Set<String> msNodes = Set.of(clusterService.localConfiguration().getName());
        CompletableFuture<Set<String>> cmgFut = new CompletableFuture<>();

        when(cmgManager.metaStorageNodes()).thenReturn(cmgFut);

        metaStorageManager = new MetaStorageManagerImpl(
                vaultManager,
                clusterService,
                cmgManager,
                raftManager,
                storage
        );

        metaStorageManager.stop();

        // Unblock the future so raft service can be initialized. Although the future should be cancelled already by the
        // stop method.
        cmgFut.complete(msNodes);

        assertThat(metaStorageManager.metaStorageServiceFuture(), willBeCancelledFast());
    }
}
