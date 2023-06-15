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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that check correctness of an invocation {@link MetaStorageManager#deployWatches()}.
 */
public class MetaStorageDeployWatchesCorrectnessTest extends IgniteAbstractTest {
    /** Vault manager. */
    private static VaultManager vaultManager;

    @BeforeAll
    public static void init() {
        vaultManager = new VaultManager(new InMemoryVaultService());

        vaultManager.start();
    }

    @AfterAll
    public static void deInit() {
        vaultManager.beforeNodeStop();

        vaultManager.stop();
    }

    /**
     * Returns a stream with test arguments.
     *
     * @return Stream of different types of Meta storages to to check.
     * @throws Exception If failed.
     */
    private static Stream<MetaStorageManager> metaStorageProvider() throws Exception {
        HybridClock clock = new HybridClockImpl();
        String mcNodeName = "mc-node-1";

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
        ClusterService clusterService = mock(ClusterService.class);
        LogicalTopologyService logicalTopologyService = mock(LogicalTopologyService.class);
        RaftManager raftManager = mock(RaftManager.class);

        when(cmgManager.metaStorageNodes()).thenReturn(completedFuture(Set.of(mcNodeName)));
        when(clusterService.nodeName()).thenReturn(mcNodeName);
        when(raftManager.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any(), any())).thenReturn(completedFuture(mock(
                RaftGroupService.class)));

        return Stream.of(
                new MetaStorageManagerImpl(
                        vaultManager,
                        clusterService,
                        cmgManager,
                        logicalTopologyService,
                        raftManager,
                        new SimpleInMemoryKeyValueStorage(mcNodeName),
                        clock
                ),
                StandaloneMetaStorageManager.create(vaultManager)
        );
    }

    /**
     * Invokes {@link MetaStorageManager#deployWatches()} and checks result.
     *
     * @param metastore Meta storage.
     */
    @ParameterizedTest
    @MethodSource("metaStorageProvider")
    public void testCheckCorrectness(MetaStorageManager metastore) throws Exception {
        var deployWatchesFut = metastore.deployWatches();

        assertFalse(deployWatchesFut.isDone());

        metastore.start();

        assertThat(deployWatchesFut, willCompleteSuccessfully());

        metastore.beforeNodeStop();

        metastore.stop();
    }
}
