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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.configuration.schemas.metastorage.MetaStorageConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.message.MetastorageMessagesFactory;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.LocalPortRangeNodeFinder;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the {@link MetaStorageManager}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ITMetaStorageManagerTest {
    /** */
    @WorkDirectory
    private Path workDir;

    /** */
    @InjectConfiguration
    private MetaStorageConfiguration metaStorageConfiguration;

    /** List of started components. */
    private final List<IgniteComponent> components = new ArrayList<>();

    /** */
    @BeforeEach
    void setUp() {
        metaStorageConfiguration.change(c -> c.changeStartupPollIntervalMillis(100)).join();
    }

    /** */
    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(components);

        for (IgniteComponent component : components)
            component.beforeNodeStop();

        for (IgniteComponent component : components)
            component.stop();
    }

    /**
     * Creates a {@link MetaStorageManager} for tests.
     */
    private MetaStorageManager createMetaStore(
        TestInfo testInfo, NetworkAddress thisNode, NodeFinder nodeFinder
    ) throws Exception {
        var vaultManager = new VaultManager(new InMemoryVaultService());

        ClusterService clusterService = ClusterServiceTestUtils.clusterService(
            testInfo,
            thisNode.port(),
            nodeFinder,
            new TestMessageSerializationRegistryImpl(),
            new TestScaleCubeClusterServiceFactory()
        );

        var raftManager = new Loza(clusterService, workDir);

        var metaStorageManager = new MetaStorageManager(
            vaultManager,
            clusterService,
            raftManager,
            new SimpleInMemoryKeyValueStorage(),
            new MetastorageMessagesFactory(),
            metaStorageConfiguration
        );

        vaultManager.start();

        components.add(vaultManager);

        vaultManager.putName(thisNode.toString()).get(1, TimeUnit.SECONDS);

        Stream.of(clusterService, raftManager, metaStorageManager).forEach(component -> {
            component.start();

            components.add(component);
        });

        return metaStorageManager;
    }

    /**
     * Test the process of sending an "init" command to an established cluster.
     */
    @Test
    void testClusterInit(TestInfo testInfo) throws Exception {
        var allNodes = new LocalPortRangeNodeFinder(10000, 10003);

        NetworkAddress addr0 = allNodes.findNodes().get(0);
        NetworkAddress addr1 = allNodes.findNodes().get(1);
        NetworkAddress addr2 = allNodes.findNodes().get(2);

        MetaStorageManager metaStorageManager0 = createMetaStore(testInfo, addr0, allNodes);
        MetaStorageManager metaStorageManager1 = createMetaStore(testInfo, addr1, allNodes);
        MetaStorageManager metaStorageManager2 = createMetaStore(testInfo, addr2, allNodes);

        // call "init" twice to emulate a race between "init" and address polling
        metaStorageManager0.init(List.of(addr1.toString()));
        metaStorageManager1.init(List.of(addr1.toString()));

        CompletableFuture<Boolean> hasMetastorageNode0 = supplyAsync(metaStorageManager0::hasMetastorageLocally);
        CompletableFuture<Boolean> hasMetastorageNode1 = supplyAsync(metaStorageManager1::hasMetastorageLocally);
        CompletableFuture<Boolean> hasMetastorageNode2 = supplyAsync(metaStorageManager2::hasMetastorageLocally);

        assertThat(hasMetastorageNode0, willBe(equalTo(false)));
        assertThat(hasMetastorageNode1, willBe(equalTo(true)));
        assertThat(hasMetastorageNode2, willBe(equalTo(false)));
    }

    /**
     * Tests a scenario when a node joins a cluster that has already received an "init" command.
     */
    @Test
    void testClusterJoin(TestInfo testInfo) throws Exception {
        var allNodes = new LocalPortRangeNodeFinder(10000, 10002);

        NetworkAddress addr0 = allNodes.findNodes().get(0);
        NetworkAddress addr1 = allNodes.findNodes().get(1);

        MetaStorageManager metaStorageManager = createMetaStore(testInfo, addr0, allNodes);
        createMetaStore(testInfo, addr1, allNodes);

        metaStorageManager.init(List.of(addr1.toString()));

        NetworkAddress addr2 = new NetworkAddress("localhost", 10002);

        MetaStorageManager metaStorageManager2 = createMetaStore(testInfo, addr2, allNodes);

        CompletableFuture<Boolean> hasMetastorageNode2 = supplyAsync(metaStorageManager2::hasMetastorageLocally);

        assertThat(hasMetastorageNode2, willBe(equalTo(false)));
    }
}
