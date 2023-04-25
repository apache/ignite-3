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

package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for {@link DistributionZoneManager} unit tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class BaseDistributionZoneManagerTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private TablesConfiguration tablesConfiguration;

    @InjectConfiguration
    protected DistributionZonesConfiguration zonesConfiguration;

    protected DistributionZoneManager distributionZoneManager;

    protected SimpleInMemoryKeyValueStorage keyValueStorage;

    protected LogicalTopology topology;

    protected ClusterStateStorage clusterStateStorage;

    protected MetaStorageManager metaStorageManager;

    protected VaultManager vaultMgr;

    private final List<IgniteComponent> components = new ArrayList<>();

    /** Revision updater. */
    protected Consumer<Function<Long, CompletableFuture<?>>> revisionUpdater;

    /**
     * Revision listener holder.
     */
    @InjectRevisionListenerHolder
    private ConfigurationStorageRevisionListenerHolder fieldRevisionListenerHolder;

    @BeforeEach
    void setUp() {
        vaultMgr = new VaultManager(new InMemoryVaultService());

        components.add(vaultMgr);

        keyValueStorage = spy(new SimpleInMemoryKeyValueStorage("test"));

        metaStorageManager = StandaloneMetaStorageManager.create(vaultMgr, keyValueStorage);

        components.add(metaStorageManager);

        clusterStateStorage = new TestClusterStateStorage();

        components.add(clusterStateStorage);

        topology = new LogicalTopologyImpl(clusterStateStorage);

        revisionUpdater = (Function<Long, CompletableFuture<?>> function) -> {
            await(function.apply(0L));

            fieldRevisionListenerHolder.listenUpdateStorageRevision(newStorageRevision -> {
                log.info("Notify about revision: {}", newStorageRevision);

                return function.apply(newStorageRevision);
            });
        };

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        when(cmgManager.logicalTopology()).thenAnswer(invocation -> completedFuture(topology.getLogicalTopology()));

        distributionZoneManager = new DistributionZoneManager(
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                new LogicalTopologyServiceImpl(topology, cmgManager),
                vaultMgr,
                revisionUpdater,
                "test"
        );

        // Not adding 'distributionZoneManager' on purpose, it's started manually.
        components.forEach(IgniteComponent::start);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (distributionZoneManager != null) {
            components.add(distributionZoneManager);
        }

        Collections.reverse(components);

        IgniteUtils.closeAll(components.stream().map(c -> c::beforeNodeStop));
        IgniteUtils.closeAll(components.stream().map(c -> c::stop));
    }

    /**
     * This method is used to initialize the meta storage revision before starting the distribution zone manager.
     *
     * @throws Exception
     */
    protected void deployWatchesAndUpdateMetaStorageRevision() throws Exception {
        // Watches are deployed before distributionZoneManager start in order to update Meta Storage revision before
        // distributionZoneManager's recovery.
        metaStorageManager.deployWatches();

        // Bump Meta Storage applied revision by modifying a fake key. DistributionZoneManager breaks on start if Vault is not empty, but
        // Meta Storage revision is equal to 0.
        var fakeKey = new ByteArray("foobar");

        CompletableFuture<Boolean> invokeFuture = metaStorageManager.invoke(
                Conditions.notExists(fakeKey),
                Operations.put(fakeKey, fakeKey.bytes()),
                Operations.noop()
        );

        assertThat(invokeFuture, willBe(true));

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() > 0, 10_000));
    }
}
